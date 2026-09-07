package badgerbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const queueStateVersion = byte(3)

// Initialization is one conflict-retried transaction: concurrent constructors
// cannot overwrite the winning limits, identity, or usage. It examines only the
// fixed metadata and the first namespace key, never existing payloads.
func (s *Store[M, D]) initializeQueueState() error {
	var identity [16]byte
	if _, err := rand.Read(identity[:]); err != nil {
		return err
	}
	var persistedIdentity [16]byte
	err := withConflictRetry(context.Background(), s.runtime, func() error {
		return s.db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get(s.keys.queueStateVersionKey)
			if err == nil {
				if item.ValueSize() > 1+16 {
					return ErrIncompatibleFormat
				}
				if err := item.Value(func(value []byte) error {
					if len(value) != 1 || value[0] != queueStateVersion {
						return ErrIncompatibleFormat
					}
					return nil
				}); err != nil {
					return err
				}
				state, err := readAdmissionState(txn, s.keys.admissionKey)
				if err != nil {
					return err
				}
				if state.Limits != s.opts.AdmissionLimits {
					return &AdmissionLimitsMismatchError{Expected: s.opts.AdmissionLimits, Actual: state.Limits}
				}
				persistedIdentity = state.identity
				return nil
			}
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			prefix := []byte("ob/" + s.opts.Namespace + "/")
			it.Seek(prefix)
			hasData := it.ValidForPrefix(prefix)
			it.Close()
			if hasData {
				return ErrIncompatibleFormat
			}
			if err := txn.Set(s.keys.queueStateVersionKey, []byte{queueStateVersion}); err != nil {
				return err
			}
			state := admissionState{UsageSnapshot: UsageSnapshot{Limits: s.opts.AdmissionLimits}, identity: identity}
			if err := s.storeAdmissionState(txn, state); err != nil {
				return err
			}
			persistedIdentity = identity
			return nil
		})
	})
	if err == nil {
		s.admissionIdentity = persistedIdentity
	}
	return err
}
func (s *Store[M, D]) loadQueueSnapshotFromIndexes(ctx context.Context, txn *badger.Txn, now time.Time) (queueSnapshot, error) {
	var snapshot queueSnapshot
	now = now.UTC()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	counts := []struct {
		prefix []byte
		depth  *int64
	}{
		{prefix: s.keys.readyPrefix, depth: &snapshot.ReadyDepth},
		{prefix: s.keys.processingPrefix, depth: &snapshot.ProcessingDepth},
		{prefix: s.keys.deadLetterPrefix, depth: &snapshot.DeadLetterDepth},
	}
	for _, count := range counts {
		depth, err := s.countVisibleTimeAndIDIndex(ctx, txn, it, count.prefix)
		if err != nil {
			return queueSnapshot{}, err
		}
		*count.depth = depth
	}

	oldest := []struct {
		depth  int64
		prefix []byte
		age    *time.Duration
	}{
		{depth: snapshot.ReadyDepth, prefix: s.keys.readyCreatedPrefix, age: &snapshot.OldestReadyAge},
		{depth: snapshot.ProcessingDepth, prefix: s.keys.processingCreatedPrefix, age: &snapshot.OldestProcessingAge},
	}
	for _, state := range oldest {
		if state.depth == 0 {
			continue
		}
		age, err := s.oldestVisibleIndexAge(ctx, txn, it, state.prefix, now)
		if err != nil {
			return queueSnapshot{}, err
		}
		*state.age = age
	}

	return snapshot, nil
}
func (s *Store[M, D]) countVisibleTimeAndIDIndex(ctx context.Context, txn *badger.Txn, it *badger.Iterator, prefix []byte) (int64, error) {
	var count int64
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if err := ctxErr(ctx); err != nil {
			return 0, err
		}
		_, id, err := parseTimeAndIDKey(prefix, it.Item().Key())
		if err != nil {
			return 0, err
		}
		if !bytes.Equal(prefix, s.keys.deadLetterPrefix) {
			marker, err := s.quarantineDLQKey(txn, id)
			if err != nil {
				return 0, err
			}
			if marker != nil {
				continue
			}
		}
		count++
	}
	return count, nil
}

// Quarantine preserves creation metadata because the creation timestamp is
// unknown until its oversized source is read. Skip those entries using only the
// bounded point marker; a quarantined row never contributes to live queue age.
func (s *Store[M, D]) oldestVisibleIndexAge(ctx context.Context, txn *badger.Txn, it *badger.Iterator, prefix []byte, now time.Time) (time.Duration, error) {
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if err := ctxErr(ctx); err != nil {
			return 0, err
		}
		createdAt, id, err := parseTimeAndIDKey(prefix, it.Item().Key())
		if err != nil {
			return 0, err
		}
		marker, err := s.quarantineDLQKey(txn, id)
		if err != nil {
			return 0, err
		}
		if marker != nil {
			continue
		}
		return positiveDuration(now.Sub(createdAt)), nil
	}
	return 0, fmt.Errorf("%w: nonempty queue has no visible creation key under %q", ErrInconsistentIndex, prefix)
}
