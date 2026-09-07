package badgerbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const queueStateVersion = byte(2)

func (s *Store[M, D]) initializeQueueState() error {
	var (
		versionPresent   bool
		namespaceHasData bool
	)

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.keys.queueStateVersionKey)
		switch {
		case err == nil:
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(value) != 1 || value[0] != queueStateVersion {
				return ErrIncompatibleFormat
			}
			versionPresent = true
			return nil
		case !errors.Is(err, badger.ErrKeyNotFound):
			return err
		}

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefixes := [][]byte{
			[]byte("ob/" + s.opts.Namespace + "/"),
			s.keys.readyPrefix,
			s.keys.processingPrefix,
			s.keys.deadLetterPrefix,
		}
		for _, prefix := range prefixes {
			it.Seek(prefix)
			if it.ValidForPrefix(prefix) {
				namespaceHasData = true
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if versionPresent {
		return nil
	}
	if namespaceHasData {
		return ErrIncompatibleFormat
	}

	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.keys.queueStateVersionKey, []byte{queueStateVersion})
	}); err != nil {
		return err
	}
	return nil
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
		depth, err := countTimeAndIDIndex(ctx, it, count.prefix)
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
		age, err := oldestIndexAge(ctx, it, state.prefix, now)
		if err != nil {
			return queueSnapshot{}, err
		}
		*state.age = age
	}

	return snapshot, nil
}
func countTimeAndIDIndex(ctx context.Context, it *badger.Iterator, prefix []byte) (int64, error) {
	var count int64
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if err := ctxErr(ctx); err != nil {
			return 0, err
		}
		if _, _, err := parseTimeAndIDKey(prefix, it.Item().Key()); err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}
func oldestIndexAge(ctx context.Context, it *badger.Iterator, prefix []byte, now time.Time) (time.Duration, error) {
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}
	it.Seek(prefix)
	if !it.ValidForPrefix(prefix) {
		return 0, fmt.Errorf("%w: nonempty queue has no creation key under %q", ErrInconsistentIndex, prefix)
	}
	createdAt, _, err := parseTimeAndIDKey(prefix, it.Item().Key())
	if err != nil {
		return 0, err
	}
	return positiveDuration(now.Sub(createdAt)), nil
}
