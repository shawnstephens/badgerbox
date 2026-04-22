package badgerbox

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	queueStateVersion    = byte(1)
	queueStateShardCount = 64
)

type queueStateCountDeltas struct {
	ready      [queueStateShardCount]int64
	processing [queueStateShardCount]int64
	deadLetter [queueStateShardCount]int64
}

type queueStateCounterValues struct {
	ready      [queueStateShardCount]uint64
	processing [queueStateShardCount]uint64
	deadLetter [queueStateShardCount]uint64
}

func (d *queueStateCountDeltas) addReady(id MessageID, delta int64) {
	d.ready[queueStateCounterShard(id)] += delta
}

func (d *queueStateCountDeltas) addProcessing(id MessageID, delta int64) {
	d.processing[queueStateCounterShard(id)] += delta
}

func (d *queueStateCountDeltas) addDeadLetter(id MessageID, delta int64) {
	d.deadLetter[queueStateCounterShard(id)] += delta
}

func queueStateCounterShard(id MessageID) byte {
	return byte(uint64(id) % queueStateShardCount)
}

func (s *Store[M, D]) initializeQueueState() error {
	stateReady, err := s.loadQueueStateStatus()
	if err != nil {
		return err
	}
	s.queueStateReady.Store(stateReady)
	return nil
}

func (s *Store[M, D]) loadQueueStateStatus() (bool, error) {
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
				return fmt.Errorf("badgerbox: unsupported queue-state metadata version")
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
			s.keys.messagePrefix,
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
		return false, err
	}
	if versionPresent {
		return true, nil
	}
	if namespaceHasData {
		return false, nil
	}

	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.keys.queueStateVersionKey, []byte{queueStateVersion})
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store[M, D]) queueStateEnabled() bool {
	return s.queueStateReady.Load()
}

func (s *Store[M, D]) applyQueueCountDeltas(txn *badger.Txn, deltas queueStateCountDeltas) error {
	if err := s.applyCounterDeltas(txn, s.keys.readyCountPrefix, deltas.ready); err != nil {
		return err
	}
	if err := s.applyCounterDeltas(txn, s.keys.processingCountPrefix, deltas.processing); err != nil {
		return err
	}
	if err := s.applyCounterDeltas(txn, s.keys.deadLetterCountPrefix, deltas.deadLetter); err != nil {
		return err
	}
	return nil
}

func (s *Store[M, D]) applyCounterDeltas(txn *badger.Txn, prefix []byte, deltas [queueStateShardCount]int64) error {
	for shard, delta := range deltas {
		if delta == 0 {
			continue
		}
		if err := adjustUint64Counter(txn, appendShardSuffix(prefix, byte(shard)), delta); err != nil {
			return err
		}
	}
	return nil
}

func adjustUint64Counter(txn *badger.Txn, key []byte, delta int64) error {
	if delta == 0 {
		return nil
	}

	current, err := loadUint64Counter(txn, key)
	if err != nil {
		return err
	}

	next := int64(current) + delta
	if next < 0 {
		return fmt.Errorf("badgerbox: queue-state counter underflow")
	}
	if next == 0 {
		if err := txn.Delete(key); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	}

	encoded := encodeUint64(uint64(next))
	return txn.Set(key, encoded[:])
}

func loadUint64Counter(txn *badger.Txn, key []byte) (uint64, error) {
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	value, err := item.ValueCopy(nil)
	if err != nil {
		return 0, err
	}
	if len(value) != 8 {
		return 0, fmt.Errorf("badgerbox: invalid queue-state counter value")
	}
	return binary.BigEndian.Uint64(value), nil
}

func (s *Store[M, D]) loadQueueStateCounters(txn *badger.Txn) (queueSnapshot, error) {
	var snapshot queueSnapshot

	for shard := 0; shard < queueStateShardCount; shard++ {
		ready, err := loadUint64Counter(txn, s.keys.readyCountShardKey(byte(shard)))
		if err != nil {
			return queueSnapshot{}, err
		}
		processing, err := loadUint64Counter(txn, s.keys.processingCountShardKey(byte(shard)))
		if err != nil {
			return queueSnapshot{}, err
		}
		deadLetter, err := loadUint64Counter(txn, s.keys.deadLetterCountShardKey(byte(shard)))
		if err != nil {
			return queueSnapshot{}, err
		}

		snapshot.ReadyDepth += int64(ready)
		snapshot.ProcessingDepth += int64(processing)
		snapshot.DeadLetterDepth += int64(deadLetter)
	}

	return snapshot, nil
}

func (s *Store[M, D]) oldestReadyAgeFromCreatedIndex(ctx context.Context, txn *badger.Txn, now time.Time) (time.Duration, error) {
	return s.oldestAgeFromCreatedIndex(ctx, txn, s.keys.readyCreatedPrefix, func(txn *badger.Txn, createdAt time.Time, id MessageID) (bool, error) {
		record, err := s.loadRecord(txn, id)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if record.Status != recordStatusPending || record.CreatedAtUnix != createdAt.UnixNano() {
			return false, nil
		}
		_, err = txn.Get(s.keys.readyKey(time.Unix(0, record.AvailableAtUnix).UTC(), id))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}, now)
}

func (s *Store[M, D]) oldestProcessingAgeFromCreatedIndex(ctx context.Context, txn *badger.Txn, now time.Time) (time.Duration, error) {
	return s.oldestAgeFromCreatedIndex(ctx, txn, s.keys.processingCreatedPrefix, func(txn *badger.Txn, createdAt time.Time, id MessageID) (bool, error) {
		record, err := s.loadRecord(txn, id)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if record.Status != recordStatusProcessing || record.CreatedAtUnix != createdAt.UnixNano() {
			return false, nil
		}

		item, err := txn.Get(s.keys.processingKey(time.Unix(0, record.LeaseUntilUnix).UTC(), id))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		tokenBytes, err := item.ValueCopy(nil)
		if err != nil {
			return false, err
		}
		if record.LeaseToken != string(tokenBytes) {
			return false, nil
		}
		return true, nil
	}, now)
}

func (s *Store[M, D]) oldestAgeFromCreatedIndex(ctx context.Context, txn *badger.Txn, prefix []byte, validate func(*badger.Txn, time.Time, MessageID) (bool, error), now time.Time) (time.Duration, error) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if err := ctxErr(ctx); err != nil {
			return 0, err
		}

		key := it.Item().KeyCopy(nil)
		createdAt, id, err := parseTimeAndIDKey(prefix, key)
		if err != nil {
			return 0, err
		}

		valid, err := validate(txn, createdAt, id)
		if err != nil {
			return 0, err
		}
		if valid {
			return positiveDuration(now.Sub(createdAt.UTC())), nil
		}
	}

	return 0, nil
}

func (s *Store[M, D]) RepairQueueState(ctx context.Context) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctxErr(ctx); err != nil {
		return err
	}

	if err := s.db.DropPrefix(s.keys.queueStatePrefix); err != nil {
		return err
	}

	var counts queueStateCounterValues
	writeBatch := s.db.NewWriteBatch()
	defer writeBatch.Cancel()

	if err := s.repairReadyQueueState(ctx, writeBatch, &counts); err != nil {
		return err
	}
	if err := s.repairProcessingQueueState(ctx, writeBatch, &counts); err != nil {
		return err
	}
	if err := s.repairDeadLetterQueueState(ctx, &counts); err != nil {
		return err
	}
	if err := writeBatch.Flush(); err != nil {
		return err
	}

	if err := s.db.Update(func(txn *badger.Txn) error {
		if err := s.storeQueueStateCounters(txn, counts); err != nil {
			return err
		}
		return txn.Set(s.keys.queueStateVersionKey, []byte{queueStateVersion})
	}); err != nil {
		return err
	}

	s.queueStateReady.Store(true)
	return nil
}

func (s *Store[M, D]) repairReadyQueueState(ctx context.Context, writeBatch *badger.WriteBatch, counts *queueStateCounterValues) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(s.keys.readyPrefix); it.ValidForPrefix(s.keys.readyPrefix); it.Next() {
			if err := ctxErr(ctx); err != nil {
				return err
			}

			key := it.Item().KeyCopy(nil)
			availableAt, id, err := parseTimeAndIDKey(s.keys.readyPrefix, key)
			if err != nil {
				return err
			}

			record, err := s.loadRecord(txn, id)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if record.Status != recordStatusPending || record.AvailableAtUnix != availableAt.UnixNano() {
				continue
			}

			if err := writeBatch.Set(s.keys.readyCreatedKey(time.Unix(0, record.CreatedAtUnix).UTC(), id), emptyValue); err != nil {
				return err
			}
			counts.ready[queueStateCounterShard(id)]++
		}
		return nil
	})
}

func (s *Store[M, D]) repairProcessingQueueState(ctx context.Context, writeBatch *badger.WriteBatch, counts *queueStateCounterValues) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(s.keys.processingPrefix); it.ValidForPrefix(s.keys.processingPrefix); it.Next() {
			if err := ctxErr(ctx); err != nil {
				return err
			}

			key := it.Item().KeyCopy(nil)
			leaseUntil, id, err := parseTimeAndIDKey(s.keys.processingPrefix, key)
			if err != nil {
				return err
			}
			tokenBytes, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}

			record, err := s.loadRecord(txn, id)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if record.Status != recordStatusProcessing || record.LeaseToken != string(tokenBytes) || record.LeaseUntilUnix != leaseUntil.UnixNano() {
				continue
			}

			if err := writeBatch.Set(s.keys.processingCreatedKey(time.Unix(0, record.CreatedAtUnix).UTC(), id), emptyValue); err != nil {
				return err
			}
			counts.processing[queueStateCounterShard(id)]++
		}
		return nil
	})
}

func (s *Store[M, D]) repairDeadLetterQueueState(ctx context.Context, counts *queueStateCounterValues) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(s.keys.deadLetterPrefix); it.ValidForPrefix(s.keys.deadLetterPrefix); it.Next() {
			if err := ctxErr(ctx); err != nil {
				return err
			}

			key := it.Item().KeyCopy(nil)
			_, id, err := parseTimeAndIDKey(s.keys.deadLetterPrefix, key)
			if err != nil {
				return err
			}
			counts.deadLetter[queueStateCounterShard(id)]++
		}
		return nil
	})
}

func (s *Store[M, D]) storeQueueStateCounters(txn *badger.Txn, counts queueStateCounterValues) error {
	if err := s.storeCounterValues(txn, s.keys.readyCountPrefix, counts.ready); err != nil {
		return err
	}
	if err := s.storeCounterValues(txn, s.keys.processingCountPrefix, counts.processing); err != nil {
		return err
	}
	if err := s.storeCounterValues(txn, s.keys.deadLetterCountPrefix, counts.deadLetter); err != nil {
		return err
	}
	return nil
}

func (s *Store[M, D]) storeCounterValues(txn *badger.Txn, prefix []byte, counts [queueStateShardCount]uint64) error {
	for shard, count := range counts {
		if count == 0 {
			continue
		}
		encoded := encodeUint64(count)
		if err := txn.Set(appendShardSuffix(prefix, byte(shard)), encoded[:]); err != nil {
			return err
		}
	}
	return nil
}
