package badgerbox

import (
	"context"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"time"
)

func (s *Store[M, D]) queueSnapshotLegacy(ctx context.Context) (queueSnapshot, error) {
	if err := s.ensureOpen(); err != nil {
		return queueSnapshot{}, err
	}
	if err := ctxErr(ctx); err != nil {
		return queueSnapshot{}, err
	}

	var snapshot queueSnapshot
	now := s.runtime.Now().UTC()
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		var oldestReadyCreated int64
		var oldestProcessingCreated int64

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

			snapshot.ReadyDepth++
			if oldestReadyCreated == 0 || record.CreatedAtUnix < oldestReadyCreated {
				oldestReadyCreated = record.CreatedAtUnix
			}
		}

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

			snapshot.ProcessingDepth++
			if oldestProcessingCreated == 0 || record.CreatedAtUnix < oldestProcessingCreated {
				oldestProcessingCreated = record.CreatedAtUnix
			}
		}

		for it.Seek(s.keys.deadLetterPrefix); it.ValidForPrefix(s.keys.deadLetterPrefix); it.Next() {
			if err := ctxErr(ctx); err != nil {
				return err
			}
			snapshot.DeadLetterDepth++
		}

		if oldestReadyCreated > 0 {
			snapshot.OldestReadyAge = now.Sub(time.Unix(0, oldestReadyCreated).UTC())
		}
		if oldestProcessingCreated > 0 {
			snapshot.OldestProcessingAge = now.Sub(time.Unix(0, oldestProcessingCreated).UTC())
		}
		return nil
	})
	return snapshot, err
}
