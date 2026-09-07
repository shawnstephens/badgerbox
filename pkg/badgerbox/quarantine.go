package badgerbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// quarantineReadyRecord uses the proposed claim's valid processing metadata,
// without writing its processing indexes. Decoding is an attempt, but no process
// callback ran. The complete disposition is atomic with other claims in the txn.
// Record bytes remain opaque; capacity moves to the DLQ without being released.
func (s *Store[M, D]) quarantineReadyRecord(txn *badger.Txn, record storedRecord, failedAt time.Time, cause error) error {
	encoded, err := json.Marshal(storedDeadLetter{
		Record: record, FailedAt: failedAt.UnixNano(),
		Error: storedFailureText(cause), Permanent: true,
	})
	if err != nil {
		return err
	}
	if err := txn.Set(s.keys.deadLetterKey(failedAt, record.ID), encoded); err != nil {
		return err
	}
	for _, key := range [][]byte{
		s.keys.messageKey(record.ID),
		s.keys.readyKey(time.Unix(0, record.AvailableAtUnix).UTC(), record.ID),
		s.keys.readyCreatedKey(time.Unix(0, record.CreatedAtUnix).UTC(), record.ID),
	} {
		if err := txn.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// A referenced quarantine retains the source under messageKey unchanged, because
// moving or rewriting it would read beyond the claim budget. Its old creation
// index remains auxiliary metadata; state observers exclude it via quarantineKey.
// ID/availability are provisional scheduling-index values until source validation.
type storedQuarantinedSource struct {
	ID              MessageID `json:"id"`
	StoredBytes     int64     `json:"stored_bytes"`
	Version         uint64    `json:"version"`
	AvailableAtUnix int64     `json:"available_at_unix_nano"`
}

func (d storedDeadLetter) MarshalJSON() ([]byte, error) {
	if d.QuarantinedSource == nil {
		type plain storedDeadLetter
		return json.Marshal(plain(d))
	}
	return json.Marshal(struct {
		Source    *storedQuarantinedSource `json:"quarantined_source"`
		FailedAt  int64                    `json:"failed_at_unix_nano"`
		Error     string                   `json:"error"`
		Permanent bool                     `json:"permanent"`
	}{d.QuarantinedSource, d.FailedAt, d.Error, d.Permanent})
}

func (s *Store[M, D]) quarantineOversizedRecord(txn *badger.Txn, id MessageID, availableAt, failedAt time.Time, storedBytes, limit int64, version uint64) error {
	key := s.keys.deadLetterKey(failedAt, id)
	encoded, err := json.Marshal(storedDeadLetter{
		QuarantinedSource: &storedQuarantinedSource{ID: id, StoredBytes: storedBytes, Version: version, AvailableAtUnix: availableAt.UnixNano()},
		FailedAt:          failedAt.UnixNano(), Error: claimSizeError(storedBytes, limit).Error(), Permanent: true,
	})
	if err != nil {
		return err
	}
	if err := txn.Set(key, encoded); err != nil {
		return err
	}
	if err := txn.Set(s.keys.quarantineKey(id), key); err != nil {
		return err
	}
	return txn.Delete(s.keys.readyKey(availableAt, id))
}

func claimSizeError(size, limit int64) error {
	return fmt.Errorf("%w: stored bytes %d > %d", ErrClaimTooLarge, size, limit)
}

// quarantineDLQKey never reads a source value. Marker length is checked before
// copying, so even a malformed marker cannot defeat a caller's payload budget.
func (s *Store[M, D]) quarantineDLQKey(txn *badger.Txn, id MessageID) ([]byte, error) {
	item, err := txn.Get(s.keys.quarantineKey(id))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if storedValueUpperBound(item) > int64(len(s.keys.deadLetterPrefix)+17+17) {
		return nil, fmt.Errorf("%w: invalid quarantine marker size", ErrInconsistentIndex)
	}
	key, err := copyStoredValue(item)
	if err != nil {
		return nil, err
	}
	_, markerID, err := parseTimeAndIDKey(s.keys.deadLetterPrefix, key)
	if err != nil || markerID != id {
		return nil, fmt.Errorf("%w: invalid quarantine marker identity", ErrInconsistentIndex)
	}
	return key, nil
}

func (s *Store[M, D]) referencedSourceItem(txn *badger.Txn, key []byte, stored storedDeadLetter) (*badger.Item, error) {
	ref := stored.QuarantinedSource
	failedAt, id, err := parseTimeAndIDKey(s.keys.deadLetterPrefix, key)
	if err != nil {
		return nil, err
	}
	if ref == nil || ref.ID != id || stored.FailedAt != failedAt.UnixNano() {
		return nil, fmt.Errorf("%w: quarantine key disagrees with reference", ErrInconsistentIndex)
	}
	marker, err := s.quarantineDLQKey(txn, id)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(marker, key) {
		return nil, fmt.Errorf("%w: quarantine reference has no matching marker", ErrInconsistentIndex)
	}
	item, err := txn.Get(s.keys.messageKey(id))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, fmt.Errorf("%w: quarantine source missing", ErrInconsistentIndex)
	}
	if err != nil {
		return nil, err
	}
	if item.Version() != ref.Version {
		return nil, fmt.Errorf("%w: quarantine source version differs from reference", ErrInconsistentIndex)
	}
	return item, nil
}

func validateQuarantineSource(record storedRecord, ref *storedQuarantinedSource) error {
	if record.ID != ref.ID || record.Status != recordStatusPending || record.AvailableAtUnix != ref.AvailableAtUnix {
		return fmt.Errorf("%w: quarantine source disagrees with scheduling metadata", ErrInconsistentIndex)
	}
	return nil
}
