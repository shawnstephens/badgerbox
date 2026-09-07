package badgerbox

import (
	"encoding/json"
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
