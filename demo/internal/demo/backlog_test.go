package demo

import (
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestCountReadyMessagesCountsNamespaceReadyKeys(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "badger")
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte("ob/demo/ready/0001"), nil); err != nil {
			return err
		}
		if err := txn.Set([]byte("ob/demo/ready/0002"), nil); err != nil {
			return err
		}
		if err := txn.Set([]byte("ob/other/ready/0001"), nil); err != nil {
			return err
		}
		if err := txn.Set([]byte("ob/demo/msg/0001"), []byte("message")); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	count, err := CountReadyMessages(db, "demo")
	if err != nil {
		t.Fatalf("CountReadyMessages() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("CountReadyMessages() = %d, want 2", count)
	}
}
