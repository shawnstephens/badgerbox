package demo

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

func CountReadyMessages(db *badger.DB, namespace string) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("count ready messages: db is nil")
	}

	prefix := []byte("ob/" + namespace + "/ready/")
	count := 0
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("count ready messages: %w", err)
	}
	return count, nil
}
