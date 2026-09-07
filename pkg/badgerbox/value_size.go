package badgerbox

import "github.com/dgraph-io/badger/v4"

// storedValueUpperBound applies to committed items. Badger v4's ValueSize is
// exact for inline values, but subtracts an approximate six-byte header for
// value-log pointers. The real header is at least five bytes, so that estimate
// can undercount by one. EstimatedSize includes log framing only for pointers.
// A conservative extra byte there prevents a first oversize value from loading.
// Keep the low-threshold regression tests when upgrading Badger. Pending writes
// have no size metadata and must be budgeted from their already-encoded bytes.
func storedValueUpperBound(item *badger.Item) int64 {
	size := item.ValueSize()
	if item.EstimatedSize() > item.KeySize()+size {
		size++
	}
	return size
}

func copyStoredValue(item *badger.Item) ([]byte, error) {
	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	if int64(len(value)) > storedValueUpperBound(item) {
		return nil, boxErrorf("Badger value exceeds metadata size bound")
	}
	return value, nil
}
