package badgerbox

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// ErrMessageIDExhausted means no further ID range fits without overflow.
var ErrMessageIDExhausted = errors.New("badgerbox: message ID sequence exhausted")

// messageSequence uses Badger's eight-byte range-end encoding. Cached ranges
// become visible only after their reservation transaction successfully commits.
type messageSequence struct {
	mu        sync.Mutex
	db        *badger.DB
	key       []byte
	bandwidth uint64
	next      uint64
	leased    uint64
}

func newMessageSequence(db *badger.DB, key []byte, bandwidth uint64) (*messageSequence, error) {
	seq := &messageSequence{db: db, key: append([]byte(nil), key...), bandwidth: bandwidth}
	if err := seq.reserve(db.Update); err != nil {
		return nil, err
	}
	return seq, nil
}

func (s *messageSequence) Next() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next >= s.leased {
		if err := s.reserve(s.db.Update); err != nil {
			return 0, err
		}
	}
	id := s.next
	s.next++
	return id, nil
}

func (s *messageSequence) reserve(update func(func(*badger.Txn) error) error) error {
	var next, leased uint64
	err := update(func(txn *badger.Txn) error {
		var err error
		next, err = loadSequenceEnd(txn, s.key)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if s.bandwidth == 0 || next > math.MaxUint64-s.bandwidth {
			return ErrMessageIDExhausted
		}
		leased = next + s.bandwidth
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], leased)
		return txn.Set(s.key, value[:])
	})
	if err != nil {
		return err
	}
	s.next, s.leased = next, leased
	return nil
}

func (s *messageSequence) Release() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.db.Update(func(txn *badger.Txn) error {
		end, err := loadSequenceEnd(txn, s.key)
		if err != nil {
			return err
		}
		if end != s.leased {
			return nil
		}
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], s.next)
		return txn.Set(s.key, value[:])
	})
	if err == nil {
		s.leased = s.next
	}
	return err
}

func loadSequenceEnd(txn *badger.Txn, key []byte) (uint64, error) {
	item, err := txn.Get(key)
	if err != nil {
		return 0, err
	}
	var end uint64
	err = item.Value(func(value []byte) error {
		if len(value) != 8 {
			return fmt.Errorf("badgerbox: invalid message ID sequence length: %d", len(value))
		}
		end = binary.BigEndian.Uint64(value)
		return nil
	})
	return end, err
}
