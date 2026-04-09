package badgerbox

import (
	"encoding/binary"
	"fmt"
	"time"
)

type keyspace struct {
	namespace        string
	messagePrefix    []byte
	readyPrefix      []byte
	processingPrefix []byte
	deadLetterPrefix []byte
	sequenceKey      []byte
}

func newKeyspace(namespace string) keyspace {
	base := "ob/" + namespace + "/"
	return keyspace{
		namespace:        namespace,
		messagePrefix:    []byte(base + "msg/"),
		readyPrefix:      []byte(base + "ready/"),
		processingPrefix: []byte(base + "processing/"),
		deadLetterPrefix: []byte(base + "dlq/"),
		sequenceKey:      []byte(base + "seq/message-id"),
	}
}

func (k keyspace) messageKey(id MessageID) []byte {
	return appendBinarySuffix(k.messagePrefix, encodeUint64(uint64(id)))
}

func (k keyspace) readyKey(availableAt time.Time, id MessageID) []byte {
	return appendTimeAndID(k.readyPrefix, availableAt.UnixNano(), uint64(id))
}

func (k keyspace) processingKey(leaseUntil time.Time, id MessageID) []byte {
	return appendTimeAndID(k.processingPrefix, leaseUntil.UnixNano(), uint64(id))
}

func (k keyspace) deadLetterKey(failedAt time.Time, id MessageID) []byte {
	return appendTimeAndID(k.deadLetterPrefix, failedAt.UnixNano(), uint64(id))
}

func appendBinarySuffix(prefix []byte, suffix [8]byte) []byte {
	key := make([]byte, len(prefix)+len(suffix))
	copy(key, prefix)
	copy(key[len(prefix):], suffix[:])
	return key
}

func appendTimeAndID(prefix []byte, ts int64, id uint64) []byte {
	timePart := encodeOrderedInt64(ts)
	idPart := encodeUint64(id)
	key := make([]byte, len(prefix)+8+1+8)
	copy(key, prefix)
	copy(key[len(prefix):], timePart[:])
	key[len(prefix)+8] = '/'
	copy(key[len(prefix)+9:], idPart[:])
	return key
}

func parseMessageKey(prefix, key []byte) (MessageID, error) {
	if len(key) != len(prefix)+8 {
		return 0, fmt.Errorf("badgerbox: invalid message key length")
	}
	if string(key[:len(prefix)]) != string(prefix) {
		return 0, fmt.Errorf("badgerbox: invalid message key prefix")
	}
	return MessageID(binary.BigEndian.Uint64(key[len(prefix):])), nil
}

func parseTimeAndIDKey(prefix, key []byte) (time.Time, MessageID, error) {
	if len(key) != len(prefix)+8+1+8 {
		return time.Time{}, 0, fmt.Errorf("badgerbox: invalid indexed key length")
	}
	if string(key[:len(prefix)]) != string(prefix) {
		return time.Time{}, 0, fmt.Errorf("badgerbox: invalid indexed key prefix")
	}
	if key[len(prefix)+8] != '/' {
		return time.Time{}, 0, fmt.Errorf("badgerbox: invalid indexed key separator")
	}

	ts := decodeOrderedInt64(key[len(prefix) : len(prefix)+8])
	id := MessageID(binary.BigEndian.Uint64(key[len(prefix)+9:]))
	return time.Unix(0, ts).UTC(), id, nil
}

func encodeOrderedInt64(value int64) [8]byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], uint64(value)^(1<<63))
	return data
}

func decodeOrderedInt64(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data) ^ (1 << 63))
}

func encodeUint64(value uint64) [8]byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], value)
	return data
}
