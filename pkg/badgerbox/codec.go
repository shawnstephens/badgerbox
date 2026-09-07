package badgerbox

import (
	"encoding/json"
	"fmt"
)

// Codec serializes and deserializes a generic type for durable storage.
// Implementations must be safe for concurrent calls. Encoded bytes are opaque;
// reopen each namespace with codecs compatible with its persisted records.
// Unmarshal errors and panics during a claim permanently dead-letter that record
// so healthy messages can continue. Inspect ListDeadLetterMetadata and requeue
// the exact dead letter after restoring codec compatibility. Metadata corruption
// remains a storage error and stops processing.
type Codec[T any] interface {
	Marshal(T) ([]byte, error)
	Unmarshal([]byte) (T, error)
}

type Serde[M any, D any] struct {
	Message     Codec[M]
	Destination Codec[D]
}

// JSONCodec is the default codec used by the store when no codec is supplied.
type JSONCodec[T any] struct{}

func (JSONCodec[T]) Marshal(value T) ([]byte, error) {
	return json.Marshal(value)
}

func (JSONCodec[T]) Unmarshal(data []byte) (T, error) {
	var value T
	if len(data) == 0 {
		return value, nil
	}

	err := json.Unmarshal(data, &value)
	return value, err
}

// decodeWithCodec isolates untrusted application decoding from persisted bytes.
// A codec may mutate its input before failing or returning an aliased value; it
// must never change the opaque bytes used for retries or codec quarantine.
func decodeWithCodec[T any](codec Codec[T], data []byte, field string) (value T, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %s codec panic: %v", ErrCodecDecode, field, recovered)
		}
	}()
	value, err = codec.Unmarshal(cloneBytes(data))
	if err != nil {
		err = fmt.Errorf("%w: %s: %w", ErrCodecDecode, field, err)
	}
	return value, err
}
