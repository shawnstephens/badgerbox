package badgerbox

import "encoding/json"

// Codec serializes and deserializes a generic type for durable storage.
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
