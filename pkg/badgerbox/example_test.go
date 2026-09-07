package badgerbox_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
)

type opaqueCodec struct{}

func (opaqueCodec) Marshal(v []byte) ([]byte, error)   { return bytes.Clone(v), nil }
func (opaqueCodec) Unmarshal(v []byte) ([]byte, error) { return bytes.Clone(v), nil }
func ExampleNew() {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// The payload uses a binary codec; the destination independently defaults to JSON.
	store, err := badgerbox.New[[]byte, string](db, badgerbox.Serde[[]byte, string]{Message: opaqueCodec{}}, badgerbox.Options{Namespace: "binary-example"})
	if err != nil {
		panic(err)
	}
	defer store.Close()
	ctx := context.Background()
	id, err := store.Enqueue(ctx, badgerbox.EnqueueRequest[[]byte, string]{Payload: []byte{255, 0}, Destination: "sink"})
	if err != nil {
		panic(err)
	}
	message, err := store.Get(ctx, id)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%x %s\n", message.Payload, message.Destination)
	// Output: ff00 sink
}
