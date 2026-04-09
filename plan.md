# `plan.md`: Generic Badger-backed outbox under `./pkg`

## Summary
Build a Go module named `badgerbox` that provides a durable outbox backed by `github.com/dgraph-io/badger/v4`. Place all library code under `./pkg`:
- core package: `./pkg/badgerbox`
- Kafka adapter package: `./pkg/kafkaoutbox`

The first version should support:
- an embedded producer API that writes messages into Badger
- an embedded processor that claims ready messages, runs a user-supplied generic `ProcessFunc`, and removes the message on success
- concurrent processing with a configurable worker pool
- at-least-once delivery with retries, lease-based recovery, and a dead-letter queue
- generic payload and destination typing so the core package is not Kafka-specific
- a first-party Kafka `ProcessFunc` adapter built on `github.com/twmb/franz-go/pkg/kgo`
- integration tests that start Kafka with Testcontainers for Go behind an `integration` build tag

Do not build a true multi-process live deployment in v1. Badger locks the DB directory, so an application and a separate worker process cannot both open the same DB path at the same time. Ship the processor as a library plus a small example worker binary that can run only when it has exclusive ownership of the DB.

## Public APIs and type design
Use generics at the core API boundary. The core `Message` should contain only outbox state plus typed payload and typed destination. Do not include Kafka fields in the core package.

Core package `pkg/badgerbox`:
- `type MessageID uint64`
- `type Codec[T any] interface { Marshal(T) ([]byte, error); Unmarshal([]byte) (T, error) }`
- `type JSONCodec[T any] struct{}`
- `type Serde[M any, D any] struct { Message Codec[M]; Destination Codec[D] }`
- `type Message[M any, D any] struct { ID MessageID; Payload M; Destination D; CreatedAt time.Time; AvailableAt time.Time; Attempt int; MaxAttempts int }`
- `type DeadLetter[M any, D any] struct { Message Message[M, D]; FailedAt time.Time; Error string; Permanent bool }`
- `type EnqueueRequest[M any, D any] struct { Payload M; Destination D; AvailableAt time.Time }`
- `type Options struct { Namespace string; IDLeaseSize uint64 }`
- `type ProcessFunc[M any, D any] func(ctx context.Context, msg Message[M, D]) error`
- `type ProcessorOptions struct { Concurrency int; ClaimBatchSize int; PollInterval time.Duration; LeaseDuration time.Duration; RetryBaseDelay time.Duration; RetryMaxDelay time.Duration; MaxAttempts int }`

Core constructors and methods:
- `func New[M any, D any](db *badger.DB, serde Serde[M, D], opts Options) (*Store[M, D], error)`
- `func (s *Store[M, D]) Close() error`
- `func (s *Store[M, D]) Enqueue(ctx context.Context, req EnqueueRequest[M, D]) (MessageID, error)`
- `func (s *Store[M, D]) EnqueueTx(ctx context.Context, txn *badger.Txn, req EnqueueRequest[M, D]) (MessageID, error)`
- `func (s *Store[M, D]) Get(ctx context.Context, id MessageID) (Message[M, D], error)`
- `func (s *Store[M, D]) ListDeadLetters(ctx context.Context, limit int, cursor []byte) ([]DeadLetter[M, D], []byte, error)`
- `func (s *Store[M, D]) RequeueDeadLetter(ctx context.Context, id MessageID, at time.Time) error`
- `func Permanent(err error) error`
- `func IsPermanent(err error) bool`
- `func NewProcessor[M any, D any](store *Store[M, D], fn ProcessFunc[M, D], opts ProcessorOptions) (*Processor[M, D], error)`
- `func (p *Processor[M, D]) Run(ctx context.Context) error`

Defaults:
- `Namespace="default"`
- `IDLeaseSize=128`
- `Concurrency=4`
- `ClaimBatchSize=32`
- `PollInterval=250 * time.Millisecond`
- `LeaseDuration=30 * time.Second`
- `RetryBaseDelay=1 * time.Second`
- `RetryMaxDelay=1 * time.Minute`
- `MaxAttempts=10`

Generic design rules:
- a single `Store[M, D]` instance handles one payload type `M` and one destination type `D`
- if an application needs multiple destination kinds in one queue, it should define an app-level union destination type such as `AppDestination`
- if an application needs heterogeneous payloads in one queue, it should define an app-level union payload type or use `json.RawMessage`
- the core outbox never interprets `M` or `D`; it only persists them through codecs and passes them to `ProcessFunc`

## Storage model and processing behavior
Use Badger `GetSequence` for message IDs. Gaps are acceptable. Release the sequence from `Store.Close()`.

Use sortable composite keys with big-endian encoded timestamps and IDs:
- `ob/<ns>/msg/<id>` stores the live record
- `ob/<ns>/ready/<availableAtUnixNano>/<id>` stores an empty value for scheduling
- `ob/<ns>/processing/<leaseUntilUnixNano>/<id>` stores the current lease token
- `ob/<ns>/dlq/<failedAtUnixNano>/<id>` stores the dead-letter record

Persist internal records as JSON envelopes that contain:
- outbox metadata fields
- `payload_bytes` containing `Serde.Message.Marshal(payload)`
- `destination_bytes` containing `Serde.Destination.Marshal(destination)`

Keep timestamps in UTC. If `AvailableAt` is zero on enqueue, set it to `time.Now().UTC()`.

Producer behavior:
- `Enqueue` writes the encoded message record and ready index in one Badger write transaction
- `EnqueueTx` writes into a caller-owned Badger transaction so app state and outbox state can commit atomically
- retry transaction closures on `badger.ErrConflict`
- use synchronous commit semantics only

Processor behavior:
- run one dispatcher goroutine and `Concurrency` worker goroutines
- only the dispatcher scans Badger for ready work
- dispatcher wakes on a poll ticker and a buffered notify channel triggered by enqueue
- dispatcher scans the `ready` prefix in lexicographic order with `PrefetchValues=false`
- dispatcher claims up to `ClaimBatchSize` messages where `AvailableAt <= now`
- claim in a write transaction: load the record, verify it is still pending, increment `Attempt`, set `leaseUntil=now+LeaseDuration`, generate a lease token, delete the ready key, and create the processing key
- workers call `ProcessFunc(ctx, msg)` with fully decoded `M` and `D`
- recover worker panics and treat them as failures
- on success, delete the live message and processing key only if the stored lease token still matches
- on retryable failure, compute backoff `min(base*2^(attempt-1), max)`, update `AvailableAt`, remove processing state, and recreate the ready key
- on permanent failure or when `Attempt >= MaxAttempts`, write a DLQ record and delete the live message
- if lease ownership changed before ack or nack, treat the worker result as stale and do nothing

Recovery behavior:
- run a reaper loop on the same poll interval
- scan `processing` keys with `leaseUntil <= now`
- atomically move expired messages back to `ready` with `AvailableAt=now`
- document clearly that handlers must be idempotent because lease expiry can cause duplicate delivery

Do not use Badger TTL for scheduling or retention. Keep scheduling explicit in record data and indexes.

## Kafka adapter and Kafka-specific types
Create package `pkg/kafkaoutbox` with Kafka-only types and adapter logic.

Kafka-specific structs:
- `type KafkaDestination struct { Topic string; Partition *int32 }`
- `type KafkaMessage struct { Key []byte; Headers map[string][]byte; Value []byte }`
- `type Options struct{}`

Kafka adapter API:
- `func NewProcessFunc(client *kgo.Client, opts Options) badgerbox.ProcessFunc[KafkaMessage, KafkaDestination]`

Kafka adapter behavior:
- map `KafkaDestination.Topic` into `kgo.Record.Topic`
- if `Partition` is non-nil, set `kgo.Record.Partition`
- map `KafkaMessage.Key`, `Headers`, and `Value` into the Kafka record
- call Franz-go synchronously so the outbox only acks after Kafka confirms publish
- return retryable errors for transient produce failures
- preserve `badgerbox.Permanent(err)` behavior if callers choose to wrap adapter errors

Do not add Kafka concepts to `pkg/badgerbox`. All Kafka routing, keys, headers, and record shaping belong in `pkg/kafkaoutbox`.

Suggested file layout:
- `pkg/badgerbox/message.go`
- `pkg/badgerbox/codec.go`
- `pkg/badgerbox/store.go`
- `pkg/badgerbox/processor.go`
- `pkg/badgerbox/keys.go`
- `pkg/badgerbox/errors.go`
- `pkg/badgerbox/retry.go`
- `pkg/kafkaoutbox/types.go`
- `pkg/kafkaoutbox/process_func.go`

## Documentation and test plan
Create a `README.md` that includes:
- import examples using `.../pkg/badgerbox` and `.../pkg/kafkaoutbox`
- a generic producer example with custom types such as `OrderEvent` and `HTTPDestination`
- an enqueue-within-transaction example
- an embedded processor example with a custom generic `ProcessFunc`
- a Kafka example using `badgerbox.Store[KafkaMessage, KafkaDestination]` plus `kafkaoutbox.NewProcessFunc`
- an explanation of at-least-once delivery
- an explanation of Badger’s single-process lock constraint
- a note that handlers must be idempotent
- a note that Badger value-log GC remains an operational responsibility of the caller
- a note that Kafka integration tests require Docker and `go test -tags=integration ./...`

Test plan:
- use real Badger instances in temporary directories for unit and integration tests
- run the suite with `-race`
- place Kafka integration tests under an `integration` build tag
- use `github.com/testcontainers/testcontainers-go/modules/kafka`
- start Kafka with the Testcontainers Kafka module and fetch broker addresses from `container.Brokers(ctx)`
- create a Franz-go client against those brokers

Core test scenarios:
- enqueue persists a typed message and creates the correct ready index
- `EnqueueTx` commits atomically with caller-owned writes
- `JSONCodec[T]` round-trips representative message and destination structs
- custom codecs can be plugged in for non-JSON encoding
- messages are claimed in `AvailableAt` then `ID` order
- concurrent workers process distinct messages without double-claiming while leases remain valid
- successful processing deletes the live record and indexes
- retryable failures reschedule with the configured backoff
- `Permanent(err)` sends a message directly to the DLQ
- hitting `MaxAttempts` sends a message to the DLQ
- expired leases are reclaimed and redelivered
- worker panic is recovered and treated as a failure
- reopening the DB after shutdown preserves pending messages
- multiple namespaces coexist correctly in one DB
- stale worker completions after lease expiry do not corrupt current state

Kafka adapter test scenarios:
- `KafkaDestination` requires `Topic`
- adapter maps `KafkaMessage` fields into the produced Kafka record shape
- end-to-end integration test enqueues `KafkaMessage` and `KafkaDestination`, runs the processor, and consumes the produced record from Kafka
- retry-path integration test fails the first processing attempt before eventual success
- Docker-unavailable failures may skip, but the tests must not silently downgrade into unit tests

## Assumptions
- v1 is designed for a single live process owning the Badger DB
- the example worker binary is for exclusive-DB operation only, not shared live deployment
- one outbox instance is strongly typed as `Store[M, D]`
- mixed destination kinds in one queue require an application-defined union destination type
- mixed payload kinds in one queue require an application-defined union payload type or `json.RawMessage`
- the built-in Kafka adapter targets `github.com/twmb/franz-go/pkg/kgo`
