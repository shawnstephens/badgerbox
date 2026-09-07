# Storage and codecs

Badgerbox format version 2 uses ready and processing records with opaque
`payload_bytes` and `destination_bytes` fields in a JSON metadata envelope.
The envelope base64-encodes arbitrary codec output; it does not require JSON
payloads. Each omitted codec independently defaults to JSON. Codecs must support
concurrent calls and remain compatible when reopening a namespace. No codec
autodetection or conversion is performed.

Existing namespaces without the current format marker are rejected before
message-ID allocation or record changes. This pre-release version provides no
automatic migration: drain older databases using their original application
before switching to a new directory. Never delete an undrained database.

Metadata validation is independent of codecs. Nil and empty codec output are
allowed. Message and destination types, codec validation, and schema evolution
remain the embedding application's responsibility.

Use `WithSyncWrites(true)` when acknowledged commits must synchronize to storage;
Badger's default is false. `EnqueueTx` is only committed after its caller commits
the transaction. Synchronization depends on the filesystem and device; process
SIGKILL tests do not simulate host failure or power loss. See the
[durability and capacity guide](TUNING.md).

Successful processing removes live records and indexes logically. Badger retains
obsolete versions until compaction and value-log GC reclaim them. Use the runner's
maintenance service or own a joined GC loop, reserve temporary rewrite space, and
monitor every volume used by `Dir` and `ValueDir`. There is no automatic backlog
quota or dead-letter retention limit. Enqueue errors must propagate to callers;
operators should stop intake before disk exhaustion.
