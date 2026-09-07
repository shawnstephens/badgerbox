# Admission under resource pressure

## Atomic namespace quotas

Set `Options.AdmissionLimits` when creating a namespace to cap retained message
count and canonical stored bytes. Each zero field explicitly means unlimited.
For example:

```go
store, err := badgerbox.New(db, serde, badgerbox.Options{
    Namespace: "orders",
    AdmissionLimits: badgerbox.AdmissionLimits{
        MaxRetainedMessages: 100_000,
        MaxRetainedBytes: 512 << 20,
    },
})
```

Limits and usage are persisted in the same transaction as the outbox write.
Concurrent stores and producer transactions cannot each spend the same capacity.
Both limits apply when set. Delayed messages, active leases, retries, dead
letters, and quarantine all retain their original charge. Only successful
acknowledgement releases it; moving failed messages to a dead-letter queue does
not make room for unlimited new failures. Transaction rollback preserves both
application state and capacity. Badger conflict detection must remain enabled.

The byte charge is the size of a canonical JSON record, including payload and
destination base64, trace context, ID, and creation time. Mutable retry, lease,
and availability fields are normalized so the charge does not change during a
message's lifetime. Empty payloads still cost bytes. Indexes, error text, storage
versions, and Badger/filesystem overhead are excluded. This is a logical backlog
budget, not a disk or decoded-memory limit.

Handle `errors.Is(err, badgerbox.ErrAdmissionLimit)` as backpressure. The wrapped
`*badgerbox.AdmissionLimitError` reports the resource (`messages` or `bytes`),
limit, current usage, and requested capacity. Retry with a bounded delay and
request deadline, or reject upstream. A request larger than the byte limit cannot
succeed merely by waiting. Producer concurrency also needs a bound: quota checks
occur after encoding the incoming message, and each in-flight request can hold
its own payload and encoded copies.

`store.Usage(ctx)` returns a consistent snapshot from one fixed-size metadata
record, without scanning messages or invoking codecs. Change limits atomically:

```go
usage, err := store.Usage(ctx)
if err != nil {
    return err
}
next := usage.Limits
next.MaxRetainedBytes = 1 << 30
err = store.CompareAndSwapAdmissionLimits(ctx, usage.Limits, next)
```

A concurrent configuration change returns `ErrAdmissionLimitsMismatch` with
the actual limits; refresh and reassess before retrying. All existing stores
enforce the new configuration on their next committed enqueue. Transactions
holding older metadata conflict and must retry. A limit may be lowered below
current usage: retained work remains intact, and new intake stops until enough
capacity is released. Zero disables that limit.

Every `New` call must supply the current persisted limits exactly; omitted
limits cannot bypass another store's configured policy. Update deployment
configuration after live tuning so reopening uses the same limits. An ordinary
snapshot followed by an enqueue cannot replace these transactional checks.

`EnqueueTx` must use a transaction from the same Badger database. Namespace
identity detects independently initialized foreign databases; it does not
distinguish cloned database images with identical metadata. Return any enqueue
error from the caller's transaction. After a transaction write error, discard the
whole transaction; committing partial outbox writes is not supported.

## Filesystem free-space guard

Use `Options.EnqueueGuard` to reject new intake when an external resource is
under pressure. The hook runs for both `Enqueue` and `EnqueueTx`, before ID
allocation, codec calls, or outbox transaction writes. It may run again on a
transaction conflict. Make custom hooks safe for concurrent calls and have them
respect context cancellation.

`pkg/admission.DiskGuard` checks available filesystem capacity. Pass both Badger
directories when they differ; the directories must already exist when checked.
For example, after opening the database:

```go
guard, err := admission.NewDiskGuard(admission.DiskGuardOptions{
    Paths: []string{db.Opts().Dir, db.Opts().ValueDir},
    MinFreeBytes: 2 << 30,
    RefreshInterval: 100 * time.Millisecond,
})
if err != nil {
    return err
}
store, err := badgerbox.New(db, serde, badgerbox.Options{
    EnqueueGuard: guard.Check,
})
```

The 2 GiB margin is an example to measure against your deployment. Include room
for concurrent writes, Badger compaction, value-log rewriting, and filesystem
overhead. A free-space observation does not reserve bytes: other writers can
consume space between the check and commit. Zero `RefreshInterval` takes a new
sample per call; overlapping calls share one probe. A positive interval also
caches failures until the sample expires. This trades probe overhead against
the time it takes admission to notice changing capacity.

`errors.Is(err, admission.ErrDiskPressure)` identifies insufficient capacity;
`errors.As` to `*admission.DiskPressureError` exposes the path and byte counts.
`admission.ErrDiskProbe` identifies a failed or invalid measurement and wraps
the underlying filesystem error where one exists. Both reject intake. A blocked
filesystem probe does not stop context cancellation: each guard permits only one
probe goroutine, and callers can time out while it remains blocked in the OS.

Do not acknowledge a producer request when its enqueue is rejected. In a
caller-owned transaction, return the error so the application state rolls back
with the outbox write:

```go
err := db.Update(func(txn *badger.Txn) error {
    if err := txn.Set(applicationKey, applicationValue); err != nil {
        return err
    }
    _, err := store.EnqueueTx(ctx, txn, request)
    return err
})
```

The guard cannot undo application writes that the caller chooses to commit
after a rejection. Guard rejection itself performs no outbox writes and leaves
the transaction usable. Avoid calling slow external services from a guard while
holding a transaction; the filesystem guard supports a short cached sample for
this reason.

Claiming, acknowledgement, retry, quarantine, and dead-letter requeue bypass
the intake guard so retained messages can continue toward settlement. They still
need disk workspace and can fail if the volume fills. The guard automatically
admits new messages again after a fresh healthy sample. Apply one shared guard
to every producer store using the protected volumes; it is an advisory process
policy, not persistent namespace configuration.
