# Admission under resource pressure

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
