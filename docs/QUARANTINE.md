# Byte-bounded claims and quarantine recovery

Set `ProcessorOptions.ClaimMaxBytes` to bound the sum of stored source-envelope
bytes loaded by each claim transaction. Zero disables this limit; negative
values are rejected. Both the single-message processor and batch processor
support it. `ClaimBatchSize` independently bounds the batch's record count.

Badger's size metadata is checked before copying or decoding a source value.
Value-log estimates use a conservative upper bound, so a batch can stop slightly
short of the configured byte limit. There is no exception that loads a first
oversized record anyway. These limits control stored bytes, not decoded objects,
Go heap, or RSS. Budget for JSON/base64 decoding, codec expansion, callback/client
copies, and concurrent workers. An application codec can allocate or block
arbitrarily; the library cannot impose a memory or execution limit on that code.

A record larger than the entire claim budget enters referenced quarantine in
one transaction. Its source remains untouched, and bounded metadata replaces
its ready scheduling entry. The processor continues to healthy records. The
retained source still occupies its original admission quota, and its old
creation index is treated as auxiliary metadata rather than live queue depth.
`Get` returns `ErrMessageQuarantined` before loading it. The dead-letter counter
records `failure_kind=claim_bytes`; no delivery callback runs for that record.
If a callback from an earlier, expired lease finishes after quarantine, its
settlement is a lost-ownership no-op and does not release admission capacity.

If an in-budget source has a valid storage envelope but an application codec
returns an error or panics, the processor moves it to an ordinary permanent
dead letter and continues. Codec input is copied so a mutating decoder cannot
corrupt the preserved payload or destination. The attempt count advances, and
the dead-letter counter records `failure_kind=codec`. Callback-attempt/success
metrics do not report an invocation that never happened. Corrupt storage
metadata that is actually read still returns an error for investigation.

## Inspect before replay

Use `ListDeadLetterMetadata` or the admin HTTP dead-letter list to inspect
failure summaries without invoking application codecs. For a referenced source,
`QuarantinedSource` supplies its stored-size upper bound and bounded failure
text. Ordinary decoded `Details` are absent: source identity remains provisional
until the source envelope is loaded and validated. The reference and its source
have separate stored-byte counts. If even the reference exceeds the page budget,
the list still returns key-only metadata and a cursor for pagination.

Fix the codec or raise the processor's claim budget before replaying. For a
bounded exact replay, use:

```go
err := store.RequeueDeadLetterWithOptions(ctx, id, failedAt,
    badgerbox.DeadLetterRequeueOptions{
        AvailableAt: availableAt,
        MaxBytes: 8 << 20,
    })
```

The replay budget includes the reference and source values. Insufficient budget
returns `ErrDeadLetterTooLarge` (HTTP 413), with both data and quota unchanged.
Exact replay validates the dead-letter key, marker, source version, identity,
state, and scheduling metadata before restoring ready work. A source changed
behind the library's back fails closed, even if the replacement has the same
size. Replay bypasses producer admission because it retains the same message;
only eventual successful acknowledgement releases capacity.

Full payload dead-letter listing also applies its byte budget to both reference
and source. It invokes the configured codecs, so a still-broken codec can make
full listing fail; metadata inspection remains available. `Audit` validates
quarantine references and counts each retained source once for quota
reconciliation. Its global scan budgets still apply. Interpret audit results
only when `Complete` is true; an oversized or malformed source can prevent a
complete audit until an operator provides an appropriate budget or repairs it.

Choose the claim budget with headroom for retry-envelope growth. A record that
fit its first claim can exceed a very tight limit after lifecycle fields grow.
It remains recoverable through the same inspection and exact replay path. No
quarantine path silently deletes undelivered data or frees its admission charge.
