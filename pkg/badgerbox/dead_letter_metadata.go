package badgerbox

import (
	"bytes"
	"context"
	"time"
	"unicode/utf8"

	"github.com/dgraph-io/badger/v4"
)

const defaultDeadLetterMetadataBytes int64 = 2 << 20
const maxDeadLetterMetadataRows = 1000
const maxDeadLetterFailureTextBytes = 1024

// DeadLetterMetadata identifies an exact stored dead letter without application
// decoding. Oversized rows have nil Details; their identity and Cursor remain usable.
type DeadLetterMetadata struct {
	ID          MessageID
	FailedAt    time.Time
	StoredBytes int64
	Oversized   bool
	Cursor      []byte
	Details     *DeadLetterDetails
	// QuarantinedSource describes a referenced value without reading it. Details
	// is nil because its storage envelope has not been validated.
	QuarantinedSource *QuarantinedSourceMetadata
}

// QuarantinedSourceMetadata is bounded reference metadata. Identity comes from
// the old scheduling index and remains provisional until a source read validates
// the envelope. StoredBytes is additional to DeadLetterMetadata.StoredBytes and
// conservatively accounts for Badger's approximate value-log size metadata.
type QuarantinedSourceMetadata struct {
	StoredBytes          int64
	FailureText          string
	FailureTextTruncated bool
	Permanent            bool
}

// DeadLetterDetails contains only validated storage-envelope metadata.
type DeadLetterDetails struct {
	State                MessageState
	CreatedAt            time.Time
	AvailableAt          time.Time
	Attempt              int
	MaxAttempts          int
	FailureText          string
	FailureTextTruncated bool
	Permanent            bool
}

// ListDeadLetterMetadata returns at most 1000 codec-free summaries. MaxBytes zero
// uses 2 MiB of stored values per page. Records larger than that entire budget
// yield key-only entries without loading their values. Each entry's Cursor can
// resume after that entry; the returned next cursor is nil at the end.
func (s *Store[M, D]) ListDeadLetterMetadata(ctx context.Context, options DeadLetterListOptions) ([]DeadLetterMetadata, []byte, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, nil, err
	}
	if err := ctxErr(ctx); err != nil {
		return nil, nil, err
	}
	if options.MaxBytes < 0 {
		return nil, nil, boxErrorf("dead-letter page max bytes must be nonnegative")
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = defaultDeadLetterMetadataBytes
	}
	if len(options.Cursor) > 0 {
		if _, _, err := parseTimeAndIDKey(s.keys.deadLetterPrefix, options.Cursor); err != nil {
			return nil, nil, err
		}
	}
	limit := min(options.Limit, maxDeadLetterMetadataRows)
	if limit <= 0 {
		return nil, nil, nil
	}
	rows := make([]DeadLetterMetadata, 0, limit)
	var next []byte
	remaining := options.MaxBytes
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		start := s.keys.deadLetterPrefix
		if len(options.Cursor) > 0 {
			start = options.Cursor
		}
		for it.Seek(start); it.ValidForPrefix(s.keys.deadLetterPrefix); it.Next() {
			if err := ctxErr(ctx); err != nil {
				return err
			}
			item := it.Item()
			if bytes.Equal(item.Key(), options.Cursor) {
				continue
			}
			if len(rows) == limit {
				next = rows[len(rows)-1].Cursor
				break
			}
			failedAt, id, err := parseTimeAndIDKey(s.keys.deadLetterPrefix, item.Key())
			if err != nil {
				return err
			}
			size := storedValueUpperBound(item)
			oversized := size > options.MaxBytes
			if !oversized && size > remaining {
				next = rows[len(rows)-1].Cursor
				break
			}
			row := DeadLetterMetadata{ID: id, FailedAt: failedAt, StoredBytes: size, Oversized: oversized, Cursor: item.KeyCopy(nil)}
			if !oversized {
				remaining -= size
				value, err := copyStoredValue(item)
				if err != nil {
					return err
				}
				stored, err := decodeStoredDeadLetter(value)
				if err != nil {
					return err
				}
				text, truncated := deadLetterFailureSummary(stored.Error)
				if stored.QuarantinedSource != nil {
					source, err := s.referencedSourceItem(txn, row.Cursor, stored)
					if err != nil {
						return err
					}
					row.QuarantinedSource = &QuarantinedSourceMetadata{StoredBytes: storedValueUpperBound(source), FailureText: text, FailureTextTruncated: truncated, Permanent: stored.Permanent}
				} else {
					if stored.Record.ID != id || stored.FailedAt != failedAt.UnixNano() {
						return boxErrorf("dead-letter key disagrees with record")
					}
					row.Details = &DeadLetterDetails{State: stored.Record.Status, CreatedAt: time.Unix(0, stored.Record.CreatedAtUnix).UTC(), AvailableAt: time.Unix(0, stored.Record.AvailableAtUnix).UTC(), Attempt: stored.Record.Attempt, MaxAttempts: stored.Record.MaxAttempts, FailureText: text, FailureTextTruncated: truncated, Permanent: stored.Permanent}
				}
			}
			rows = append(rows, row)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return rows, next, nil
}

func deadLetterFailureSummary(text string) (string, bool) {
	if len(text) <= maxDeadLetterFailureTextBytes {
		return text, false
	}
	n := maxDeadLetterFailureTextBytes
	for n > 0 && !utf8.RuneStart(text[n]) {
		n--
	}
	// Copy the prefix so summaries do not retain an arbitrarily large failure string.
	return string(append([]byte(nil), text[:n]...)), true
}

// DeadLetterRequeueOptions controls exact requeue. MaxBytes zero disables the
// stored-value limit for trusted callers; negative values are invalid. Referenced
// quarantine charges both the DLQ metadata and the retained source value.
type DeadLetterRequeueOptions struct {
	AvailableAt time.Time
	MaxBytes    int64
}
