package badgerbox

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/dgraph-io/badger/v4"
)

const (
	maxLeaseTokenBytes  = 256
	maxStoredErrorBytes = 4 << 10
	// Includes worst-case JSON escaping of the token and stored failure text,
	// maximum-width attempts/timestamps, and the dead-letter wrapper.
	lifecycleValueHeadroom = 32 << 10
	// Conservatively covers Badger's transaction marker and per-entry overhead.
	lifecycleTxnOverhead = 1 << 10
)

// validateRecordSize checks a newly prepared ready record before any queue
// writes. Reserve space for claims, retries, recovery, and dead-lettering under
// the current DB options; reopening with smaller limits requires a migration.
func (s *Store[M, D]) validateRecordSize(record storedRecord, encodedSize int) error {
	opts := s.db.Opts()
	// Both callers prepare a ready record with zero attempts and no lease.
	// Normalize the two remaining mutable fields to their minimum JSON widths
	// before reserving headroom. Requeue may change availability or retain a new
	// attempt limit; neither should change admission for the same payload.
	minValueSize := int64(encodedSize - len(strconv.FormatInt(record.AvailableAtUnix, 10)) - len(strconv.Itoa(record.MaxAttempts)) + 2)
	maxValueSize := minValueSize + lifecycleValueHeadroom
	if maxValueSize > opts.ValueLogFileSize || (opts.InMemory && maxValueSize > opts.ValueThreshold) {
		return fmt.Errorf("%w: record plus lifecycle headroom exceeds Badger's value limit", ErrMessageTooLarge)
	}

	inlineLimit := opts.ValueThreshold
	if opts.VLogPercentile > 0 {
		// Badger's adaptive threshold never exceeds MaxBatchSize. Use that upper
		// bound so later threshold changes cannot invalidate admission.
		inlineLimit = s.db.MaxBatchSize()
	}
	valueCost := maxValueSize
	if !opts.InMemory {
		if minValueSize >= inlineLimit {
			// Every lifecycle representation is guaranteed to use the value log.
			valueCost = 12 // Badger value pointer.
		} else {
			// Growth across the threshold is not monotonic in transaction cost:
			// the most expensive representation is just below the threshold.
			valueCost = max(12, min(maxValueSize, inlineLimit-1))
		}
	}

	// processing-created is the longest lifecycle key (17-byte time/ID suffix).
	maxKeySize := int64(len(s.keys.processingCreatedPrefix) + 17)
	if maxKeySize > 65000 { // Badger's key limit, including the full index prefix.
		return fmt.Errorf("%w: lifecycle index key exceeds Badger's key limit", ErrMessageTooLarge)
	}
	// Claim/retry/recovery each write at most five entries. A claim additionally
	// stores the token as the processing index value; other indexes are empty.
	maxTxnSize := valueCost + 5*maxKeySize + maxLeaseTokenBytes + lifecycleTxnOverhead
	if maxTxnSize >= s.db.MaxBatchSize() || 6 >= s.db.MaxBatchCount() {
		return fmt.Errorf("%w: lifecycle transition exceeds transaction limit: %w", ErrMessageTooLarge, badger.ErrTxnTooBig)
	}
	return nil
}

func storedFailureText(err error) string {
	text := strings.ToValidUTF8(err.Error(), "?")
	if len(text) <= maxStoredErrorBytes {
		return text
	}
	const suffix = "\n[truncated]"
	end := maxStoredErrorBytes - len(suffix)
	for !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end] + suffix
}
