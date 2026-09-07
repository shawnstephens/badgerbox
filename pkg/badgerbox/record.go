package badgerbox

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// decodeStoredRecord validates the storage envelope without invoking application
// codecs. Live reads, dead-letter operations, and diagnostics can share it;
// callers must additionally compare the decoded identity with their storage key.
func decodeStoredRecord(data []byte) (storedRecord, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return storedRecord{}, err
	}
	for _, field := range []string{
		"id", "status", "created_at_unix_nano", "available_at_unix_nano",
		"attempt", "max_attempts", "payload_bytes", "destination_bytes",
	} {
		allowNull := field == "payload_bytes" || field == "destination_bytes"
		if err := requireRecordField(fields, field, allowNull); err != nil {
			return storedRecord{}, err
		}
	}

	var record storedRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return storedRecord{}, err
	}
	if record.Attempt < 0 || record.MaxAttempts <= 0 {
		return storedRecord{}, fmt.Errorf("badgerbox: record attempts must be nonnegative and max_attempts must be positive")
	}
	// An expired lease can leave Attempt >= MaxAttempts, and a processor may
	// reopen the queue with a lower attempt limit. Neither implies corruption.
	// Timestamps may be zero or negative; presence, not a zero value, determines
	// whether Unix-nanosecond metadata was actually stored.
	switch record.Status {
	case MessageStateReady:
		for _, field := range []string{"lease_token", "lease_until_unix_nano"} {
			if _, present := fields[field]; present {
				if err := requireRecordField(fields, field, false); err != nil {
					return storedRecord{}, err
				}
			}
		}
		if record.LeaseToken != "" || record.LeaseUntilUnix != 0 {
			return storedRecord{}, fmt.Errorf("badgerbox: ready record must not have an active lease")
		}
	case MessageStateProcessing:
		for _, field := range []string{"lease_token", "lease_until_unix_nano"} {
			if err := requireRecordField(fields, field, false); err != nil {
				return storedRecord{}, err
			}
		}
		if record.Attempt == 0 || record.LeaseToken == "" {
			return storedRecord{}, fmt.Errorf("badgerbox: processing record requires a positive attempt and a nonempty lease token")
		}
	default:
		return storedRecord{}, fmt.Errorf("badgerbox: invalid record state %q", record.Status)
	}
	return record, nil
}

func requireRecordField(fields map[string]json.RawMessage, field string, allowNull bool) error {
	value, present := fields[field]
	if !present {
		return fmt.Errorf("badgerbox: record field %s is missing", field)
	}
	if !allowNull && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return fmt.Errorf("badgerbox: record field %s is null", field)
	}
	return nil
}

func decodeStoredDeadLetter(data []byte) (storedDeadLetter, error) {
	var envelope struct {
		Record    json.RawMessage `json:"record"`
		FailedAt  *int64          `json:"failed_at_unix_nano"`
		Error     string          `json:"error"`
		Permanent bool            `json:"permanent"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return storedDeadLetter{}, err
	}
	if envelope.FailedAt == nil {
		return storedDeadLetter{}, fmt.Errorf("badgerbox: dead-letter failed_at_unix_nano is missing or null")
	}
	record, err := decodeStoredRecord(envelope.Record)
	if err != nil {
		return storedDeadLetter{}, err
	}
	return storedDeadLetter{
		Record: record, FailedAt: *envelope.FailedAt,
		Error: envelope.Error, Permanent: envelope.Permanent,
	}, nil
}
