package badgerbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestStoredRecordRequiresLifecycleMetadata(t *testing.T) {
	for _, state := range []MessageState{MessageStateReady, MessageStateProcessing} {
		required := []string{"id", "status", "created_at_unix_nano", "available_at_unix_nano", "attempt", "max_attempts", "payload_bytes", "destination_bytes"}
		if state == MessageStateProcessing {
			required = append(required, "lease_token", "lease_until_unix_nano")
		}
		for _, field := range required {
			for _, null := range []bool{false, true} {
				if null && (field == "payload_bytes" || field == "destination_bytes") {
					continue
				}
				name := string(state) + "/" + field + "/missing"
				if null {
					name = string(state) + "/" + field + "/null"
				}
				t.Run(name, func(t *testing.T) {
					data := marshalRecordTestValue(t, validRecordForValidation(state))
					data = changeRecordTestField(t, data, field, nil, !null)
					_, err := decodeStoredRecord(data)
					if err == nil || !strings.Contains(err.Error(), field) {
						t.Fatalf("invalid %s accepted or unidentified: %v", field, err)
					}
				})
			}
		}
	}
}

func TestStoredRecordRejectsInvalidLifecycleValues(t *testing.T) {
	cases := []struct {
		name  string
		state MessageState
		field string
		value any
	}{
		{"negative-attempt", MessageStateReady, "attempt", -1},
		{"zero-max-attempts", MessageStateReady, "max_attempts", 0},
		{"negative-max-attempts", MessageStateProcessing, "max_attempts", -1},
		{"fractional-timestamp", MessageStateReady, "created_at_unix_nano", 1.5},
		{"string-timestamp", MessageStateReady, "available_at_unix_nano", "0"},
		{"unknown-state", MessageStateReady, "status", "unknown"},
		{"ready-token", MessageStateReady, "lease_token", "unreleased"},
		{"ready-deadline", MessageStateReady, "lease_until_unix_nano", 1},
		{"ready-null-token", MessageStateReady, "lease_token", nil},
		{"ready-null-deadline", MessageStateReady, "lease_until_unix_nano", nil},
		{"processing-zero-attempt", MessageStateProcessing, "attempt", 0},
		{"processing-empty-token", MessageStateProcessing, "lease_token", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := changeRecordTestField(t, marshalRecordTestValue(t, validRecordForValidation(tc.state)), tc.field, tc.value, false)
			if _, err := decodeStoredRecord(data); err == nil {
				t.Fatal("invalid lifecycle metadata accepted")
			}
		})
	}
}

func TestStoredRecordAcceptsZeroTimesAndOpaqueBytes(t *testing.T) {
	for _, state := range []MessageState{MessageStateReady, MessageStateProcessing} {
		for _, payload := range [][]byte{nil, {}, {0, 255, 128}} {
			record := validRecordForValidation(state)
			record.ID = 0
			record.PayloadBytes, record.DestinationBytes = payload, payload
			record.CreatedAtUnix, record.AvailableAtUnix, record.LeaseUntilUnix = 0, -1, 0
			// Recovery and a lower configured retry limit may leave attempts above max.
			record.Attempt, record.MaxAttempts = 4, 2
			got, err := decodeStoredRecord(marshalRecordTestValue(t, record))
			if err != nil || !reflect.DeepEqual(got, record) {
				t.Fatalf("valid %s record changed: got=%+v err=%v", state, got, err)
			}
		}
	}
	record := validRecordForValidation(MessageStateReady)
	data := marshalRecordTestValue(t, record)
	for _, field := range []string{"lease_token", "lease_until_unix_nano"} {
		data = changeRecordTestField(t, data, field, nil, true)
	}
	if _, err := decodeStoredRecord(data); err != nil {
		t.Fatalf("ready records may omit empty lease fields: %v", err)
	}
}

func TestMalformedLiveRecordsNeverDecodeOrMutate(t *testing.T) {
	cases := []struct {
		name    string
		state   MessageState
		field   string
		value   any
		missing bool
	}{
		{"missing-created", MessageStateReady, "created_at_unix_nano", nil, true},
		{"null-available", MessageStateReady, "available_at_unix_nano", nil, false},
		{"negative-attempt", MessageStateReady, "attempt", -1, false},
		{"ready-lease", MessageStateReady, "lease_token", "stale", false},
		{"missing-lease-until", MessageStateProcessing, "lease_until_unix_nano", nil, true},
		{"null-lease-token", MessageStateProcessing, "lease_token", nil, false},
		{"zero-processing-attempt", MessageStateProcessing, "attempt", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var decodes atomic.Int64
			codec := recordValidationCodec{&decodes}
			db, store, cleanup := openTestStore[string, string](t, "metadata", Serde[string, string]{Message: codec, Destination: codec})
			defer cleanup()
			id, err := store.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "payload", Destination: "destination"})
			if err != nil {
				t.Fatal(err)
			}
			token := "unused"
			if tc.state == MessageStateProcessing {
				work, err := store.claimReadyBatch(t.Context(), time.Now(), 1, time.Minute, 3)
				if err != nil || len(work) != 1 {
					t.Fatalf("claim=%v err=%v", work, err)
				}
				token = work[0].LeaseToken
			}
			key := store.keys.messageKey(id)
			corruptRecordTestValue(t, db, key, func(data []byte) []byte { return changeRecordTestField(t, data, tc.field, tc.value, tc.missing) })
			before := recordTestNamespace(t, db, store.opts.Namespace)
			decodes.Store(0)
			type operation struct {
				name string
				run  func() error
			}
			operations := []operation{
				{"get", func() error { _, err := store.Get(t.Context(), id); return err }},
			}
			if tc.state == MessageStateReady {
				operations = append(operations, operation{"claim", func() error { _, err := store.claimReadyBatch(t.Context(), time.Now(), 1, time.Minute, 3); return err }})
			} else {
				operations = append(operations,
					operation{"acknowledge", func() error { return store.acknowledge(t.Context(), id, token) }},
					operation{"retry", func() error {
						_, err := store.failProcessing(t.Context(), id, token, errors.New("retry"), time.Second, time.Second)
						return err
					}},
					operation{"dead-letter", func() error {
						_, err := store.failProcessing(t.Context(), id, token, Permanent(errors.New("failed")), time.Second, time.Second)
						return err
					}},
					operation{"recover", func() error { _, err := store.requeueExpired(t.Context(), time.Now().Add(2*time.Minute)); return err }},
				)
			}
			for _, op := range operations {
				if err := op.run(); err == nil {
					t.Errorf("%s accepted malformed metadata", op.name)
				}
				if decodes.Load() != 0 {
					t.Fatalf("%s invoked application codecs", op.name)
				}
				if after := recordTestNamespace(t, db, store.opts.Namespace); !reflect.DeepEqual(after, before) {
					t.Fatalf("%s changed a row, index, or counter", op.name)
				}
			}
		})
	}
}

func TestStoredDeadLetterRequiresFailureTime(t *testing.T) {
	letter := storedDeadLetter{Record: validRecordForValidation(MessageStateProcessing), FailedAt: 0}
	data := marshalRecordTestValue(t, letter)
	if got, err := decodeStoredDeadLetter(data); err != nil || !reflect.DeepEqual(got, letter) {
		t.Fatalf("valid epoch failure time changed: got=%+v err=%v", got, err)
	}
	for _, missing := range []bool{false, true} {
		invalid := changeRecordTestField(t, data, "failed_at_unix_nano", nil, missing)
		if _, err := decodeStoredDeadLetter(invalid); err == nil {
			t.Fatalf("missing=%v: malformed failure time accepted", missing)
		}
	}
}

func TestMalformedDeadLettersNeverDecodeOrRequeue(t *testing.T) {
	for _, field := range []string{"created_at_unix_nano", "attempt", "max_attempts", "lease_until_unix_nano"} {
		t.Run(field, func(t *testing.T) {
			var decodes atomic.Int64
			codec := recordValidationCodec{&decodes}
			db, store, cleanup := openTestStore[string, string](t, "dead-metadata", Serde[string, string]{Message: codec, Destination: codec})
			defer cleanup()
			id, err := store.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "opaque", Destination: "opaque"})
			if err != nil {
				t.Fatal(err)
			}
			work, err := store.claimReadyBatch(t.Context(), time.Now(), 1, time.Minute, 3)
			if err != nil || len(work) != 1 {
				t.Fatalf("work=%v err=%v", work, err)
			}
			if _, err := store.failProcessing(t.Context(), id, work[0].LeaseToken, Permanent(errors.New("failed")), time.Second, time.Second); err != nil {
				t.Fatal(err)
			}
			values := recordTestNamespace(t, db, store.opts.Namespace)
			var failedAt time.Time
			for key := range values {
				if bytes.HasPrefix([]byte(key), store.keys.deadLetterPrefix) {
					failedAt, _, err = parseTimeAndIDKey(store.keys.deadLetterPrefix, []byte(key))
					if err != nil {
						t.Fatal(err)
					}
					corruptRecordTestValue(t, db, []byte(key), func(data []byte) []byte {
						var envelope map[string]json.RawMessage
						if err := json.Unmarshal(data, &envelope); err != nil {
							t.Fatal(err)
						}
						envelope["record"] = changeRecordTestField(t, envelope["record"], field, nil, true)
						return marshalRecordTestValue(t, envelope)
					})
				}
			}
			before := recordTestNamespace(t, db, store.opts.Namespace)
			decodes.Store(0)
			if _, _, err := store.ListDeadLetters(t.Context(), 10, nil); err == nil {
				t.Error("list accepted malformed dead letter")
			}
			if err := store.RequeueDeadLetter(t.Context(), id, failedAt, time.Now()); err == nil {
				t.Error("requeue accepted malformed dead letter")
			}
			if decodes.Load() != 0 {
				t.Fatal("dead-letter operation invoked application codecs")
			}
			if after := recordTestNamespace(t, db, store.opts.Namespace); !reflect.DeepEqual(after, before) {
				t.Fatal("dead-letter operation changed storage")
			}
		})
	}
}

func validRecordForValidation(state MessageState) storedRecord {
	record := storedRecord{ID: 7, CreatedAtUnix: 1, AvailableAtUnix: 2, MaxAttempts: 3, Status: state}
	if state == MessageStateProcessing {
		record.Attempt = 1
		record.LeaseToken = "owned"
		record.LeaseUntilUnix = 3
	}
	return record
}
func marshalRecordTestValue(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func changeRecordTestField(t *testing.T, data []byte, field string, value any, missing bool) []byte {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if missing {
		delete(fields, field)
	} else {
		fields[field] = marshalRecordTestValue(t, value)
	}
	return marshalRecordTestValue(t, fields)
}
func corruptRecordTestValue(t *testing.T, db *badger.DB, key []byte, change func([]byte) []byte) {
	t.Helper()
	if err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return txn.Set(key, change(data))
	}); err != nil {
		t.Fatal(err)
	}
}
func recordTestNamespace(t *testing.T, db *badger.DB, namespace string) map[string][]byte {
	t.Helper()
	values := make(map[string][]byte)
	if err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("ob/" + namespace + "/")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			data, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			values[string(it.Item().KeyCopy(nil))] = data
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return values
}

type recordValidationCodec struct{ decodes *atomic.Int64 }

func (c recordValidationCodec) Marshal(value string) ([]byte, error) { return []byte(value), nil }
func (c recordValidationCodec) Unmarshal(data []byte) (string, error) {
	c.decodes.Add(1)
	return string(data), nil
}
