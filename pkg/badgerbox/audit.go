package badgerbox

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const defaultAuditSampleLimit = 20

// AuditQueueState identifies the lifecycle state associated with an audit anomaly.
type AuditQueueState string

const (
	// AuditQueueStateReady identifies ready-message diagnostics.
	AuditQueueStateReady AuditQueueState = "ready"
	// AuditQueueStateProcessing identifies processing-message diagnostics.
	AuditQueueStateProcessing AuditQueueState = "processing"
	// AuditQueueStateDeadLetter identifies dead-letter diagnostics.
	AuditQueueStateDeadLetter AuditQueueState = "dead_letter"
)

// AuditIndexKind identifies the index family associated with an audit anomaly.
type AuditIndexKind string

const (
	// AuditIndexKindLifecycle identifies a state's lifecycle index.
	AuditIndexKindLifecycle AuditIndexKind = "lifecycle"
	// AuditIndexKindCreated identifies a state's created-time index.
	AuditIndexKindCreated AuditIndexKind = "created"
	// AuditIndexKindDeadLetter identifies the dead-letter index.
	AuditIndexKindDeadLetter AuditIndexKind = "dead_letter"
)

// AuditAnomalyKind identifies a consistency failure found by Audit.
type AuditAnomalyKind string

const (
	// AuditAnomalyMissing identifies a row without its expected index.
	AuditAnomalyMissing AuditAnomalyKind = "missing"
	// AuditAnomalyOrphaned identifies an index without a live row.
	AuditAnomalyOrphaned AuditAnomalyKind = "orphaned"
	// AuditAnomalyWrongState identifies an index targeting a row in another state.
	AuditAnomalyWrongState AuditAnomalyKind = "wrong_state"
	// AuditAnomalyTimestampMismatch identifies a row/index timestamp disagreement.
	AuditAnomalyTimestampMismatch AuditAnomalyKind = "timestamp_mismatch"
	// AuditAnomalyValueMismatch identifies an unexpected index value.
	AuditAnomalyValueMismatch AuditAnomalyKind = "value_mismatch"
	// AuditAnomalyDuplicate identifies extra index keys for one message ID.
	AuditAnomalyDuplicate AuditAnomalyKind = "duplicate"
	// AuditAnomalyLiveRowCollision identifies a dead letter sharing a live-row ID.
	AuditAnomalyLiveRowCollision AuditAnomalyKind = "live_row_collision"
	// AuditAnomalyRecordIDMismatch identifies a dead-letter key/record ID disagreement.
	AuditAnomalyRecordIDMismatch AuditAnomalyKind = "record_id_mismatch"
	// AuditAnomalyFailedAtMismatch identifies a dead-letter key/record timestamp disagreement.
	AuditAnomalyFailedAtMismatch AuditAnomalyKind = "failed_at_mismatch"
	// AuditAnomalyUnexpectedRecordState identifies a dead letter not stored as processing.
	AuditAnomalyUnexpectedRecordState AuditAnomalyKind = "unexpected_record_state"
)

// AuditOptions controls diagnostic queue index auditing.
type AuditOptions struct {
	// SampleLimit is the maximum sample count per anomaly class; values below one use the default.
	SampleLimit int
	// Now overrides the due/future reference clock; zero uses the store runtime.
	Now time.Time
}

// AuditReport summarizes queue row/index consistency without mutating storage.
type AuditReport struct {
	// GeneratedAt is the reference time used to classify audit results.
	GeneratedAt time.Time `json:"generated_at"`
	// Namespace is the Badger namespace audited by the store.
	Namespace string `json:"namespace"`
	// LiveRows is the number of primary message rows.
	LiveRows int64 `json:"live_rows"`
	// States contains per-state primary-row and index diagnostics.
	States AuditStateReports `json:"states"`
	// DeadLetters contains dead-letter row diagnostics.
	DeadLetters AuditDeadLetterReport `json:"dead_letters"`
	// Samples contains bounded row and anomaly examples.
	Samples AuditSamples `json:"samples"`
}

// AuditStateReports contains diagnostics for every live lifecycle state.
type AuditStateReports struct {
	// Ready contains ready-row and index diagnostics.
	Ready AuditStateReport `json:"ready"`
	// Processing contains processing-row and index diagnostics.
	Processing AuditStateReport `json:"processing"`
}

// AuditStateReport summarizes primary rows and both indexes for one lifecycle state.
type AuditStateReport struct {
	// Rows is the number of primary rows in this state.
	Rows int64 `json:"rows"`
	// Lifecycle contains lifecycle-index consistency counts.
	Lifecycle AuditIndexReport `json:"lifecycle"`
	// Created contains created-time-index consistency counts.
	Created AuditIndexReport `json:"created"`
}

// AuditIndexReport summarizes consistency for one state-specific index family.
type AuditIndexReport struct {
	// Keys is the number of structurally valid keys scanned.
	Keys int64 `json:"keys"`
	// Missing is the number of state rows without an index key.
	Missing int64 `json:"missing"`
	// Orphaned is the number of index keys without a live row.
	Orphaned int64 `json:"orphaned"`
	// WrongState is the number of index keys targeting a row in another state.
	WrongState int64 `json:"wrong_state"`
	// TimestampMismatches is the number of index timestamps that disagree with their rows.
	TimestampMismatches int64 `json:"timestamp_mismatches"`
	// ValueMismatches is the number of index values that violate the index contract.
	ValueMismatches int64 `json:"value_mismatches"`
	// DuplicateKeys is the number of extra keys for IDs repeated within this index family.
	DuplicateKeys int64 `json:"duplicate_keys"`
}

// AuditDeadLetterReport summarizes dead-letter record consistency.
type AuditDeadLetterReport struct {
	// Rows is the number of dead-letter rows.
	Rows int64 `json:"rows"`
	// DuplicateIDs is the number of extra dead-letter rows that repeat a key ID
	// or embedded record ID.
	DuplicateIDs int64 `json:"duplicate_ids"`
	// LiveRowCollisions is the number of dead letters whose ID also has a live row.
	LiveRowCollisions int64 `json:"live_row_collisions"`
	// RecordIDMismatches is the number of key/record ID disagreements.
	RecordIDMismatches int64 `json:"record_id_mismatches"`
	// FailedAtMismatches is the number of key/record failure-time disagreements.
	FailedAtMismatches int64 `json:"failed_at_mismatches"`
	// UnexpectedRecordStates is the number of dead letters not stored in processing state.
	UnexpectedRecordStates int64 `json:"unexpected_record_states"`
}

// AuditSamples contains bounded diagnostic examples without message contents.
type AuditSamples struct {
	// Anomalies contains normalized consistency examples.
	Anomalies []AuditAnomalySample `json:"anomalies,omitempty"`
}

// AuditAnomalySample identifies one row/index inconsistency without exposing stored contents.
type AuditAnomalySample struct {
	// State identifies the index or record lifecycle family.
	State AuditQueueState `json:"state"`
	// IndexKind identifies the index family.
	IndexKind AuditIndexKind `json:"index_kind"`
	// AnomalyKind identifies the consistency failure.
	AnomalyKind AuditAnomalyKind `json:"anomaly_kind"`
	// MessageID is the identifier encoded in the index key, or the primary-row ID for a missing index.
	MessageID MessageID `json:"message_id"`
	// ActualMessageID is the conflicting stored identifier when applicable.
	ActualMessageID *MessageID `json:"actual_message_id,omitempty"`
	// ExpectedState is the lifecycle state required by the index family.
	ExpectedState MessageState `json:"expected_state,omitempty"`
	// ActualState is the stored lifecycle state when a row exists.
	ActualState MessageState `json:"actual_state,omitempty"`
	// ExpectedAt is the timestamp derived from the stored row when applicable.
	ExpectedAt *time.Time `json:"expected_at,omitempty"`
	// ActualAt is the timestamp encoded in the index key when applicable.
	ActualAt *time.Time `json:"actual_at,omitempty"`
}

type auditRecordView struct {
	status      MessageState
	createdAt   time.Time
	availableAt time.Time
	leaseUntil  time.Time
	leaseToken  string
}

type auditIndexSpec struct {
	state  AuditQueueState
	kind   AuditIndexKind
	prefix []byte
	report *AuditIndexReport
}

type auditSampler struct {
	limit  int
	counts map[string]int
	items  *[]AuditAnomalySample
}

// Audit scans all primary rows, lifecycle indexes, created-time indexes, and
// dead letters. It reports well-formed inconsistencies and returns errors for
// malformed keys or undecodable records; it never repairs or mutates storage.
func (s *Store[M, D]) Audit(ctx context.Context, opts AuditOptions) (AuditReport, error) {
	if err := s.ensureOpen(); err != nil {
		return AuditReport{}, err
	}
	if err := ctxErr(ctx); err != nil {
		return AuditReport{}, err
	}
	now := opts.Now.UTC()
	if now.IsZero() {
		now = s.runtime.Now().UTC()
	}
	limit := opts.SampleLimit
	if limit < 1 {
		limit = defaultAuditSampleLimit
	}
	report := AuditReport{GeneratedAt: now, Namespace: s.keys.namespace}
	sampler := auditSampler{limit: limit, counts: make(map[string]int), items: &report.Samples.Anomalies}
	err := s.db.View(func(txn *badger.Txn) error {
		records, err := s.collectAuditRows(ctx, txn, &report)
		if err != nil {
			return err
		}
		for _, spec := range s.auditIndexSpecs(&report) {
			if err := s.auditIndex(ctx, txn, records, spec, &sampler); err != nil {
				return err
			}
		}
		return s.auditDeadLetters(ctx, txn, records, &report, &sampler)
	})
	return report, err
}

func (s *Store[M, D]) collectAuditRows(ctx context.Context, txn *badger.Txn, report *AuditReport) (map[MessageID]auditRecordView, error) {
	records := make(map[MessageID]auditRecordView)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Seek(s.keys.messagePrefix); it.ValidForPrefix(s.keys.messagePrefix); it.Next() {
		if err := ctxErr(ctx); err != nil {
			return nil, err
		}
		id, err := parseMessageKey(s.keys.messagePrefix, it.Item().Key())
		if err != nil {
			return nil, err
		}
		value, err := it.Item().ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		record, err := decodeAuditRecord(id, value)
		if err != nil {
			return nil, err
		}
		records[id] = record
		report.LiveRows++
		stateReport := auditStateReport(report, record.status)
		stateReport.Rows++

	}
	return records, nil
}

func (s *Store[M, D]) auditIndexSpecs(report *AuditReport) []auditIndexSpec {
	return []auditIndexSpec{
		{AuditQueueStateReady, AuditIndexKindLifecycle, s.keys.readyPrefix, &report.States.Ready.Lifecycle},
		{AuditQueueStateReady, AuditIndexKindCreated, s.keys.readyCreatedPrefix, &report.States.Ready.Created},
		{AuditQueueStateProcessing, AuditIndexKindLifecycle, s.keys.processingPrefix, &report.States.Processing.Lifecycle},
		{AuditQueueStateProcessing, AuditIndexKindCreated, s.keys.processingCreatedPrefix, &report.States.Processing.Created},
	}
}

func (s *Store[M, D]) auditIndex(ctx context.Context, txn *badger.Txn, records map[MessageID]auditRecordView, spec auditIndexSpec, sampler *auditSampler) error {
	seen := make(map[MessageID]int)
	firstSeenAt := make(map[MessageID]time.Time)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Seek(spec.prefix); it.ValidForPrefix(spec.prefix); it.Next() {
		if err := ctxErr(ctx); err != nil {
			return err
		}
		indexAt, id, err := parseTimeAndIDKey(spec.prefix, it.Item().Key())
		if err != nil {
			return err
		}
		value, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		spec.report.Keys++
		firstAt, hasFirst := firstSeenAt[id]
		if !hasFirst {
			firstSeenAt[id] = indexAt
		}
		seen[id]++
		record, ok := records[id]
		if seen[id] > 1 {
			spec.report.DuplicateKeys++
			duplicateAt := indexAt
			if ok && AuditQueueState(record.status) == spec.state {
				expectedAt := auditRecordIndexTime(record, spec)
				if indexAt.Equal(expectedAt) && !firstAt.Equal(expectedAt) {
					duplicateAt = firstAt
				}
			}
			sampler.add(spec, AuditAnomalyDuplicate, id, auditRecordView{}, nil, &duplicateAt, nil)
		}
		if !ok {
			spec.report.Orphaned++
			sampler.add(spec, AuditAnomalyOrphaned, id, auditRecordView{}, nil, &indexAt, nil)
		} else {
			if AuditQueueState(record.status) != spec.state {
				spec.report.WrongState++
				sampler.add(spec, AuditAnomalyWrongState, id, record, nil, &indexAt, nil)
			}
			expectedAt := auditRecordIndexTime(record, spec)
			if !indexAt.Equal(expectedAt) {
				spec.report.TimestampMismatches++
				sampler.add(spec, AuditAnomalyTimestampMismatch, id, record, &expectedAt, &indexAt, nil)
			}
		}
		if auditIndexValueMismatch(spec, record, ok, value) {
			spec.report.ValueMismatches++
			sampler.add(spec, AuditAnomalyValueMismatch, id, record, nil, &indexAt, nil)
		}
	}
	ids := make([]MessageID, 0, min(len(records), sampler.limit))
	for id, record := range records {
		if err := ctxErr(ctx); err != nil {
			return err
		}
		if AuditQueueState(record.status) == spec.state && seen[id] == 0 {
			spec.report.Missing++
			insertSmallestMessageID(&ids, sampler.limit, id)
		}
	}
	for _, id := range ids {
		if err := ctxErr(ctx); err != nil {
			return err
		}
		record := records[id]
		expectedAt := auditRecordIndexTime(record, spec)
		sampler.add(spec, AuditAnomalyMissing, id, record, &expectedAt, nil, nil)
	}
	return nil
}

func (s *Store[M, D]) auditDeadLetters(ctx context.Context, txn *badger.Txn, records map[MessageID]auditRecordView, report *AuditReport, sampler *auditSampler) error {
	spec := auditIndexSpec{state: AuditQueueStateDeadLetter, kind: AuditIndexKindDeadLetter}
	seenKeyIDs := make(map[MessageID]int)
	seenRecordIDs := make(map[MessageID]int)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Seek(s.keys.deadLetterPrefix); it.ValidForPrefix(s.keys.deadLetterPrefix); it.Next() {
		if err := ctxErr(ctx); err != nil {
			return err
		}
		failedAt, id, err := parseTimeAndIDKey(s.keys.deadLetterPrefix, it.Item().Key())
		if err != nil {
			return err
		}
		value, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		deadLetter, err := decodeAuditDeadLetter(value)
		if err != nil {
			return err
		}
		if !isValidRecordState(deadLetter.Record.Status) {
			return boxErrorf("invalid dead-letter record status %q", deadLetter.Record.Status)
		}
		report.DeadLetters.Rows++
		seenKeyIDs[id]++
		seenRecordIDs[deadLetter.Record.ID]++
		keyIDRepeated := seenKeyIDs[id] > 1
		recordIDRepeated := seenRecordIDs[deadLetter.Record.ID] > 1
		if keyIDRepeated || recordIDRepeated {
			report.DeadLetters.DuplicateIDs++
			var actualID *MessageID
			if recordIDRepeated && deadLetter.Record.ID != id {
				repeatedID := deadLetter.Record.ID
				actualID = &repeatedID
			}
			sampler.add(spec, AuditAnomalyDuplicate, id, auditRecordView{}, nil, &failedAt, actualID)
		}
		_, keyIDCollision := records[id]
		_, recordIDCollision := records[deadLetter.Record.ID]
		if keyIDCollision || recordIDCollision {
			report.DeadLetters.LiveRowCollisions++
			var actualID *MessageID
			if deadLetter.Record.ID != id && recordIDCollision {
				collisionID := deadLetter.Record.ID
				actualID = &collisionID
			}
			sampler.add(spec, AuditAnomalyLiveRowCollision, id, auditRecordView{}, nil, &failedAt, actualID)
		}
		if deadLetter.Record.ID != id {
			report.DeadLetters.RecordIDMismatches++
			actualID := deadLetter.Record.ID
			sampler.add(spec, AuditAnomalyRecordIDMismatch, id, auditRecordView{}, nil, &failedAt, &actualID)
		}
		storedFailedAt := time.Unix(0, deadLetter.FailedAt).UTC()
		if !failedAt.Equal(storedFailedAt) {
			report.DeadLetters.FailedAtMismatches++
			sampler.add(spec, AuditAnomalyFailedAtMismatch, id, auditRecordView{}, &storedFailedAt, &failedAt, nil)
		}
		if deadLetter.Record.Status != recordStatusProcessing {
			report.DeadLetters.UnexpectedRecordStates++
			sampler.add(spec, AuditAnomalyUnexpectedRecordState, id, auditRecordView{status: deadLetter.Record.Status}, nil, &failedAt, nil)
		}
	}
	return nil
}

func decodeAuditDeadLetter(value []byte) (storedDeadLetter, error) {
	var deadLetter storedDeadLetter
	if err := json.Unmarshal(value, &deadLetter); err != nil {
		return storedDeadLetter{}, err
	}
	var rawDeadLetter struct {
		FailedAt json.RawMessage `json:"failed_at_unix_nano"`
		Record   struct {
			ID json.RawMessage `json:"id"`
		} `json:"record"`
	}
	if err := json.Unmarshal(value, &rawDeadLetter); err != nil {
		return storedDeadLetter{}, err
	}
	rawID := bytes.TrimSpace(rawDeadLetter.Record.ID)
	if len(rawID) == 0 {
		return storedDeadLetter{}, boxErrorf("dead-letter record ID is missing")
	}
	if bytes.Equal(rawID, []byte("null")) {
		return storedDeadLetter{}, boxErrorf("dead-letter record ID is null")
	}
	rawFailedAt := bytes.TrimSpace(rawDeadLetter.FailedAt)
	if len(rawFailedAt) == 0 {
		return storedDeadLetter{}, boxErrorf("dead-letter failed_at_unix_nano is missing")
	}
	if bytes.Equal(rawFailedAt, []byte("null")) {
		return storedDeadLetter{}, boxErrorf("dead-letter failed_at_unix_nano is null")
	}

	var envelope struct {
		Record json.RawMessage `json:"record"`
	}
	if err := json.Unmarshal(value, &envelope); err != nil {
		return storedDeadLetter{}, err
	}
	if _, err := decodeAuditRecord(deadLetter.Record.ID, envelope.Record); err != nil {
		return storedDeadLetter{}, err
	}
	return deadLetter, nil
}

func decodeAuditRecord(id MessageID, value []byte) (auditRecordView, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(value, &fields); err != nil {
		return auditRecordView{}, err
	}
	for _, field := range []string{"id", "status", "payload_bytes", "destination_bytes"} {
		if _, ok := fields[field]; !ok {
			return auditRecordView{}, boxErrorf("record field %s is missing", field)
		}
	}

	var record storedRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return auditRecordView{}, err
	}
	var rawRecord struct {
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(value, &rawRecord); err != nil {
		return auditRecordView{}, err
	}
	if len(rawRecord.ID) == 0 {
		return auditRecordView{}, boxErrorf("record ID is missing for message key %s", id)
	} else if bytes.Equal(rawRecord.ID, []byte("null")) {
		return auditRecordView{}, boxErrorf("record ID is null for message key %s", id)
	} else if err := json.Unmarshal(rawRecord.ID, &record.ID); err != nil {
		return auditRecordView{}, err
	} else if record.ID != id {
		return auditRecordView{}, boxErrorf("record ID %s does not match message key %s", record.ID, id)
	}
	if !isValidRecordState(record.Status) {
		return auditRecordView{}, boxErrorf("invalid record status %q", record.Status)
	}
	return auditRecordView{
		status: record.Status, createdAt: time.Unix(0, record.CreatedAtUnix).UTC(),
		availableAt: time.Unix(0, record.AvailableAtUnix).UTC(),
		leaseUntil:  time.Unix(0, record.LeaseUntilUnix).UTC(), leaseToken: record.LeaseToken,
	}, nil
}

func auditStateReport(report *AuditReport, state MessageState) *AuditStateReport {
	switch state {
	case recordStatusPending:
		return &report.States.Ready
	default:
		return &report.States.Processing
	}
}

func auditRecordIndexTime(record auditRecordView, spec auditIndexSpec) time.Time {
	if spec.kind == AuditIndexKindCreated {
		return record.createdAt
	}
	if spec.state == AuditQueueStateProcessing {
		return record.leaseUntil
	}
	return record.availableAt
}

func auditIndexValueMismatch(spec auditIndexSpec, record auditRecordView, found bool, value []byte) bool {
	if spec.kind == AuditIndexKindCreated || spec.state != AuditQueueStateProcessing {
		return len(value) != 0
	}
	return found && !bytes.Equal(value, []byte(record.leaseToken))
}

func (s *auditSampler) add(spec auditIndexSpec, kind AuditAnomalyKind, id MessageID, record auditRecordView, expectedAt, actualAt *time.Time, actualID *MessageID) {
	class := string(spec.state) + "/" + string(spec.kind) + "/" + string(kind)
	if s.counts[class] >= s.limit {
		return
	}
	s.counts[class]++
	sample := AuditAnomalySample{
		State: spec.state, IndexKind: spec.kind, AnomalyKind: kind, MessageID: id,
		ActualMessageID: actualID, ExpectedAt: expectedAt, ActualAt: actualAt,
	}
	if kind == AuditAnomalyWrongState || kind == AuditAnomalyUnexpectedRecordState {
		sample.ExpectedState = MessageState(spec.state)
		if spec.state == AuditQueueStateDeadLetter {
			sample.ExpectedState = recordStatusProcessing
		}
		sample.ActualState = record.status
	}
	*s.items = append(*s.items, sample)
}

func insertSmallestMessageID(ids *[]MessageID, limit int, id MessageID) {
	if limit < 1 {
		return
	}
	index := sort.Search(len(*ids), func(index int) bool { return (*ids)[index] >= id })
	if index >= limit {
		return
	}
	if len(*ids) < limit {
		*ids = append(*ids, 0)
	} else {
		*ids = (*ids)[:limit]
	}
	copy((*ids)[index+1:], (*ids)[index:len(*ids)-1])
	(*ids)[index] = id
}

func isValidRecordState(state MessageState) bool {
	return state == recordStatusPending || state == recordStatusProcessing
}
