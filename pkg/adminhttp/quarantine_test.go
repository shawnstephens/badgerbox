package adminhttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
)

func TestQuarantinedSourceProjectionPreservesBoundedProgress(t *testing.T) {
	h := handler{namespace: "test", maxResponseBytes: 1024}
	rows := make([]badgerbox.DeadLetterMetadata, 0, 4)
	for id := range 4 {
		rows = append(rows, badgerbox.DeadLetterMetadata{
			ID: badgerbox.MessageID(id), FailedAt: time.Unix(1, 0), Cursor: []byte(fmt.Sprint(id)), StoredBytes: 256,
			QuarantinedSource: &badgerbox.QuarantinedSourceMetadata{StoredBytes: 1 << 20, FailureText: strings.Repeat("\x00😀", 1024), Permanent: true},
		})
	}
	for len(rows) > 0 {
		data, err := h.encodeDeadLetters(t.Context(), rows, nil)
		if err != nil {
			t.Fatal(err)
		}
		var page deadLetterPage
		if len(data)+1 > h.maxResponseBytes || json.Unmarshal(data, &page) != nil || len(page.DeadLetters) == 0 {
			t.Fatalf("invalid bounded page: %d bytes %s", len(data), data)
		}
		for _, row := range page.DeadLetters {
			if row.Metadata != nil || row.QuarantinedSource == nil || row.StoredBytes != 256 || row.QuarantinedSource.StoredBytes != 1<<20 || !row.QuarantinedSource.Permanent || !row.QuarantinedSource.FailureTextTruncated {
				t.Fatalf("projection=%+v reference=%+v", row, row.QuarantinedSource)
			}
		}
		n := len(page.DeadLetters)
		if n < len(rows) {
			cursor, err := h.decodeCursor(map[string][]string{"cursor": {page.NextCursor}})
			if err != nil || string(cursor) != string(rows[n-1].Cursor) {
				t.Fatalf("pagination lost exact identity: cursor=%s err=%v", cursor, err)
			}
		} else if page.NextCursor != "" {
			t.Fatal("unexpected final cursor")
		}
		rows = rows[n:]
	}
}

func TestQuarantineHTTPInspectionPreservesSourceAndQuota(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := badgerbox.New[string, string](db, badgerbox.Serde[string, string]{}, badgerbox.Options{Namespace: "quarantine-http", AdmissionLimits: badgerbox.AdmissionLimits{MaxRetainedMessages: 2}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	bigID, err := s.Enqueue(t.Context(), badgerbox.EnqueueRequest[string, string]{Payload: strings.Repeat("secret-payload", 6000)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(t.Context(), badgerbox.EnqueueRequest[string, string]{Payload: "healthy"}); err != nil {
		t.Fatal(err)
	}
	processor, err := badgerbox.NewBatchProcessor(s, func(_ context.Context, messages []badgerbox.Message[string, string], results chan<- badgerbox.BatchProcessResult) error {
		for _, message := range messages {
			if message.ID == bigID {
				return errors.New("oversized source reached callback")
			}
			results <- badgerbox.BatchProcessResult{ID: message.ID}
		}
		return nil
	}, badgerbox.BatchProcessorOptions{ProcessorOptions: badgerbox.ProcessorOptions{Concurrency: 1, ClaimMaxBytes: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- processor.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("processor shutdown: %v", err)
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	var letters []badgerbox.DeadLetterMetadata
	for time.Now().Before(deadline) {
		letters, _, err = s.ListDeadLetterMetadata(t.Context(), badgerbox.DeadLetterListOptions{Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		usage, err := s.Usage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(letters) == 1 && usage.RetainedMessages == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(letters) != 1 || letters[0].QuarantinedSource == nil {
		t.Fatalf("missing quarantine: %+v", letters)
	}
	h, err := New(s, Options{Namespace: "quarantine-http", MaxRequeueBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	r := call(h, "GET", "/dead-letters", "")
	var page deadLetterPage
	if r.Code != 200 || json.Unmarshal(r.Body.Bytes(), &page) != nil || len(page.DeadLetters) != 1 || page.DeadLetters[0].QuarantinedSource == nil || page.DeadLetters[0].Metadata != nil {
		t.Fatalf("quarantine response: %d %s", r.Code, r.Body)
	}
	if strings.Contains(r.Body.String(), "secret-payload") {
		t.Fatal("HTTP inspection exposed source contents")
	}
	before, err := s.Usage(t.Context())
	if err != nil || before.RetainedMessages != 1 {
		t.Fatalf("usage=%+v error=%v", before, err)
	}
	body := fmt.Sprintf(`{"failed_at":%q}`, letters[0].FailedAt.Format(time.RFC3339Nano))
	r = call(h, "POST", "/dead-letters/"+bigID.String()+"/requeue", body)
	if r.Code != 413 || !strings.Contains(r.Body.String(), "dead_letter_too_large") {
		t.Fatalf("oversized source requeue: %d %s", r.Code, r.Body)
	}
	after, err := s.Usage(t.Context())
	if err != nil || after != before {
		t.Fatalf("HTTP requeue changed retained capacity: before=%+v after=%+v error=%v", before, after, err)
	}
	if _, err := s.Get(t.Context(), bigID); !errors.Is(err, badgerbox.ErrMessageQuarantined) {
		t.Fatalf("quarantined source was changed: %v", err)
	}
	audit, err := s.Audit(t.Context(), badgerbox.AuditOptions{})
	if err != nil || !audit.Complete || !audit.Usage.Matches {
		t.Fatalf("quarantine accounting audit=%+v error=%v", audit.Usage, err)
	}
}
