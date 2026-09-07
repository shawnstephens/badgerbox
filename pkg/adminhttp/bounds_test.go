package adminhttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
)

func TestRequeueAdmissionIncludesFlushAndErrors(t *testing.T) {
	s := &fakeStore{}
	h, err := New(s, Options{Namespace: "test", MaxConcurrentRequeues: 1})
	if err != nil {
		t.Fatal(err)
	}
	body := `{"failed_at":"2026-09-06T00:00:00Z"}`
	entered, blocked, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(writer{httptest.NewRecorder(), blocked, entered}, httptest.NewRequest("POST", "/dead-letters/1/requeue", strings.NewReader(body)))
	}()
	<-entered
	if r := call(h, "POST", "/dead-letters/1/requeue", body); r.Code != 429 {
		t.Fatalf("not admitted through flush: %d", r.Code)
	}
	close(blocked)
	<-done
	if s.requeueOptions.MaxBytes != 2<<20 {
		t.Fatalf("unbounded requeue: %+v", s.requeueOptions)
	}
	s.requeueErr = badgerbox.ErrDeadLetterTooLarge
	if r := call(h, "POST", "/dead-letters/1/requeue", body); r.Code != 413 || !strings.Contains(r.Body.String(), "dead_letter_too_large") {
		t.Fatalf("%d %s", r.Code, r.Body)
	}
	s.requeueErr = nil
	if r := call(h, "POST", "/dead-letters/1/requeue", body); r.Code != 200 {
		t.Fatalf("slot leaked: %d", r.Code)
	}
}

func TestWireBudgetPreservesContinuationAfterEscaping(t *testing.T) {
	h := handler{namespace: "test", maxResponseBytes: 1024}
	rows := []badgerbox.DeadLetterMetadata{}
	for i := 0; i < 5; i++ {
		rows = append(rows, badgerbox.DeadLetterMetadata{ID: badgerbox.MessageID(i), FailedAt: time.Unix(1, 0), Cursor: []byte(fmt.Sprint(i)), Details: &badgerbox.DeadLetterDetails{FailureText: strings.Repeat("\x00", 1024)}})
	}
	for len(rows) > 0 {
		data, err := h.encodeDeadLetters(t.Context(), rows, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(data)+1 > 1024 {
			t.Fatalf("response grew past budget: %d", len(data))
		}
		var page deadLetterPage
		if err := json.Unmarshal(data, &page); err != nil {
			t.Fatal(err)
		}
		if len(page.DeadLetters) == 0 {
			t.Fatal("no progress")
		}
		if !page.DeadLetters[0].Metadata.FailureTextTruncated {
			t.Fatal("summary truncation was hidden")
		}
		n := len(page.DeadLetters)
		if n < len(rows) {
			cursor, err := h.decodeCursor(map[string][]string{"cursor": {page.NextCursor}})
			if err != nil || string(cursor) != string(rows[n-1].Cursor) {
				t.Fatalf("cursor=%s err=%v", cursor, err)
			}
		} else if page.NextCursor != "" {
			t.Fatal("unexpected final cursor")
		}
		rows = rows[n:]
	}
}

func TestAuditBudgetResponseIsExplicitlyIncomplete(t *testing.T) {
	s := &fakeStore{audit: func(context.Context) (badgerbox.AuditReport, error) {
		return badgerbox.AuditReport{ScannedKeys: 7}, &badgerbox.AuditLimitError{Budget: "keys", Limit: 7}
	}}
	h, _ := New(s, Options{Namespace: "test", MaxResponseBytes: 4096})
	r := call(h, "GET", "/audit", "")
	if r.Code != 413 || !strings.Contains(r.Body.String(), `"complete":false`) || !strings.Contains(r.Body.String(), `"code":"audit_incomplete"`) || !strings.Contains(r.Body.String(), `"scanned_keys":7`) {
		t.Fatalf("%d %s", r.Code, r.Body)
	}
	small, _ := New(s, Options{Namespace: "test", MaxResponseBytes: 1024})
	smallResult := call(small, "GET", "/audit", "")
	if smallResult.Code != 413 || smallResult.Body.Len() > 1024 || !strings.Contains(smallResult.Body.String(), `"code":"audit_incomplete"`) {
		t.Fatalf("small incomplete response: %d %s", smallResult.Code, smallResult.Body)
	}
	s.audit = func(context.Context) (badgerbox.AuditReport, error) {
		return badgerbox.AuditReport{Complete: true, Samples: badgerbox.AuditSamples{Anomalies: make([]badgerbox.AuditAnomalySample, 100)}}, nil
	}
	r = call(h, "GET", "/audit", "")
	if r.Code != 200 || r.Body.Len() > 4096 || !strings.Contains(r.Body.String(), `"samples_truncated":true`) {
		t.Fatalf("%d %s", r.Code, r.Body)
	}
}

type hostilePayload struct{}

func (hostilePayload) MarshalJSON() ([]byte, error) { panic("application JSON marshaler invoked") }

type hostileCodec struct{ armed *atomic.Bool }

func (hostileCodec) Marshal(hostilePayload) ([]byte, error) {
	return []byte("compact application encoding"), nil
}
func (c hostileCodec) Unmarshal([]byte) (hostilePayload, error) {
	if c.armed.Load() {
		panic("application decoder invoked")
	}
	return hostilePayload{}, nil
}

func TestAdminNeverDecodesOrMarshalsApplicationPayloads(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var armed atomic.Bool
	codec := hostileCodec{&armed}
	s, err := badgerbox.New[hostilePayload, hostilePayload](db, badgerbox.Serde[hostilePayload, hostilePayload]{Message: codec, Destination: codec}, badgerbox.Options{Namespace: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id, err := s.Enqueue(t.Context(), badgerbox.EnqueueRequest[hostilePayload, hostilePayload]{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := badgerbox.NewProcessor(s, func(context.Context, badgerbox.Message[hostilePayload, hostilePayload]) error {
		return badgerbox.Permanent(errors.New("failed"))
	}, badgerbox.ProcessorOptions{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	var rows []badgerbox.DeadLetterMetadata
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rows, _, err = s.ListDeadLetterMetadata(t.Context(), badgerbox.DeadLetterListOptions{Limit: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatal("dead letter not created")
	}
	armed.Store(true)
	h, err := New(s, Options{Namespace: "test"})
	if err != nil {
		t.Fatal(err)
	}
	r := call(h, "GET", "/dead-letters", "")
	if r.Code != 200 || strings.Contains(r.Body.String(), `"payload"`) || strings.Contains(r.Body.String(), `"destination"`) {
		t.Fatalf("%d %s", r.Code, r.Body)
	}
	body := fmt.Sprintf(`{"failed_at":%q}`, rows[0].FailedAt.Format(time.RFC3339Nano))
	r = call(h, "POST", "/dead-letters/"+id.String()+"/requeue", body)
	if r.Code != 200 {
		t.Fatalf("%d %s", r.Code, r.Body)
	}
}

type deadlineWriter struct {
	*httptest.ResponseRecorder
	deadline time.Time
	reset    bool
	flushErr error
}

func (w *deadlineWriter) SetWriteDeadline(at time.Time) error {
	if at.IsZero() {
		w.reset = true
	} else {
		w.deadline = at
	}
	return nil
}
func (w *deadlineWriter) FlushError() error { return w.flushErr }
func TestDeadlinePrecedesStorageAndSurvivesFailedFlush(t *testing.T) {
	w := &deadlineWriter{ResponseRecorder: httptest.NewRecorder(), flushErr: errors.New("write timeout")}
	s := &fakeStore{audit: func(ctx context.Context) (badgerbox.AuditReport, error) {
		if w.deadline.IsZero() {
			t.Error("deadline set after storage")
		}
		deadline, _ := ctx.Deadline()
		if !deadline.Equal(w.deadline) {
			t.Error("storage and write deadlines differ")
		}
		return badgerbox.AuditReport{Complete: true}, nil
	}}
	h, _ := New(s, Options{Namespace: "test"})
	h.ServeHTTP(w, httptest.NewRequest("GET", "/audit", nil))
	if w.reset {
		t.Fatal("failed flush lost deadline")
	}
}

func TestSlowRequeueBodyHasReadDeadline(t *testing.T) {
	h, _ := New(&fakeStore{}, Options{Namespace: "test", Timeout: 50 * time.Millisecond})
	finished := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(w, r); finished <- struct{}{} }))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "POST /dead-letters/1/requeue HTTP/1.1\r\nHost: test\r\nContent-Length: 100\r\n\r\n{"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("slow request body bypassed timeout")
	}
}
