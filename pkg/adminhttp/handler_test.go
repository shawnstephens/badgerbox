package adminhttp

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeStore struct {
	audit   func(context.Context) (badgerbox.AuditReport, error)
	letters []badgerbox.DeadLetter[[]byte, string]
	next    []byte
	failed  time.Time
	opts    badgerbox.DeadLetterListOptions
}

func (s *fakeStore) Audit(ctx context.Context, _ badgerbox.AuditOptions) (badgerbox.AuditReport, error) {
	if s.audit != nil {
		return s.audit(ctx)
	}
	return badgerbox.AuditReport{Namespace: "test"}, nil
}
func (s *fakeStore) ListDeadLettersWithOptions(_ context.Context, o badgerbox.DeadLetterListOptions) ([]badgerbox.DeadLetter[[]byte, string], []byte, error) {
	s.opts = o
	return s.letters, s.next, nil
}
func (s *fakeStore) RequeueDeadLetter(_ context.Context, _ badgerbox.MessageID, f, _ time.Time) error {
	s.failed = f
	return nil
}

type writer struct {
	*httptest.ResponseRecorder
	blocked chan struct{}
	entered chan struct{}
}

func (w writer) SetWriteDeadline(time.Time) error { return nil }
func (w writer) FlushError() error {
	if w.entered != nil {
		close(w.entered)
		<-w.blocked
	}
	return nil
}
func call(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRecorder()
	h.ServeHTTP(writer{ResponseRecorder: r}, httptest.NewRequest(method, path, strings.NewReader(body)))
	return r
}
func TestGenericRoutesValidationAndLimits(t *testing.T) {
	s := &fakeStore{letters: []badgerbox.DeadLetter[[]byte, string]{{Message: badgerbox.Message[[]byte, string]{ID: 5, Payload: []byte{255, 0}, Destination: "custom"}}}, next: []byte("key")}
	h, err := New[[]byte, string](s, Options{Namespace: "test", MaxResponseBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	r := call(h, "GET", "/dead-letters", "")
	if r.Code != 200 || !strings.Contains(r.Body.String(), "/wA=") {
		t.Fatalf("%d %s", r.Code, r.Body)
	}
	if s.opts.MaxBytes != 256 {
		t.Fatal(s.opts)
	}
	var page deadLetterPage[[]byte, string]
	if err = json.Unmarshal(r.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	other, _ := New[[]byte, string](s, Options{Namespace: "other"})
	if r = call(other, "GET", "/dead-letters?cursor="+page.NextCursor, ""); r.Code != 400 {
		t.Fatal(r.Code)
	}
	for _, path := range []string{"/audit?sample_limit=0", "/dead-letters?page_size=1&page_size=2", "/dead-letters?cursor=!!!"} {
		if r = call(h, "GET", path, ""); r.Code != 400 {
			t.Fatalf("%s %d", path, r.Code)
		}
	}
	for _, body := range []string{`{}`, `{"failed_at":null}`, `{"failed_at":"2026-09-06T00:00:00Z","x":1}`, `{"failed_at":"2026-09-06T00:00:00Z"}{}`, strings.Repeat(" ", 17<<10) + `{}`} {
		if r = call(h, "POST", "/dead-letters/5/requeue", body); r.Code != 400 {
			t.Fatalf("body accepted: %d", r.Code)
		}
	}
	r = call(h, "POST", "/dead-letters/5/requeue", `{"failed_at":"2026-09-06T00:00:00.123456789Z"}`)
	if r.Code != 200 || s.failed.Nanosecond() != 123456789 {
		t.Fatalf("%d %v", r.Code, s.failed)
	}
	s.letters[0].Message.Payload = make([]byte, 2048)
	if r = call(h, "GET", "/dead-letters", ""); r.Code != 413 || r.Body.Len() > 1024 {
		t.Fatalf("%d bytes=%d", r.Code, r.Body.Len())
	}
	unsupported := httptest.NewRecorder()
	h.ServeHTTP(unsupported, httptest.NewRequest("GET", "/dead-letters", nil))
	if unsupported.Code != 500 || unsupported.Body.Len() > 256 {
		t.Fatalf("unsupported writer: %d", unsupported.Code)
	}
}
func TestAdmissionIncludesResponseFlush(t *testing.T) {
	h, _ := New[[]byte, string](&fakeStore{}, Options{Namespace: "test", MaxConcurrentLists: 1})
	entered, blocked, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(writer{httptest.NewRecorder(), blocked, entered}, httptest.NewRequest("GET", "/dead-letters", nil))
	}()
	<-entered
	if r := call(h, "GET", "/dead-letters", ""); r.Code != 429 {
		t.Fatalf("admission released before flush: %d", r.Code)
	}
	close(blocked)
	<-done
	if r := call(h, "GET", "/dead-letters", ""); r.Code != 200 {
		t.Fatal(r.Code)
	}
}
func TestAuditTimeout(t *testing.T) {
	s := &fakeStore{audit: func(ctx context.Context) (badgerbox.AuditReport, error) {
		<-ctx.Done()
		return badgerbox.AuditReport{}, ctx.Err()
	}}
	h, _ := New[[]byte, string](s, Options{Namespace: "test", Timeout: time.Millisecond})
	if r := call(h, "GET", "/audit", ""); r.Code != 504 {
		t.Fatal(r.Code)
	}
	s.audit = func(context.Context) (badgerbox.AuditReport, error) {
		return badgerbox.AuditReport{}, errors.New("private detail")
	}
	if r := call(h, "GET", "/audit", ""); strings.Contains(r.Body.String(), "private detail") {
		t.Fatal("error detail leaked")
	}
}
