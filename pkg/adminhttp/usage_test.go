package adminhttp

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
)

func TestUsageAndAuditExposePersistedAccounting(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	limits := badgerbox.AdmissionLimits{MaxRetainedMessages: 2, MaxRetainedBytes: 1 << 20}
	store, err := badgerbox.New[string, string](db, badgerbox.Serde[string, string]{}, badgerbox.Options{Namespace: "usage", AdmissionLimits: limits})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Enqueue(t.Context(), badgerbox.EnqueueRequest[string, string]{Payload: "secret contents"}); err != nil {
		t.Fatal(err)
	}
	h, err := New(store, Options{Namespace: "usage"})
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []badgerbox.AdmissionLimits{limits, {}} {
		usage, err := store.Usage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CompareAndSwapAdmissionLimits(t.Context(), usage.Limits, next); err != nil {
			t.Fatal(err)
		}
		usage.Limits = next
		r := call(h, "GET", "/usage", "")
		var response usageResponse
		if r.Code != 200 || json.Unmarshal(r.Body.Bytes(), &response) != nil || response.Namespace != "usage" || response.UsageSnapshot != usage {
			t.Fatalf("usage response: %d %s", r.Code, r.Body)
		}
		if strings.Contains(r.Body.String(), "secret contents") || strings.Contains(r.Body.String(), "payload") {
			t.Fatal("usage exposed message content")
		}
		r = call(h, "GET", "/audit", "")
		var audit struct {
			Usage badgerbox.AuditUsageReport `json:"usage"`
		}
		if r.Code != 200 || json.Unmarshal(r.Body.Bytes(), &audit) != nil || !audit.Usage.Matches || audit.Usage.Persisted != usage {
			t.Fatalf("audit response: %d %s", r.Code, r.Body)
		}
	}
}

func TestUsagePreservesUint64JSONAndBoundsRequests(t *testing.T) {
	want := badgerbox.UsageSnapshot{Limits: badgerbox.AdmissionLimits{MaxRetainedMessages: math.MaxUint64, MaxRetainedBytes: 0}, RetainedMessages: math.MaxUint64 - 1, RetainedBytes: math.MaxUint64}
	s := &fakeStore{usage: func(context.Context) (badgerbox.UsageSnapshot, error) { return want, nil }}
	h, err := New(s, Options{Namespace: "test", MaxResponseBytes: 1024, MaxConcurrentUsage: 1})
	if err != nil {
		t.Fatal(err)
	}
	r := call(h, "GET", "/usage", "")
	var got usageResponse
	if r.Code != 200 || r.Body.Len() > 1024 || json.Unmarshal(r.Body.Bytes(), &got) != nil || got.UsageSnapshot != want {
		t.Fatalf("uint64 response: %d %s", r.Code, r.Body)
	}
	for _, path := range []string{"/usage?unexpected=1", "/usage?bad=%zz"} {
		if r := call(h, "GET", path, ""); r.Code != http.StatusBadRequest {
			t.Fatalf("%s response=%d", path, r.Code)
		}
	}
	if r := call(h, "POST", "/usage", `{}`); r.Code != http.StatusMethodNotAllowed {
		t.Fatalf("usage accepted mutation method: %d", r.Code)
	}
	if _, err := New(s, Options{Namespace: "test", MaxConcurrentUsage: -1}); err == nil {
		t.Fatal("invalid usage concurrency accepted")
	}
	entered, blocked, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(writer{httptest.NewRecorder(), blocked, entered}, httptest.NewRequest("GET", "/usage", nil))
	}()
	<-entered
	if r := call(h, "GET", "/usage", ""); r.Code != 429 || r.Header().Get("Retry-After") != "1" {
		t.Fatalf("usage slot released before flush: %d", r.Code)
	}
	// Slow usage responses must not consume the separate dead-letter page slots.
	if r := call(h, "GET", "/dead-letters", ""); r.Code != 200 {
		t.Fatalf("usage blocked independent inspection: %d", r.Code)
	}
	close(blocked)
	<-done
	if r := call(h, "GET", "/usage", ""); r.Code != 200 {
		t.Fatalf("usage slot leaked: %d", r.Code)
	}
}

func TestUsageDeadlineUnavailableAndPrivateErrors(t *testing.T) {
	s := &fakeStore{usage: func(ctx context.Context) (badgerbox.UsageSnapshot, error) {
		<-ctx.Done()
		return badgerbox.UsageSnapshot{}, ctx.Err()
	}}
	h, err := New(s, Options{Namespace: "test", Timeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if r := call(h, "GET", "/usage", ""); r.Code != 504 {
		t.Fatalf("usage deadline=%d", r.Code)
	}
	s.usage = func(context.Context) (badgerbox.UsageSnapshot, error) {
		return badgerbox.UsageSnapshot{}, errors.New("private disk detail")
	}
	if r := call(h, "GET", "/usage", ""); r.Code != 500 || strings.Contains(r.Body.String(), "private") {
		t.Fatalf("private error leaked: %d %s", r.Code, r.Body)
	}
	h, _ = New(nil, Options{Namespace: "test"})
	if r := call(h, "GET", "/usage", ""); r.Code != 503 {
		t.Fatalf("unavailable usage=%d", r.Code)
	}
}
