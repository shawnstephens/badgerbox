// Package adminhttp exposes bounded administration routes for a generic queue.
package adminhttp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
)

const (
	defaultTimeout            = 30 * time.Second
	defaultMaxResponseBytes   = 8 << 20
	defaultMaxConcurrentLists = 4
	maxRequestBody            = 16 << 10
)

// Store is the generic administration surface used by the handler.
type Store interface {
	Audit(context.Context, badgerbox.AuditOptions) (badgerbox.AuditReport, error)
	ListDeadLetterMetadata(context.Context, badgerbox.DeadLetterListOptions) ([]badgerbox.DeadLetterMetadata, []byte, error)
	RequeueDeadLetterWithOptions(context.Context, badgerbox.MessageID, time.Time, badgerbox.DeadLetterRequeueOptions) error
}

// Options configures a namespace-bound administration handler.
type Options struct {
	// Namespace binds reports and opaque cursors to one queue (at most 256 bytes).
	Namespace string
	// Timeout bounds the entire request, including body reading, storage and flushing.
	// Nonpositive values use 30 seconds.
	Timeout time.Duration
	// MaxResponseBytes caps encoded JSON responses including the final newline.
	// Zero uses 8 MiB; explicit values must be at least 1 KiB. Dead-letter reads
	// additionally cap stored bytes at one quarter of this budget before decode.
	MaxResponseBytes int
	// MaxConcurrentLists limits concurrent dead-letter pages, including response
	// writing. Zero uses four. Audits always allow only one in-flight request.
	// Excess requests receive HTTP 429 immediately rather than waiting.
	MaxConcurrentLists int
	// MaxConcurrentRequeues bounds mutations through response flushing. Zero uses one.
	MaxConcurrentRequeues int
	// MaxRequeueBytes caps stored bytes before loading a requeue record. Zero uses 2 MiB.
	MaxRequeueBytes int64
	// Now supplies the default requeue availability time. Nil uses time.Now.
	Now func() time.Time
}

// New returns a standard HTTP handler with relative audit, dead-letter, and
// requeue routes. A nil store keeps the routes available and returns HTTP 503.
// Network middleware must preserve http.ResponseController read/write-deadline and
// flush support. An unsupported writer receives a small HTTP 500 error, never
// the potentially large response. Reuse one handler per namespace to share its
// admission limits.
func New(store Store, options Options) (http.Handler, error) {
	namespace := strings.TrimSpace(options.Namespace)
	if namespace == "" {
		return nil, fmt.Errorf("queue admin namespace is empty")
	}
	if options.Timeout <= 0 {
		options.Timeout = defaultTimeout
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = defaultMaxResponseBytes
	}
	if options.MaxResponseBytes < 1<<10 {
		return nil, fmt.Errorf("queue admin max response bytes must be at least 1024")
	}
	if options.MaxConcurrentLists == 0 {
		options.MaxConcurrentLists = defaultMaxConcurrentLists
	}
	if options.MaxConcurrentLists < 1 {
		return nil, fmt.Errorf("queue admin max concurrent lists must be positive")
	}
	if len(namespace) > 256 {
		return nil, fmt.Errorf("queue admin namespace must be at most 256 bytes")
	}
	if options.MaxConcurrentRequeues == 0 {
		options.MaxConcurrentRequeues = 1
	}
	if options.MaxConcurrentRequeues < 1 {
		return nil, fmt.Errorf("queue admin max concurrent requeues must be positive")
	}
	if options.MaxRequeueBytes == 0 {
		options.MaxRequeueBytes = 2 << 20
	}
	if options.MaxRequeueBytes < 1 {
		return nil, fmt.Errorf("queue admin max requeue bytes must be positive")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	h := handler{
		store: store, namespace: namespace, timeout: options.Timeout, now: options.Now,
		maxResponseBytes: options.MaxResponseBytes,
		maxRequeueBytes:  options.MaxRequeueBytes, requeueSlots: make(chan struct{}, options.MaxConcurrentRequeues),
		auditSlots: make(chan struct{}, 1), listSlots: make(chan struct{}, options.MaxConcurrentLists),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /audit", h.withDeadline(h.admit(h.auditSlots, h.audit)))
	mux.HandleFunc("GET /dead-letters", h.withDeadline(h.admit(h.listSlots, h.listDeadLetters)))
	mux.HandleFunc("POST /dead-letters/{message_id}/requeue", h.withDeadline(h.admit(h.requeueSlots, h.requeueDeadLetter)))
	return mux, nil
}

type handler struct {
	store            Store
	namespace        string
	timeout          time.Duration
	now              func() time.Time
	maxResponseBytes int
	maxRequeueBytes  int64
	requeueSlots     chan struct{}
	auditSlots       chan struct{}
	listSlots        chan struct{}
}

type deadLetterPage struct {
	DeadLetters []deadLetterResponse `json:"dead_letters"`
	NextCursor  string               `json:"next_cursor"`
}
type deadLetterResponse struct {
	MessageID   string             `json:"message_id"`
	Status      string             `json:"status"`
	FailedAt    time.Time          `json:"failed_at"`
	StoredBytes int64              `json:"stored_bytes"`
	Oversized   bool               `json:"oversized"`
	Metadata    *deadLetterDetails `json:"metadata,omitempty"`
}
type deadLetterDetails struct {
	CreatedAt            time.Time `json:"created_at"`
	AvailableAt          time.Time `json:"available_at"`
	Attempt              int       `json:"attempt"`
	MaxAttempts          int       `json:"max_attempts"`
	FailureText          string    `json:"failure_text"`
	FailureTextTruncated bool      `json:"failure_text_truncated"`
	Permanent            bool      `json:"permanent"`
}

type requeueRequest struct {
	FailedAt    json.RawMessage `json:"failed_at"`
	AvailableAt json.RawMessage `json:"available_at"`
}

type requeueResponse struct {
	MessageID   string    `json:"message_id"`
	Status      string    `json:"status"`
	AvailableAt time.Time `json:"available_at"`
}

type cursorEnvelope struct {
	Namespace string `json:"namespace"`
	Cursor    string `json:"cursor"`
}

func (h handler) admit(slots chan struct{}, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			next(w, request)
		default:
			w.Header().Set("Retry-After", "1")
			h.writeError(w, http.StatusTooManyRequests, "queue administration busy; retry later")
		}
	}
}

func (h handler) audit(w http.ResponseWriter, request *http.Request) {
	query, err := parseQuery(request)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	sampleLimit, err := queryInt(query, "sample_limit", 20, 1, 100)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if h.store == nil {
		h.writeError(w, http.StatusServiceUnavailable, "queue runtime unavailable")
		return
	}
	ctx := request.Context()
	report, err := h.store.Audit(ctx, badgerbox.AuditOptions{SampleLimit: sampleLimit})
	if err != nil && !errors.Is(err, badgerbox.ErrAuditLimitExceeded) {
		h.writeOperationError(w, ctx, err)
		return
	}
	status := http.StatusOK
	code := ""
	if err != nil {
		report.Complete = false
		status = http.StatusRequestEntityTooLarge
		code = "audit_incomplete"
	}
	data, encodeErr := h.encodeAudit(ctx, report, code)
	if errors.Is(encodeErr, errResponseBudget) && code == "audit_incomplete" {
		h.writeJSON(w, http.StatusRequestEntityTooLarge, auditProgress{Code: code, ScannedKeys: report.ScannedKeys, ScannedBytes: report.ScannedBytes})
		return
	}
	if encodeErr != nil {
		h.writeEncodingError(w, ctx, encodeErr)
		return
	}
	h.writeData(w, status, data)
}

func (h handler) listDeadLetters(w http.ResponseWriter, request *http.Request) {
	query, err := parseQuery(request)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	pageSize, err := queryInt(query, "page_size", 100, 1, 1000)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cursor, err := h.decodeCursor(query)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if h.store == nil {
		h.writeError(w, http.StatusServiceUnavailable, "queue runtime unavailable")
		return
	}
	ctx := request.Context()
	deadLetters, nextCursor, err := h.store.ListDeadLetterMetadata(ctx, badgerbox.DeadLetterListOptions{
		Limit: pageSize, Cursor: cursor, MaxBytes: int64(h.maxResponseBytes / 4),
	})
	if err != nil {
		h.writeOperationError(w, ctx, err)
		return
	}
	data, err := h.encodeDeadLetters(ctx, deadLetters, nextCursor)
	if err != nil {
		h.writeEncodingError(w, ctx, err)
		return
	}
	h.writeData(w, http.StatusOK, data)
}

func (h handler) requeueDeadLetter(w http.ResponseWriter, request *http.Request) {
	messageID, err := strconv.ParseUint(request.PathValue("message_id"), 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "message_id must be a decimal uint64")
		return
	}
	failedAt, availableAt, err := decodeRequeueRequest(w, request)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if h.store == nil {
		h.writeError(w, http.StatusServiceUnavailable, "queue runtime unavailable")
		return
	}
	if availableAt.IsZero() {
		availableAt = h.now().UTC()
	}
	ctx := request.Context()
	err = h.store.RequeueDeadLetterWithOptions(ctx, badgerbox.MessageID(messageID), failedAt, badgerbox.DeadLetterRequeueOptions{AvailableAt: availableAt, MaxBytes: h.maxRequeueBytes})
	if err != nil {
		h.writeOperationError(w, ctx, err)
		return
	}
	h.writeJSON(w, http.StatusOK, requeueResponse{
		MessageID: strconv.FormatUint(messageID, 10), Status: "requeued", AvailableAt: availableAt,
	})
}

func newDeadLetterResponse(row badgerbox.DeadLetterMetadata) deadLetterResponse {
	result := deadLetterResponse{MessageID: row.ID.String(), Status: "dead_letter", FailedAt: row.FailedAt, StoredBytes: row.StoredBytes, Oversized: row.Oversized}
	if d := row.Details; d != nil {
		text, truncated := boundedText(d.FailureText, 1024)
		result.Metadata = &deadLetterDetails{CreatedAt: d.CreatedAt, AvailableAt: d.AvailableAt, Attempt: d.Attempt, MaxAttempts: d.MaxAttempts, FailureText: text, FailureTextTruncated: d.FailureTextTruncated || truncated, Permanent: d.Permanent}
	}
	return result
}

func decodeRequeueRequest(w http.ResponseWriter, request *http.Request) (time.Time, time.Time, error) {
	request.Body = http.MaxBytesReader(w, request.Body, maxRequestBody)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var body requeueRequest
	if err := decoder.Decode(&body); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return time.Time{}, time.Time{}, err
	}
	failedAt, err := parseTime(body.FailedAt, "failed_at", true)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	availableAt, err := parseTime(body.AvailableAt, "available_at", false)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return failedAt, availableAt, nil
}

func parseQuery(request *http.Request) (url.Values, error) {
	values, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("query string is malformed: %w", err)
	}
	return values, nil
}

func queryInt(query url.Values, key string, fallback, minimum, maximum int) (int, error) {
	values, present := query[key]
	if !present {
		return fallback, nil
	}
	if len(values) != 1 {
		return 0, fmt.Errorf("%s must be specified once", key)
	}
	value, err := strconv.Atoi(values[0])
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d", key, minimum, maximum)
	}
	return value, nil
}

func (h handler) encodeCursor(cursor []byte) (string, error) {
	if len(cursor) == 0 {
		return "", nil
	}
	if len(cursor) > 1024 {
		return "", fmt.Errorf("queue cursor is too large")
	}
	encoded, err := json.Marshal(cursorEnvelope{
		Namespace: h.namespace, Cursor: base64.StdEncoding.EncodeToString(cursor),
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func (h handler) decodeCursor(query url.Values) ([]byte, error) {
	values, present := query["cursor"]
	if !present || len(values) == 1 && values[0] == "" {
		return nil, nil
	}
	if len(values) != 1 {
		return nil, fmt.Errorf("cursor must be specified once")
	}
	if len(values[0]) > 4096 {
		return nil, fmt.Errorf("cursor is too large")
	}
	encoded, err := base64.RawURLEncoding.DecodeString(values[0])
	if err != nil {
		return nil, fmt.Errorf("cursor must be opaque base64url: %w", err)
	}
	var envelope cursorEnvelope
	if unmarshalErr := json.Unmarshal(encoded, &envelope); unmarshalErr != nil {
		return nil, fmt.Errorf("cursor is malformed: %w", unmarshalErr)
	}
	if envelope.Namespace != h.namespace {
		return nil, fmt.Errorf("cursor belongs to another queue namespace")
	}
	cursor, err := base64.StdEncoding.DecodeString(envelope.Cursor)
	if err != nil || len(cursor) == 0 {
		return nil, fmt.Errorf("cursor is malformed")
	}
	return cursor, nil
}

func parseTime(raw json.RawMessage, field string, required bool) (time.Time, error) {
	if len(raw) == 0 {
		if required {
			return time.Time{}, fmt.Errorf("%s is required", field)
		}
		return time.Time{}, nil
	}
	var value string
	if string(raw) == "null" || json.Unmarshal(raw, &value) != nil || value == "" {
		return time.Time{}, fmt.Errorf("%s must be an RFC3339 string", field)
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be RFC3339: %w", field, err)
	}
	parsed = parsed.UTC()
	if !time.Unix(0, parsed.UnixNano()).UTC().Equal(parsed) {
		return time.Time{}, fmt.Errorf("%s must be representable as Unix nanoseconds", field)
	}
	return parsed, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("JSON body must contain exactly one object")
	}
	return nil
}

func (h handler) writeOperationError(w http.ResponseWriter, ctx context.Context, err error) {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
		h.writeError(w, http.StatusGatewayTimeout, "queue operation timed out")
	case errors.Is(err, badgerbox.ErrDeadLetterTooLarge):
		h.writeJSON(w, http.StatusRequestEntityTooLarge, errorResponse{Error: "dead letter exceeds administration requeue limit", Code: "dead_letter_too_large"})
	case errors.Is(err, badgerbox.ErrMessageTooLarge):
		h.writeJSON(w, http.StatusRequestEntityTooLarge, errorResponse{Error: "record exceeds lifecycle storage limits", Code: "message_too_large"})
	case errors.Is(err, badgerbox.ErrLiveMessageExists):
		h.writeError(w, http.StatusConflict, "message already exists in live queue")
	case errors.Is(err, badgerbox.ErrNotFound):
		h.writeError(w, http.StatusNotFound, "queue record not found")
	default:
		h.writeError(w, http.StatusInternalServerError, "queue storage operation failed")
	}
}

func (h handler) writeError(w http.ResponseWriter, status int, message string) {
	text, _ := boundedText(message, 128)
	h.writeJSON(w, status, errorResponse{Error: text})
}

// writeJSON is used only for the small, owned requeue and error response types.
func (h handler) writeJSON(w http.ResponseWriter, status int, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		status, data = http.StatusInternalServerError, []byte(`{"error":"queue response encoding failed"}`)
	}
	h.writeData(w, status, data)
}

type errorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}

func (h handler) writeData(w http.ResponseWriter, status int, data []byte) {
	if len(data) >= h.maxResponseBytes {
		status, data = http.StatusRequestEntityTooLarge, []byte(`{"error":"queue response too large"}`)
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)+1))
	w.WriteHeader(status)
	if _, err := w.Write(append(data, '\n')); err != nil {
		return
	}
	_ = http.NewResponseController(w).Flush()
}

func (h handler) withDeadline(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), h.timeout)
		defer cancel()
		deadline, _ := ctx.Deadline()
		controller := http.NewResponseController(w)
		if err := controller.SetWriteDeadline(deadline); err != nil {
			w.Header().Set("Connection", "close")
			h.writeError(w, http.StatusInternalServerError, "queue response deadlines unavailable")
			return
		}
		tracked := &flushTrackingWriter{ResponseWriter: w}
		defer func() {
			if tracked.flushed {
				_ = controller.SetWriteDeadline(time.Time{})
			}
		}()
		if err := controller.SetReadDeadline(deadline); err != nil {
			w.Header().Set("Connection", "close")
			h.writeError(w, http.StatusInternalServerError, "queue request deadlines unavailable")
			return
		}
		// net/http may drain an unread body while flushing any response,
		// including GET and HEAD. Keep its read deadline until that drain ends;
		// the server resets it when reading the next request.
		next(tracked, request.WithContext(ctx))
	}
}

// Preserve a failed flush's deadline for net/http's final buffered write.
type flushTrackingWriter struct {
	http.ResponseWriter
	flushed bool
}

func (w *flushTrackingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *flushTrackingWriter) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	w.flushed = err == nil
	return err
}
