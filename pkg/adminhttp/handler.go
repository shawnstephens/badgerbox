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
type Store[M, D any] interface {
	Audit(context.Context, badgerbox.AuditOptions) (badgerbox.AuditReport, error)
	ListDeadLettersWithOptions(context.Context, badgerbox.DeadLetterListOptions) ([]badgerbox.DeadLetter[M, D], []byte, error)
	RequeueDeadLetter(context.Context, badgerbox.MessageID, time.Time, time.Time) error
}

// Options configures a namespace-bound administration handler.
type Options struct {
	// Namespace binds reports and opaque pagination cursors to one queue namespace.
	Namespace string
	// Timeout separately bounds storage work and response writing. Nonpositive
	// values use 30 seconds for each phase.
	Timeout time.Duration
	// MaxResponseBytes caps encoded JSON responses including the final newline.
	// Zero uses 8 MiB; explicit values must be at least 1 KiB. Dead-letter reads
	// additionally cap stored bytes at one quarter of this budget before decode.
	MaxResponseBytes int
	// MaxConcurrentLists limits concurrent dead-letter pages, including response
	// writing. Zero uses four. Audits always allow only one in-flight request.
	// Excess requests receive HTTP 429 immediately rather than waiting.
	MaxConcurrentLists int
	// Now supplies the default requeue availability time. Nil uses time.Now.
	Now func() time.Time
}

// New returns a standard HTTP handler with relative audit, dead-letter, and
// requeue routes. A nil store keeps the routes available and returns HTTP 503.
// Network middleware must preserve http.ResponseController write-deadline and
// flush support. An unsupported writer receives a small HTTP 500 error, never
// the potentially large response. Reuse one handler per namespace to share its
// admission limits.
func New[M, D any](store Store[M, D], options Options) (http.Handler, error) {
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
	if options.Now == nil {
		options.Now = time.Now
	}
	h := handler[M, D]{
		store: store, namespace: namespace, timeout: options.Timeout, now: options.Now,
		maxResponseBytes: options.MaxResponseBytes,
		auditSlots:       make(chan struct{}, 1), listSlots: make(chan struct{}, options.MaxConcurrentLists),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /audit", h.admit(h.auditSlots, h.audit))
	mux.HandleFunc("GET /dead-letters", h.admit(h.listSlots, h.listDeadLetters))
	mux.HandleFunc("POST /dead-letters/{message_id}/requeue", h.requeueDeadLetter)
	return mux, nil
}

type handler[M, D any] struct {
	store            Store[M, D]
	namespace        string
	timeout          time.Duration
	now              func() time.Time
	maxResponseBytes int
	auditSlots       chan struct{}
	listSlots        chan struct{}
}

type deadLetterPage[M, D any] struct {
	DeadLetters []deadLetterResponse[M, D] `json:"dead_letters"`
	NextCursor  string                     `json:"next_cursor"`
}

type deadLetterResponse[M, D any] struct {
	MessageID   string    `json:"message_id"`
	Status      string    `json:"status"`
	Payload     M         `json:"payload"`
	Destination D         `json:"destination"`
	CreatedAt   time.Time `json:"created_at"`
	AvailableAt time.Time `json:"available_at"`
	FailedAt    time.Time `json:"failed_at"`
	Attempt     int       `json:"attempt"`
	MaxAttempts int       `json:"max_attempts"`
	FailureText string    `json:"failure_text"`
	Permanent   bool      `json:"permanent"`
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

func (h handler[M, D]) admit(slots chan struct{}, next http.HandlerFunc) http.HandlerFunc {
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

func (h handler[M, D]) audit(w http.ResponseWriter, request *http.Request) {
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
	ctx, cancel := context.WithTimeout(request.Context(), h.timeout)
	defer cancel()
	report, err := h.store.Audit(ctx, badgerbox.AuditOptions{SampleLimit: sampleLimit})
	if err != nil {
		h.writeOperationError(w, ctx, err)
		return
	}
	h.writeJSON(w, http.StatusOK, report)
}

func (h handler[M, D]) listDeadLetters(w http.ResponseWriter, request *http.Request) {
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
	ctx, cancel := context.WithTimeout(request.Context(), h.timeout)
	defer cancel()
	deadLetters, nextCursor, err := h.store.ListDeadLettersWithOptions(ctx, badgerbox.DeadLetterListOptions{
		Limit: pageSize, Cursor: cursor, MaxBytes: int64(h.maxResponseBytes / 4),
	})
	if err != nil {
		h.writeOperationError(w, ctx, err)
		return
	}
	response := deadLetterPage[M, D]{DeadLetters: make([]deadLetterResponse[M, D], 0, len(deadLetters))}
	for _, deadLetter := range deadLetters {
		response.DeadLetters = append(response.DeadLetters, newDeadLetterResponse(deadLetter))
	}
	response.NextCursor, err = h.encodeCursor(nextCursor)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "queue cursor encoding failed")
		return
	}
	h.writeJSON(w, http.StatusOK, response)
}

func (h handler[M, D]) requeueDeadLetter(w http.ResponseWriter, request *http.Request) {
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
	ctx, cancel := context.WithTimeout(request.Context(), h.timeout)
	defer cancel()
	err = h.store.RequeueDeadLetter(ctx, badgerbox.MessageID(messageID), failedAt, availableAt)
	if err != nil {
		h.writeOperationError(w, ctx, err)
		return
	}
	h.writeJSON(w, http.StatusOK, requeueResponse{
		MessageID: strconv.FormatUint(messageID, 10), Status: "requeued", AvailableAt: availableAt,
	})
}

func newDeadLetterResponse[M, D any](deadLetter badgerbox.DeadLetter[M, D]) deadLetterResponse[M, D] {
	message := deadLetter.Message
	return deadLetterResponse[M, D]{MessageID: message.ID.String(), Status: "dead_letter", Payload: message.Payload, Destination: message.Destination, CreatedAt: message.CreatedAt, AvailableAt: message.AvailableAt, FailedAt: deadLetter.FailedAt, Attempt: message.Attempt, MaxAttempts: message.MaxAttempts, FailureText: deadLetter.Error, Permanent: deadLetter.Permanent}
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

func (h handler[M, D]) encodeCursor(cursor []byte) (string, error) {
	if len(cursor) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(cursorEnvelope{
		Namespace: h.namespace, Cursor: base64.StdEncoding.EncodeToString(cursor),
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func (h handler[M, D]) decodeCursor(query url.Values) ([]byte, error) {
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

func (h handler[M, D]) writeOperationError(w http.ResponseWriter, ctx context.Context, err error) {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(ctx.Err(), context.DeadlineExceeded):
		h.writeError(w, http.StatusGatewayTimeout, "queue operation timed out")
	case errors.Is(err, badgerbox.ErrDeadLetterTooLarge):
		h.writeError(w, http.StatusRequestEntityTooLarge, "dead letter exceeds administration response limit")
	case errors.Is(err, badgerbox.ErrLiveMessageExists):
		h.writeError(w, http.StatusConflict, "message already exists in live queue")
	case errors.Is(err, badgerbox.ErrNotFound):
		h.writeError(w, http.StatusNotFound, "queue record not found")
	default:
		h.writeError(w, http.StatusInternalServerError, "queue storage operation failed")
	}
}

func (h handler[M, D]) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}

func (h handler[M, D]) writeJSON(w http.ResponseWriter, status int, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		status, data = http.StatusInternalServerError, []byte(`{"error":"queue response encoding failed"}`)
	}
	if len(data) >= h.maxResponseBytes { // Include the newline in the byte budget.
		status, data = http.StatusRequestEntityTooLarge, []byte(`{"error":"queue response too large; reduce page_size or sample_limit"}`)
	}
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(time.Now().Add(h.timeout)); err != nil {
		// Do not send a large response through middleware that hides deadlines.
		w.Header().Set("Connection", "close")
		status, data = http.StatusInternalServerError, []byte(`{"error":"queue response deadlines unavailable"}`)
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)+1))
	w.WriteHeader(status)
	if _, err := w.Write(append(data, '\n')); err != nil {
		return
	}
	// Flush even small buffered responses before clearing the deadline. Clearing
	// it first would leave net/http's post-handler flush unbounded.
	if err := controller.Flush(); err != nil {
		return
	}
	_ = controller.SetWriteDeadline(time.Time{})
}
