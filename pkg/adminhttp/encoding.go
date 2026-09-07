package adminhttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
)

var errResponseBudget = errors.New("queue response budget exceeded")

func boundedText(text string, limit int) (string, bool) {
	if len(text) <= limit {
		return text, false
	}
	n := limit
	for n > 0 && !utf8.RuneStart(text[n]) {
		n--
	}
	return text[:n], true
}

// Encode one bounded metadata entry at a time. Never marshal a page containing
// arbitrary application objects, or build an oversized page and check afterward.
func (h handler) encodeDeadLetters(ctx context.Context, rows []badgerbox.DeadLetterMetadata, next []byte) ([]byte, error) {
	data := []byte(`{"dead_letters":[`)
	var suffix []byte
	count := 0
	for i, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cursor := row.Cursor
		if i == len(rows)-1 && len(next) == 0 {
			cursor = nil
		}
		encodedCursor, err := h.encodeCursor(cursor)
		if err != nil {
			return nil, err
		}
		cursorJSON, _ := json.Marshal(encodedCursor)
		candidateSuffix := append([]byte(`],"next_cursor":`), cursorJSON...)
		candidateSuffix = append(candidateSuffix, '}')
		result := newDeadLetterResponse(row)
		encoded, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		comma := 0
		if count > 0 {
			comma = 1
		}
		if len(data)+comma+len(encoded)+len(candidateSuffix)+1 > h.maxResponseBytes {
			if count > 0 {
				break
			}
			// A long failure summary must not make the first ordinary row inaccessible.
			if result.Metadata != nil && result.Metadata.FailureText != "" {
				result.Metadata.FailureText = ""
				result.Metadata.FailureTextTruncated = true
				encoded, err = json.Marshal(result)
				if err != nil {
					return nil, err
				}
			}
			if len(data)+len(encoded)+len(candidateSuffix)+1 > h.maxResponseBytes {
				return nil, errResponseBudget
			}
		}
		if count > 0 {
			data = append(data, ',')
		}
		data = append(data, encoded...)
		suffix = candidateSuffix
		count++
	}
	if count == 0 {
		if len(rows) > 0 {
			return nil, errResponseBudget
		}
		suffix = []byte(`],"next_cursor":""}`)
	}
	return append(data, suffix...), nil
}

type auditSummary struct {
	Complete     bool                            `json:"complete"`
	Code         string                          `json:"code,omitempty"`
	GeneratedAt  time.Time                       `json:"generated_at"`
	Namespace    string                          `json:"namespace"`
	ScannedKeys  int64                           `json:"scanned_keys"`
	ScannedBytes int64                           `json:"scanned_bytes"`
	LiveRows     int64                           `json:"live_rows"`
	States       badgerbox.AuditStateReports     `json:"states"`
	DeadLetters  badgerbox.AuditDeadLetterReport `json:"dead_letters"`
	Usage        badgerbox.AuditUsageReport      `json:"usage"`
}

func (h handler) encodeAudit(ctx context.Context, report badgerbox.AuditReport, code string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	summary := auditSummary{Complete: report.Complete, Code: code, GeneratedAt: report.GeneratedAt, Namespace: h.namespace, ScannedKeys: report.ScannedKeys, ScannedBytes: report.ScannedBytes, LiveRows: report.LiveRows, States: report.States, DeadLetters: report.DeadLetters, Usage: report.Usage}
	data, err := json.Marshal(summary)
	if err != nil {
		return nil, err
	}
	data = append(data[:len(data)-1], []byte(`,"samples":{"anomalies":[`)...)
	const suffixFalse = `]},"samples_truncated":false}`
	const suffixTrue = `]},"samples_truncated":true}`
	if len(data)+len(suffixFalse)+1 > h.maxResponseBytes {
		return nil, errResponseBudget
	}
	count := 0
	for _, sample := range report.Samples.Anomalies {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// These strings are enums in the built-in store. Bound external Store
		// implementations too before encoding any sample.
		if len(sample.State) > 64 || len(sample.IndexKind) > 64 || len(sample.AnomalyKind) > 64 || len(sample.ExpectedState) > 64 || len(sample.ActualState) > 64 {
			return nil, errors.New("invalid audit sample")
		}
		encoded, err := json.Marshal(sample)
		if err != nil {
			return nil, err
		}
		comma := 0
		if count > 0 {
			comma = 1
		}
		if len(data)+comma+len(encoded)+len(suffixFalse)+1 > h.maxResponseBytes {
			break
		}
		if count > 0 {
			data = append(data, ',')
		}
		data = append(data, encoded...)
		count++
	}
	suffix := suffixFalse
	if count < len(report.Samples.Anomalies) {
		suffix = suffixTrue
	}
	return append(data, []byte(suffix)...), nil
}

func (h handler) writeEncodingError(w http.ResponseWriter, ctx context.Context, err error) {
	if errors.Is(err, errResponseBudget) {
		h.writeError(w, http.StatusRequestEntityTooLarge, "queue response too large; increase response budget")
		return
	}
	h.writeOperationError(w, ctx, err)
}

// Preserve an explicit incomplete result even when full diagnostics cannot fit.
type auditProgress struct {
	Complete     bool   `json:"complete"`
	Code         string `json:"code"`
	ScannedKeys  int64  `json:"scanned_keys"`
	ScannedBytes int64  `json:"scanned_bytes"`
}
