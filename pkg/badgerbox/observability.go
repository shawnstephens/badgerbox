package badgerbox

import (
	"context"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/internal/instrumentation"
	"github.com/shawnstephens/badgerbox/pkg/telemetry"
	"time"
)

const (
	metricModeExpired = "expired"
	metricModeManual  = "manual"
	metricModeRetry   = "retry"
)

const (
	metricOutcomeCommitted  = "committed"
	metricOutcomeDeadLetter = "dead_letter"
	metricOutcomePrepared   = "prepared"
	metricOutcomeRetried    = "retried"
	metricOutcomeSuccess    = "success"
)

const defaultInstrumentationName = "github.com/shawnstephens/badgerbox"

const defaultObservabilityPollInterval = 5 * time.Second

type ObservabilityOptions = telemetry.Options
type QueueSnapshot = telemetry.QueueSnapshot
type queueSnapshot = QueueSnapshot
type otelInstrumentation = instrumentation.Queue

func newOTelInstrumentation(opts ObservabilityOptions, namespace string, snapshot func(context.Context) (queueSnapshot, error)) (*otelInstrumentation, error) {
	return instrumentation.NewQueue(opts, namespace, snapshot, failureKind)
}

func (s *Store[M, D]) queueSnapshot(ctx context.Context) (QueueSnapshot, error) {
	return s.QueueSnapshot(ctx)
}

// QueueSnapshot reads lifecycle and creation index keys without decoding payloads.
// Depth collection is O(N); use Audit for row/index reconciliation.
// It returns ErrInconsistentIndex when a nonempty state has no creation index.
func (s *Store[M, D]) QueueSnapshot(ctx context.Context) (QueueSnapshot, error) {
	if err := s.ensureOpen(); err != nil {
		return QueueSnapshot{}, err
	}
	if err := ctxErr(ctx); err != nil {
		return QueueSnapshot{}, err
	}
	var result QueueSnapshot
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		result, err = s.loadQueueSnapshotFromIndexes(ctx, txn, s.runtime.Now().UTC())
		return err
	})
	return result, err
}

func normalizeObservabilityOptions(opts ObservabilityOptions) ObservabilityOptions {
	if opts.PollInterval <= 0 {
		opts.PollInterval = defaultObservabilityPollInterval
	}
	return opts
}

func failureKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrClaimTooLarge):
		return "claim_bytes"
	case errors.Is(err, ErrCodecDecode):
		return "codec"
	case IsPermanent(err):
		return "permanent"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "context"
	default:
		return "error"
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func positiveDuration(value time.Duration) time.Duration {
	if value < 0 {
		return 0
	}
	return value
}

func contextWithOTelInstrumentation(ctx context.Context, o *otelInstrumentation) context.Context {
	return instrumentation.WithDeliveryObserver(ctx, o)
}
