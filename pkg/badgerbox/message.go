package badgerbox

import (
	"context"
	"time"
)

type MessageID uint64

type Serde[M any, D any] struct {
	Message     Codec[M]
	Destination Codec[D]
}

type Message[M any, D any] struct {
	ID          MessageID
	Payload     M
	Destination D
	CreatedAt   time.Time
	AvailableAt time.Time
	Attempt     int
	MaxAttempts int
}

type DeadLetter[M any, D any] struct {
	Message   Message[M, D]
	FailedAt  time.Time
	Error     string
	Permanent bool
}

type EnqueueRequest[M any, D any] struct {
	Payload     M
	Destination D
	AvailableAt time.Time
}

type Options struct {
	Namespace   string
	IDLeaseSize uint64
}

type ProcessFunc[M any, D any] func(ctx context.Context, msg Message[M, D]) error

type ProcessorOptions struct {
	Concurrency    int
	ClaimBatchSize int
	PollInterval   time.Duration
	LeaseDuration  time.Duration
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	MaxAttempts    int
}
