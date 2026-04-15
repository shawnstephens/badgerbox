package badgerbox

import "time"

type MessageID uint64

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
