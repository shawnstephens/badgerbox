package badgerbox

import (
	"strconv"
	"time"
)

type MessageID uint64

func (id MessageID) String() string { return strconv.FormatUint(uint64(id), 10) }

type MessageState string

const (
	MessageStateReady      MessageState = "ready"
	MessageStateProcessing MessageState = "processing"
)

type Message[M any, D any] struct {
	State       MessageState
	ID          MessageID
	Payload     M
	Destination D
	CreatedAt   time.Time
	AvailableAt time.Time
	Attempt     int
	MaxAttempts int
}

type DeadLetter[M any, D any] struct {
	Message  Message[M, D]
	FailedAt time.Time
	// Error is valid UTF-8, limited to 4 KiB with a marker when truncated.
	Error     string
	Permanent bool
}

type EnqueueRequest[M any, D any] struct {
	Payload     M
	Destination D
	AvailableAt time.Time
}
