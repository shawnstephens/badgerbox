package badgerbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

type Runtime interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
	NewTicker(time.Duration) Ticker
	NewLeaseToken() (string, error)
}

type Ticker interface {
	Chan() <-chan time.Time
	Stop()
}

type SystemRuntime struct{}

func (SystemRuntime) Now() time.Time {
	return time.Now().UTC()
}

func (SystemRuntime) Sleep(ctx context.Context, delay time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (SystemRuntime) NewTicker(interval time.Duration) Ticker {
	return systemTicker{ticker: time.NewTicker(interval)}
}

func (SystemRuntime) NewLeaseToken() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", fmt.Errorf("badgerbox: generate lease token: %w", err)
	}
	return hex.EncodeToString(data[:]), nil
}

type systemTicker struct {
	ticker *time.Ticker
}

func (t systemTicker) Chan() <-chan time.Time {
	return t.ticker.C
}

func (t systemTicker) Stop() {
	t.ticker.Stop()
}
