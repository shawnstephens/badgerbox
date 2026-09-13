package badgerbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// CircuitBreakerOptions enables admission control for one processor's downstream.
// Zero fields select defaults. A nil *CircuitBreakerOptions disables the breaker.
type CircuitBreakerOptions struct {
	FailureThreshold int
	InitialCooldown  time.Duration
	MaxCooldown      time.Duration
	// IsUnavailable must be concurrency-safe. Nil uses badgerbox.IsUnavailable.
	IsUnavailable func(error) bool
	// OnStateChange is an optional, concurrency-safe, nonblocking notification.
	// It runs outside the breaker lock; concurrent notifications may arrive out of order.
	OnStateChange func(from, to string)
}

func normalizeCircuitBreakerOptions(opts *CircuitBreakerOptions) (*CircuitBreakerOptions, error) {
	if opts == nil {
		return nil, nil
	}
	o := *opts
	if o.FailureThreshold < 0 || o.InitialCooldown < 0 || o.MaxCooldown < 0 {
		return nil, fmt.Errorf("badgerbox: circuit breaker options must not be negative")
	}
	if o.FailureThreshold == 0 {
		o.FailureThreshold = 3
	}
	if o.InitialCooldown == 0 {
		o.InitialCooldown = 5 * time.Second
	}
	if o.MaxCooldown == 0 {
		o.MaxCooldown = time.Minute
	}
	if o.MaxCooldown < o.InitialCooldown {
		return nil, fmt.Errorf("badgerbox: circuit breaker maximum cooldown is below initial cooldown")
	}
	if o.IsUnavailable == nil {
		o.IsUnavailable = IsUnavailable
	}
	return &o, nil
}

type circuitState string

const (
	circuitClosed   circuitState = "closed"
	circuitOpen     circuitState = "open"
	circuitHalfOpen circuitState = "half_open"
)

type circuitPermit struct {
	generation uint64
	trial      bool
}

type circuitBreaker struct {
	mu          sync.Mutex
	opts        CircuitBreakerOptions
	runtime     Runtime
	obs         *otelInstrumentation
	state       circuitState
	generation  uint64
	failures    int
	cooldown    time.Duration
	limiter     *rate.Limiter
	reservation *rate.Reservation
	trial       bool
	changed     chan struct{}
	openedAt    time.Time
}

func newCircuitBreaker(opts *CircuitBreakerOptions, runtime Runtime, obs *otelInstrumentation) *circuitBreaker {
	if opts == nil {
		return nil
	}
	b := &circuitBreaker{opts: *opts, runtime: runtime, obs: obs, state: circuitClosed, cooldown: opts.InitialCooldown, limiter: rate.NewLimiter(rate.Inf, 1), changed: make(chan struct{})}
	if obs != nil {
		obs.RecordCircuitState(context.Background(), string(circuitClosed))
	}
	return b
}

func (b *circuitBreaker) changeLocked(state circuitState) func() {
	from := b.state
	b.state = state
	close(b.changed)
	b.changed = make(chan struct{})
	if b.obs != nil {
		b.obs.RecordCircuitTransition(context.Background(), string(from), string(state))
		if from == circuitOpen {
			b.obs.RecordCircuitOpenDuration(context.Background(), b.runtime.Now().Sub(b.openedAt))
		}
	}
	return func() {
		if b.opts.OnStateChange != nil {
			b.opts.OnStateChange(string(from), string(state))
		}
	}
}

func (b *circuitBreaker) openLocked() func() {
	now := b.runtime.Now()
	if b.reservation != nil {
		b.reservation.CancelAt(now)
	}
	b.generation++
	b.trial = false
	b.openedAt = now
	b.limiter = rate.NewLimiter(rate.Every(b.cooldown), 1)
	b.limiter.AllowN(now, 1) // Drain the initially full bucket: no immediate trial.
	b.reservation = b.limiter.ReserveN(now, 1)
	return b.changeLocked(circuitOpen)
}

// wait is only called by the dispatcher, after draining obsolete queued claims.
func (b *circuitBreaker) wait(ctx context.Context) error {
	if b == nil {
		return ctxErr(ctx)
	}
	for {
		if err := ctxErr(ctx); err != nil {
			return err
		}
		b.mu.Lock()
		if b.state != circuitOpen {
			b.mu.Unlock()
			return nil
		}
		r, generation := b.reservation, b.generation
		if !r.OK() {
			b.mu.Unlock()
			return fmt.Errorf("badgerbox: invalid circuit recovery reservation")
		}
		delay := r.DelayFrom(b.runtime.Now())
		b.mu.Unlock()
		if err := b.runtime.Sleep(ctx, delay); err != nil {
			r.CancelAt(b.runtime.Now())
			return err
		}
		b.mu.Lock()
		if ctxErr(ctx) != nil {
			b.mu.Unlock()
			r.CancelAt(b.runtime.Now())
			return ctx.Err()
		}
		if b.state != circuitOpen || b.generation != generation {
			b.mu.Unlock()
			continue
		}
		if r.DelayFrom(b.runtime.Now()) > 0 {
			b.mu.Unlock()
			continue
		}
		b.reservation = nil
		notify := b.changeLocked(circuitHalfOpen)
		b.mu.Unlock()
		notify()
		return nil
	}
}

func (b *circuitBreaker) admit() (circuitPermit, bool) {
	if b == nil {
		return circuitPermit{}, true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	p := circuitPermit{generation: b.generation, trial: b.state == circuitHalfOpen}
	if b.state == circuitOpen || b.trial {
		return p, false
	}
	if p.trial {
		b.trial = true
		return p, true
	}
	return p, b.limiter.AllowN(b.runtime.Now(), 1)
}

func (b *circuitBreaker) start(p circuitPermit) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return p.generation == b.generation && b.state != circuitOpen
}

func (b *circuitBreaker) empty(p circuitPermit) {
	if b == nil || !p.trial {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if p.generation == b.generation {
		b.trial = false
	}
}

func (b *circuitBreaker) signals() (circuitState, <-chan struct{}) {
	if b == nil {
		return circuitClosed, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state, b.changed
}

// report records producer failures before storage settlement; successful trial
// reports are delayed until acknowledgement has completed.
func (b *circuitBreaker) report(p circuitPermit, success, unavailable bool) {
	if b == nil {
		return
	}
	b.mu.Lock()
	if p.generation != b.generation {
		b.mu.Unlock()
		return
	}
	var notify func()
	if p.trial {
		if b.obs != nil {
			outcome := "message_error"
			if success {
				outcome = "success"
			} else if unavailable {
				outcome = "unavailable"
			}
			b.obs.RecordCircuitTrial(context.Background(), outcome)
		}
		if success {
			b.failures = 0
			b.cooldown = b.opts.InitialCooldown
			b.trial = false
			b.generation++
			b.limiter = rate.NewLimiter(rate.Inf, 1)
			notify = b.changeLocked(circuitClosed)
		} else {
			if unavailable {
				if b.cooldown > b.opts.MaxCooldown/2 {
					b.cooldown = b.opts.MaxCooldown
				} else {
					b.cooldown *= 2
				}
			}
			notify = b.openLocked()
		}
	} else if b.state == circuitClosed {
		if success {
			b.failures = 0
		} else if unavailable {
			b.failures++
			if b.failures >= b.opts.FailureThreshold {
				notify = b.openLocked()
			}
		}
	}
	b.mu.Unlock()
	if notify != nil {
		notify()
	}
}

func (b *circuitBreaker) unavailable(err error) bool {
	var panicErr producerPanicError
	return b != nil && err != nil && !errors.Is(err, context.Canceled) && !errors.As(err, &panicErr) && !IsPermanent(err) && b.opts.IsUnavailable(err)
}

func (b *circuitBreaker) close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.reservation != nil {
		b.reservation.CancelAt(b.runtime.Now())
	}
}
