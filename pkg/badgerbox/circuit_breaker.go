package badgerbox

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
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
	// MessageErrorCooldown defaults to min(1s, InitialCooldown).
	MessageErrorCooldown time.Duration
	// DisableJitter uses exact cooldowns instead of independently sampling 80–100%.
	DisableJitter bool
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
	if o.FailureThreshold < 0 || o.InitialCooldown < 0 || o.MaxCooldown < 0 || o.MessageErrorCooldown < 0 {
		return nil, fmt.Errorf("badgerbox: circuit breaker options must not be negative")
	}
	if o.FailureThreshold == 0 {
		o.FailureThreshold = 3
	}
	if o.InitialCooldown == 0 {
		o.InitialCooldown = 5 * time.Second
	}
	if o.MaxCooldown == 0 {
		o.MaxCooldown = o.InitialCooldown
	}
	if o.MessageErrorCooldown == 0 {
		o.MessageErrorCooldown = min(time.Second, o.InitialCooldown)
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
	trialID    uint64
}

type circuitBreaker struct {
	mu          sync.Mutex
	opts        CircuitBreakerOptions
	runtime     Runtime
	now         func() time.Time
	random      func() float64
	obs         *otelInstrumentation
	state       circuitState
	generation  uint64
	failures    int
	cooldown    time.Duration
	limiter     *rate.Limiter
	reservation *rate.Reservation
	// Trial ownership survives state/generation changes until invocation cleanup.
	trialID     uint64
	nextTrialID uint64
	changed     chan struct{}
	openedAt    time.Time
}

func newCircuitBreaker(opts *CircuitBreakerOptions, runtime Runtime, obs *otelInstrumentation) *circuitBreaker {
	if opts == nil {
		return nil
	}
	now := runtime.Now
	if elapsed, ok := runtime.(MonotonicRuntime); ok {
		now = elapsed.MonotonicNow
	}
	b := &circuitBreaker{opts: *opts, runtime: runtime, now: now, random: rand.Float64, obs: obs, state: circuitClosed, cooldown: opts.InitialCooldown, limiter: rate.NewLimiter(rate.Inf, 1), changed: make(chan struct{})}
	if obs != nil {
		obs.RecordCircuitState(context.Background(), string(circuitClosed))
	}
	return b
}

func (b *circuitBreaker) signalLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *circuitBreaker) changeLocked(state circuitState) func() {
	from := b.state
	b.state = state
	b.signalLocked()
	if b.obs != nil {
		b.obs.RecordCircuitTransition(context.Background(), string(from), string(state))
		if from == circuitOpen {
			b.obs.RecordCircuitOpenDuration(context.Background(), b.now().Sub(b.openedAt))
		}
	}
	return func() {
		if b.opts.OnStateChange != nil {
			b.opts.OnStateChange(string(from), string(state))
		}
	}
}

// minimumCircuitDelay rounds up, including sub-nanosecond fractional results.
func minimumCircuitDelay(nominal time.Duration) time.Duration {
	return nominal - nominal/5
}

func (b *circuitBreaker) sampleDelay(nominal time.Duration) time.Duration {
	if b.opts.DisableJitter {
		return nominal
	}
	minimum := minimumCircuitDelay(nominal)
	spread := nominal - minimum
	// Clamp before adding: float rounding near MaxInt64 must not overflow.
	offset := min(spread, time.Duration(b.random()*float64(spread)))
	return minimum + offset
}

func (b *circuitBreaker) deferralDelay() time.Duration {
	if b.opts.DisableJitter {
		return b.opts.InitialCooldown
	}
	return minimumCircuitDelay(b.opts.InitialCooldown)
}

func (b *circuitBreaker) openLocked(nominal time.Duration, reason string) func() {
	now := b.now()
	if b.reservation != nil {
		b.reservation.CancelAt(now)
	}
	b.generation++
	b.openedAt = now
	delay := b.sampleDelay(nominal)
	b.limiter = rate.NewLimiter(rate.Every(delay), 1)
	b.limiter.AllowN(now, 1) // Drain the initially full bucket: no immediate trial.
	b.reservation = b.limiter.ReserveN(now, 1)
	if b.obs != nil {
		b.obs.RecordCircuitRecoveryDelay(context.Background(), reason, delay)
	}
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
		delay := r.DelayFrom(b.now())
		b.mu.Unlock()
		if err := b.runtime.Sleep(ctx, delay); err != nil {
			r.CancelAt(b.now())
			return err
		}
		b.mu.Lock()
		if ctxErr(ctx) != nil {
			b.mu.Unlock()
			r.CancelAt(b.now())
			return ctx.Err()
		}
		if b.state != circuitOpen || b.generation != generation {
			b.mu.Unlock()
			continue
		}
		if r.DelayFrom(b.now()) > 0 {
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
	if b.state == circuitOpen || (p.trial && b.trialID != 0) {
		return p, false
	}
	if p.trial {
		b.nextTrialID++
		b.trialID = b.nextTrialID
		p.trialID = b.trialID
		return p, true
	}
	return p, b.limiter.AllowN(b.now(), 1)
}

func (b *circuitBreaker) start(p circuitPermit) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return p.generation == b.generation && b.state != circuitOpen && (!p.trial || p.trialID == b.trialID)
}

func (b *circuitBreaker) empty(p circuitPermit) {
	b.clearTrial(p, false)
}

func (b *circuitBreaker) releaseUnstarted(p circuitPermit) {
	b.finishTrial(p)
}

// finishTrial runs after settlement and joining the producer invocation. A prior
// generation can release its own occupancy without releasing a newer trial.
func (b *circuitBreaker) finishTrial(p circuitPermit) {
	b.clearTrial(p, true)
}

func (b *circuitBreaker) clearTrial(p circuitPermit, wake bool) {
	if b == nil || !p.trial {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if p.trialID != 0 && p.trialID == b.trialID {
		b.trialID = 0
		if wake {
			b.signalLocked()
		}
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

// report records unavailable failures before storage settlement. Other outcomes
// are reported after their acknowledgement or retry/DLQ settlement.
func (b *circuitBreaker) report(p circuitPermit, success, unavailable bool) {
	if b == nil {
		return
	}
	b.mu.Lock()
	if p.generation != b.generation || (p.trial && p.trialID != b.trialID) {
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
			if unavailable {
				notify = b.openLocked(b.cooldown, "unavailable")
			} else {
				notify = b.openLocked(b.opts.MessageErrorCooldown, "message_error")
			}
		}
	} else if b.state == circuitClosed {
		if success {
			b.failures = 0
		} else if unavailable {
			b.failures++
			if b.failures >= b.opts.FailureThreshold {
				notify = b.openLocked(b.cooldown, "unavailable")
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
		b.reservation.CancelAt(b.now())
	}
}
