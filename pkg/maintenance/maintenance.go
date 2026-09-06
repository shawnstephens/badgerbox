// Package maintenance manages database-wide compaction and value-log collection.
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Operation identifies a database-wide maintenance operation.
type Operation string

const (
	// OperationFlatten identifies startup LSM flattening.
	OperationFlatten Operation = "flatten"
	// OperationValueLogGC identifies periodic value-log garbage collection.
	OperationValueLogGC Operation = "value_log_gc"
)

// Result describes one completed database-wide maintenance operation.
type Result struct {
	Operation Operation     // Operation identifies the completed maintenance operation.
	Duration  time.Duration // Duration is the nonnegative wall-clock operation duration.
	Err       error         // Err is the Badger result; ErrNoRewrite is normal for value-log GC.
}

// Observer receives completed maintenance results without controlling execution.
type Observer interface {
	ObserveMaintenance(context.Context, Result)
}

// Options configures startup Flatten and periodic value-log GC.
type Options struct {
	FlattenOnStartup       bool          // FlattenOnStartup enables synchronous startup LSM flattening.
	ValueLogGCInterval     time.Duration // ValueLogGCInterval controls periodic GC cadence; zero disables it.
	ValueLogGCDiscardRatio float64       // ValueLogGCDiscardRatio controls value-log rewrite eligibility.
	Clock                  Clock         // Clock optionally supplies time and ticker behavior.
	Logger                 *slog.Logger  // Logger optionally receives maintenance failures.
	Observer               Observer      // Observer optionally receives completed operation results.
}

type badgerMaintainer interface {
	Flatten(int) error
	RunValueLogGC(float64) error
}

// Service owns database-wide startup and periodic maintenance for a caller-owned Badger database.
// It never opens or closes the database. Stop waits for an in-flight Badger call, which cannot be canceled.
type Service struct {
	mu          sync.Mutex
	db          badgerMaintainer
	dbOptions   badger.Options
	options     Options
	startupRan  bool
	startupDone <-chan struct{}
	started     bool
	stopped     bool
	cancel      context.CancelFunc
	done        <-chan struct{}
}

// ValidateOptions rejects settings that make Badger maintenance unsafe or ineffective.
// Callers may use it before badger.Open so invalid settings do not create database files.
func ValidateOptions(dbOptions badger.Options, options Options) error {
	if options.ValueLogGCInterval < 0 {
		return maintenanceErrorf("Badger value-log GC interval must be greater than or equal to zero")
	}
	if options.ValueLogGCInterval > 0 &&
		!(options.ValueLogGCDiscardRatio > 0 && options.ValueLogGCDiscardRatio < 1) {
		return maintenanceErrorf("Badger value-log GC discard ratio must be strictly between zero and one")
	}
	if dbOptions.ReadOnly && options.FlattenOnStartup {
		return maintenanceErrorf("Badger startup Flatten cannot be enabled for a read-only database")
	}
	if options.ValueLogGCInterval > 0 && dbOptions.ReadOnly {
		return maintenanceErrorf("Badger value-log GC cannot be enabled for a read-only database")
	}
	if options.ValueLogGCInterval > 0 && dbOptions.InMemory {
		return maintenanceErrorf("Badger value-log GC cannot be enabled for an in-memory database")
	}
	if options.ValueLogGCInterval > 0 && dbOptions.ValueThreshold <= 0 {
		return maintenanceErrorf("Badger value threshold must be greater than zero when value-log GC is enabled")
	}
	return nil
}

// New binds maintenance to db without taking ownership of the database.
func New(db *badger.DB, options Options) (*Service, error) {
	if db == nil {
		return nil, ErrNilDB
	}
	return newService(db, db.Opts(), options)
}

func newService(
	db badgerMaintainer,
	dbOptions badger.Options,
	options Options,
) (*Service, error) {
	if db == nil {
		return nil, ErrNilDB
	}
	if err := ValidateOptions(dbOptions, options); err != nil {
		return nil, err
	}
	if options.Clock == nil {
		options.Clock = systemClock{}
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	return &Service{db: db, dbOptions: dbOptions, options: options}, nil
}

// RunStartup performs optional startup Flatten synchronously.
// The Badger error is logged, observed, and returned so callers can choose whether it is fatal.
func (m *Service) RunStartup(ctx context.Context) error {
	if m == nil || m.db == nil {
		return ErrNilDB
	}
	if err := ctxErr(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	if m.startupRan || m.started || m.stopped {
		m.mu.Unlock()
		return maintenanceErrorf("Badger startup maintenance is already run, started, or stopped")
	}
	startupDone := make(chan struct{})
	m.startupRan = true
	m.startupDone = startupDone
	m.mu.Unlock()
	defer close(startupDone)
	if !m.options.FlattenOnStartup {
		return nil
	}

	startedAt := m.options.Clock.Now()
	err := m.db.Flatten(max(1, m.dbOptions.NumCompactors))
	m.record(ctx, OperationFlatten, startedAt, err)
	if err != nil {
		m.options.Logger.ErrorContext(ctx, "badgerbox startup Flatten failed", "error", err)
	}
	return err
}

// Start begins periodic value-log GC. The first attempt occurs after one interval.
// Start may be called only once, including when periodic GC is disabled.
func (m *Service) Start(ctx context.Context) error {
	if m == nil || m.db == nil {
		return ErrNilDB
	}
	if err := ctxErr(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started || m.stopped {
		return maintenanceErrorf("Badger maintenance is already started or stopped")
	}
	if m.startupDone != nil {
		select {
		case <-m.startupDone:
		default:
			return maintenanceErrorf("Badger startup maintenance is still running")
		}
	}
	m.started = true
	if m.options.ValueLogGCInterval == 0 {
		return nil
	}

	workerCtx, cancel := context.WithCancel(ctx)
	ticker := m.options.Clock.NewTicker(m.options.ValueLogGCInterval)
	done := make(chan struct{})
	m.cancel = cancel
	m.done = done
	go m.runValueLogGC(workerCtx, ticker, done)
	return nil
}

// Stop cancels and joins periodic maintenance. Stop is safe to call more than once.
func (m *Service) Stop() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if !m.stopped {
		m.stopped = true
		if m.cancel != nil {
			m.cancel()
		}
	}
	done := m.done
	startupDone := m.startupDone
	m.mu.Unlock()
	if startupDone != nil {
		<-startupDone
	}
	if done != nil {
		<-done
	}
}

func (m *Service) runValueLogGC(ctx context.Context, ticker Ticker, done chan<- struct{}) {
	defer close(done)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			if ctx.Err() != nil {
				return
			}
			startedAt := m.options.Clock.Now()
			err := m.db.RunValueLogGC(m.options.ValueLogGCDiscardRatio)
			m.record(ctx, OperationValueLogGC, startedAt, err)
			if err != nil && !errors.Is(err, badger.ErrNoRewrite) {
				m.options.Logger.ErrorContext(ctx, "badgerbox value-log GC failed", "error", err)
			}
		}
	}
}

func (m *Service) record(
	ctx context.Context,
	operation Operation,
	startedAt time.Time,
	err error,
) {
	if m.options.Observer == nil {
		return
	}
	duration := max(time.Duration(0), m.options.Clock.Now().Sub(startedAt))
	m.options.Observer.ObserveMaintenance(ctx, Result{
		Operation: operation,
		Duration:  duration,
		Err:       err,
	})
}

// Ticker supplies periodic maintenance notifications.
type Ticker interface {
	Chan() <-chan time.Time
	Stop()
}

// Clock supplies time and tickers for deterministic maintenance scheduling.
type Clock interface {
	Now() time.Time
	NewTicker(time.Duration) Ticker
}
type systemClock struct{}

func (systemClock) Now() time.Time                   { return time.Now() }
func (systemClock) NewTicker(d time.Duration) Ticker { return systemTicker{time.NewTicker(d)} }

type systemTicker struct{ *time.Ticker }

func (t systemTicker) Chan() <-chan time.Time { return t.C }

var ErrNilDB = errors.New("badgerbox maintenance: database is nil")

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return errors.New("badgerbox maintenance: context is nil")
	}
	return ctx.Err()
}
func maintenanceErrorf(format string, args ...any) error {
	return fmt.Errorf("badgerbox maintenance: "+format, args...)
}
