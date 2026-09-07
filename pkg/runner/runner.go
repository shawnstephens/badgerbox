// Package runner owns a shared Badger database, typed queues, and their lifecycle.
package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/maintenance"
	"github.com/shawnstephens/badgerbox/pkg/telemetry"
)

// Options uses caller-selected Badger settings without environment or path defaults.
type Options struct {
	Badger             badger.Options
	Maintenance        maintenance.Options
	Telemetry          telemetry.Options
	QueueFailurePolicy QueueFailurePolicy
}

// QueueOptions configures a typed queue. Store.Namespace is required and unique.
// Empty telemetry providers inherit the runner providers independently.
type QueueOptions struct {
	Store     badgerbox.Options
	Processor badgerbox.BatchProcessorOptions
}

// DeliveryHooks owns a named delivery dependency shared by any number of queues.
// Flush must honor its context. Close must join outstanding asynchronous callbacks.
// Hooks run once after workers and maintenance have joined, in registration order.
type DeliveryHooks struct {
	Flush func(context.Context) error
	Close func() error
}
type queue struct {
	namespace string
	run       func(context.Context) error
	close     func() error
}
type delivery struct {
	name  string
	hooks DeliveryHooks
}

// Runner must be shut down after intake has stopped. Stores returned by Register
// are owned by Runner; callers must not close them independently.
type Runner struct {
	mu                sync.Mutex
	db                *badger.DB
	maintenance       *maintenance.Service
	metrics           *telemetry.BadgerMetrics
	options           Options
	queues            []queue
	deliveries        []delivery
	started, stopping bool
	cancel            context.CancelFunc
	workers           sync.WaitGroup
	runErr            error
	errors            chan error
	stopOnce          sync.Once
	stopped           chan struct{}
	closeOnce         sync.Once
	closed            chan struct{}
	closeErr          error
}

// Open validates maintenance settings before opening the database and runs optional
// startup maintenance. It does not start queue workers or periodic maintenance.
func Open(ctx context.Context, options Options) (*Runner, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if options.QueueFailurePolicy != IsolateQueue && options.QueueFailurePolicy != FailFast {
		return nil, fmt.Errorf("badgerbox runner: invalid queue failure policy %d", options.QueueFailurePolicy)
	}
	if options.Telemetry.DurationMaxWindow < 0 {
		return nil, errors.New("badgerbox runner: duration maximum window must be nonnegative")
	}
	if err := maintenance.ValidateOptions(options.Badger, options.Maintenance); err != nil {
		return nil, err
	}
	db, err := badger.Open(options.Badger)
	if err != nil {
		return nil, err
	}
	r := &Runner{db: db, options: options, errors: make(chan error, 1), stopped: make(chan struct{}), closed: make(chan struct{})}
	success := false
	defer func() {
		if !success {
			if r.maintenance != nil {
				r.maintenance.Stop()
			}
			if r.metrics != nil {
				_ = r.metrics.Close()
			}
			_ = db.Close()
		}
	}()
	maintenanceOpts := options.Maintenance
	if options.Telemetry.MeterProvider != nil {
		observer, err := telemetry.NewBadgerMaintenanceMetrics(telemetry.BadgerMaintenanceMetricsOptions{MeterProvider: options.Telemetry.MeterProvider, MeterName: options.Telemetry.MeterName})
		if err != nil {
			return nil, err
		}
		maintenanceOpts.Observer = observers{maintenanceOpts.Observer, observer}
		r.metrics, err = telemetry.NewBadgerMetrics(db, telemetry.BadgerMetricsOptions{MeterProvider: options.Telemetry.MeterProvider, MeterName: options.Telemetry.MeterName})
		if err != nil {
			return nil, err
		}
	}
	r.maintenance, err = maintenance.New(db, maintenanceOpts)
	if err != nil {
		return nil, err
	}
	if err = r.maintenance.RunStartup(ctx); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	success = true
	return r, nil
}

type observers []maintenance.Observer

func (o observers) ObserveMaintenance(ctx context.Context, result maintenance.Result) {
	for _, observer := range o {
		if observer != nil {
			observer.ObserveMaintenance(ctx, result)
		}
	}
}

// Register creates a queue using the supplied codecs and batch delivery function.
// Missing codecs default independently to JSON in the core store.
func Register[M, D any](r *Runner, serde badgerbox.Serde[M, D], options QueueOptions, fn badgerbox.BatchProcessFunc[M, D]) (*badgerbox.Store[M, D], error) {
	if r == nil {
		return nil, errors.New("badgerbox runner: runner is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopping {
		return nil, errors.New("badgerbox runner: registration is closed")
	}
	namespace := options.Store.Namespace
	if namespace == "" {
		return nil, errors.New("badgerbox runner: namespace is required")
	}
	for _, q := range r.queues {
		if q.namespace == namespace {
			return nil, fmt.Errorf("badgerbox runner: namespace %q already registered", namespace)
		}
	}
	if fn == nil {
		return nil, badgerbox.ErrProcessorFuncNil
	}
	obs := &options.Store.Observability
	defaults := r.options.Telemetry
	if obs.MeterProvider == nil {
		obs.MeterProvider = defaults.MeterProvider
	}
	if obs.TracerProvider == nil {
		obs.TracerProvider = defaults.TracerProvider
	}
	if obs.Propagator == nil {
		obs.Propagator = defaults.Propagator
	}
	if obs.MeterName == "" {
		obs.MeterName = defaults.MeterName
	}
	if obs.TracerName == "" {
		obs.TracerName = defaults.TracerName
	}
	if obs.PollInterval <= 0 {
		obs.PollInterval = defaults.PollInterval
	}
	if obs.DurationMaxWindow == 0 {
		obs.DurationMaxWindow = defaults.DurationMaxWindow
	}
	store, err := badgerbox.New(r.db, serde, options.Store)
	if err != nil {
		return nil, err
	}
	processor, err := badgerbox.NewBatchProcessor(store, fn, options.Processor)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	r.queues = append(r.queues, queue{namespace, processor.Run, store.Close})
	return store, nil
}

// RegisterDelivery transfers lifecycle ownership after successful registration.
func (r *Runner) RegisterDelivery(name string, hooks DeliveryHooks) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopping {
		return errors.New("badgerbox runner: registration is closed")
	}
	if name == "" {
		return errors.New("badgerbox runner: delivery name is required")
	}
	for _, d := range r.deliveries {
		if d.name == name {
			return fmt.Errorf("badgerbox runner: delivery %q already registered", name)
		}
	}
	r.deliveries = append(r.deliveries, delivery{name, hooks})
	return nil
}

// Start starts all registered queues. Caller cancellation may stop intake without
// ending workers; Stop explicitly cancels and joins processing and maintenance.
// IsolateQueue leaves healthy queues and maintenance running after a worker error.
// FailFast cancels all queues and maintenance. Failed queues are not restarted.
func (r *Runner) Start(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopping {
		return errors.New("badgerbox runner: already started or stopped")
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	if err := r.maintenance.Start(runCtx); err != nil {
		cancel()
		return err
	}
	r.started = true
	r.cancel = cancel
	for _, q := range r.queues {
		r.workers.Go(func() {
			if err := q.run(runCtx); err != nil {
				// Run can return bare cancellation before startup finishes. After
				// startup it joins failures, including detached settlement errors;
				// keep those even when their cause is cancellation.
				if err == context.Canceled && runCtx.Err() != nil {
					return
				}
				queueErr := &QueueError{Namespace: q.namespace, Err: err}
				r.mu.Lock()
				r.runErr = errors.Join(r.runErr, queueErr)
				r.mu.Unlock()
				select {
				case r.errors <- queueErr:
				default:
				}
				if r.options.QueueFailurePolicy == FailFast {
					cancel()
				}
			}
		})
	}
	return nil
}

// Errors reports *QueueError notifications without blocking workers. Notifications
// may be dropped when the buffer is full; Stop and Shutdown retain every failure.
// A notification does not imply that healthy queues stopped. Shutdown closes the channel.
func (r *Runner) Errors() <-chan error { return r.errors }

// Stop cancels workers and maintenance and waits within ctx. A timeout leaves
// dependencies open; call Stop or Shutdown again after work has finished.
func (r *Runner) Stop(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	r.stopOnce.Do(func() {
		r.mu.Lock()
		r.stopping = true
		if r.cancel != nil {
			r.cancel()
		}
		r.mu.Unlock()
		go func() { r.maintenance.Stop(); r.workers.Wait(); close(r.stopped) }()
	})
	select {
	case <-r.stopped:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.runErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown joins workers, flushes and closes delivery dependencies, unregisters
// telemetry, and closes the stores and database, in that order. If joining times
// out no dependency is closed. Once joining succeeds, noncancelable Close calls
// finish in one background closer even if the caller's deadline expires.
func (r *Runner) Shutdown(ctx context.Context) error {
	stopErr := r.Stop(ctx)
	select {
	case <-r.stopped:
	default:
		return stopErr
	}
	if err := contextError(ctx); err != nil {
		return errors.Join(stopErr, err)
	}
	r.closeOnce.Do(func() {
		go func() {
			defer close(r.closed)
			err := stopErr
			for _, d := range r.deliveries {
				if d.hooks.Flush != nil {
					err = errors.Join(err, d.hooks.Flush(ctx))
				}
				if d.hooks.Close != nil {
					err = errors.Join(err, d.hooks.Close())
				}
			}
			for _, q := range r.queues {
				err = errors.Join(err, q.close())
			}
			if r.metrics != nil {
				err = errors.Join(err, r.metrics.Close())
			}
			err = errors.Join(err, r.db.Close())
			r.closeErr = err
			close(r.errors)
		}()
	})
	select {
	case <-r.closed:
		return r.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
func contextError(ctx context.Context) error {
	if ctx == nil {
		return badgerbox.ErrNilContext
	}
	return ctx.Err()
}
