package telemetry

import (
	"context"
	"expvar"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// Badger database-wide metric instrument names. Directory and disk gauges are
	// sampled during OpenTelemetry collection; compaction-written is a reset-safe
	// monotonic counter.
	badgerLSMSizeMetric           = "badgerbox_badger_lsm_size_bytes"
	badgerVlogSizeMetric          = "badgerbox_badger_vlog_size_bytes"
	badgerTotalSizeMetric         = "badgerbox_badger_total_size_bytes"
	badgerDiskTotalMetric         = "badgerbox_badger_disk_total_bytes"
	badgerDiskAvailableMetric     = "badgerbox_badger_disk_available_bytes"
	badgerPendingWritesMetric     = "badgerbox_badger_memtable_pending_writes"
	badgerActiveTablesMetric      = "badgerbox_badger_compaction_active_tables"
	badgerCompactionWrittenMetric = "badgerbox_badger_compaction_written_bytes"
	badgerCollectionErrorsMetric  = "badgerbox_badger_collection_errors"
)

const (
	badgerLSMSizeExpvar           = "badger_size_bytes_lsm"
	badgerVlogSizeExpvar          = "badger_size_bytes_vlog"
	badgerPendingWritesExpvar     = "badger_write_pending_num_memtable"
	badgerActiveTablesExpvar      = "badger_compaction_current_num_lsm"
	badgerCompactionWrittenExpvar = "badger_write_bytes_compaction"
)

type expvarLookup func(string) expvar.Var
type diskUsage func(string) (int64, int64, error)

// BadgerMetricsOptions configures database-wide Badger OpenTelemetry metrics.
type BadgerMetricsOptions struct {
	// MeterProvider registers the Badger instruments and is required.
	MeterProvider metric.MeterProvider
	// MeterName overrides the module's default OpenTelemetry instrumentation scope.
	MeterName string
	// DiskPath selects the filesystem reported by disk-capacity gauges. The
	// Badger LSM directory is used when DiskPath is empty.
	DiskPath string
}

// BadgerMetrics owns database-wide Badger metric instruments and their
// asynchronous callback registration.
type BadgerMetrics struct {
	lookup    expvarLookup
	diskUsage diskUsage
	lsmPath   string
	vlogPath  string
	diskPath  string

	gauges            map[string]metric.Int64ObservableGauge
	compactionWritten metric.Int64ObservableCounter
	collectionErrors  metric.Int64Counter
	registration      metric.Registration
	compactionTotals  monotonicCounters
	compactionMu      sync.Mutex
	releaseCollector  func()
	closeOnce         sync.Once
	closeErr          error
}

// badgerMetricsCollectorRegistry prevents duplicate collection of Badger's
// process-global compaction statistics. It is process-scoped, not database-scoped.
type badgerMetricsCollectorRegistry struct {
	mu     sync.Mutex
	active bool
}

var processBadgerMetricsCollectors badgerMetricsCollectorRegistry

// acquire reserves the process-wide collector slot and returns an idempotent
// release function for constructor failures and Close.
func (r *badgerMetricsCollectorRegistry) acquire() (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active {
		return nil, telemetryErrorf("Badger metrics collector is already active")
	}
	r.active = true
	var releaseOnce sync.Once
	return func() {
		releaseOnce.Do(func() {
			r.mu.Lock()
			r.active = false
			r.mu.Unlock()
		})
	}, nil
}

type counterState struct {
	raw   int64
	total int64
}

type monotonicCounters struct {
	mu     sync.Mutex
	levels map[string]counterState
}

// observe converts Badger's resettable raw per-level counter into a monotonic
// total suitable for OpenTelemetry counter export.
func (c *monotonicCounters) observe(level string, raw int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.levels == nil {
		c.levels = make(map[string]counterState)
	}
	state, exists := c.levels[level]
	if !exists {
		state.total = raw
	} else if raw >= state.raw {
		state.total += raw - state.raw
	} else {
		state.total += raw
	}
	state.raw = raw
	c.levels[level] = state
	return state.total
}

// NewBadgerMetrics reads Badger's in-process expvar registry without exposing
// /debug/vars or starting another HTTP server. Badger refreshes its
// directory-keyed LSM and value-log sizes once per minute; active-compaction
// tables and compaction-written bytes are process-global, with written bytes
// exported by level. Only one BadgerMetrics collector may be active in a
// process because Badger's compaction values are process-global. Close the
// collector before creating another one or closing the database.
// The database must be disk-backed and opened with MetricsEnabled=true;
// unsupported modes return ErrBadgerMetricsInMemory or ErrBadgerMetricsDisabled
// instead of exporting missing or stale process-global expvar values.
func NewBadgerMetrics(db *badger.DB, opts BadgerMetricsOptions) (*BadgerMetrics, error) {
	if db == nil {
		return nil, ErrNilDB
	}
	return newBadgerMetricsWithSources(db.Opts(), opts, expvar.Get, statfs)
}

// newBadgerMetricsWithSources constructs the collector with injectable sources
// for tests and releases its reservation if construction fails.
func newBadgerMetricsWithSources(
	badgerOpts badger.Options,
	opts BadgerMetricsOptions,
	lookup expvarLookup,
	diskUsage diskUsage,
) (*BadgerMetrics, error) {
	if opts.MeterProvider == nil {
		return nil, telemetryErrorf("Badger metrics meter provider is nil")
	}
	if badgerOpts.InMemory {
		return nil, ErrBadgerMetricsInMemory
	}
	if !badgerOpts.MetricsEnabled {
		return nil, ErrBadgerMetricsDisabled
	}
	releaseCollector, err := processBadgerMetricsCollectors.acquire()
	if err != nil {
		return nil, err
	}
	constructed := false
	defer func() {
		if !constructed {
			releaseCollector()
		}
	}()
	meterName := strings.TrimSpace(opts.MeterName)
	if meterName == "" {
		meterName = defaultInstrumentationName
	}
	telemetry := &BadgerMetrics{
		lookup:           lookup,
		diskUsage:        diskUsage,
		lsmPath:          badgerOpts.Dir,
		vlogPath:         badgerOpts.ValueDir,
		diskPath:         opts.DiskPath,
		gauges:           make(map[string]metric.Int64ObservableGauge),
		releaseCollector: releaseCollector,
	}
	if telemetry.diskPath == "" {
		telemetry.diskPath = telemetry.lsmPath
	}
	meter := opts.MeterProvider.Meter(meterName)
	observableNames := []string{
		badgerLSMSizeMetric, badgerVlogSizeMetric,
		badgerTotalSizeMetric, badgerDiskTotalMetric,
		badgerDiskAvailableMetric, badgerPendingWritesMetric,
		badgerActiveTablesMetric,
	}
	observables := make([]metric.Observable, 0, len(observableNames)+1)
	for _, name := range observableNames {
		gauge, gaugeErr := meter.Int64ObservableGauge(name)
		if gaugeErr != nil {
			return nil, telemetryErrorf("register Badger observable gauge %q: %w", name, gaugeErr)
		}
		telemetry.gauges[name] = gauge
		observables = append(observables, gauge)
	}
	if telemetry.compactionWritten, err = meter.Int64ObservableCounter(badgerCompactionWrittenMetric); err != nil {
		return nil, telemetryErrorf("register Badger compaction counter: %w", err)
	}
	observables = append(observables, telemetry.compactionWritten)
	if telemetry.collectionErrors, err = meter.Int64Counter(badgerCollectionErrorsMetric); err != nil {
		return nil, telemetryErrorf("register Badger collection error counter: %w", err)
	}
	telemetry.registration, err = meter.RegisterCallback(telemetry.observe, observables...)
	if err != nil {
		return nil, telemetryErrorf("register Badger metrics callback: %w", err)
	}
	constructed = true
	return telemetry, nil
}

func (t *BadgerMetrics) observe(ctx context.Context, observer metric.Observer) error {
	lsm, lsmOK := t.observeDirectoryValue(ctx, observer, badgerLSMSizeExpvar, t.lsmPath, badgerLSMSizeMetric)
	vlog, vlogOK := t.observeDirectoryValue(ctx, observer, badgerVlogSizeExpvar, t.vlogPath, badgerVlogSizeMetric)
	if lsmOK && vlogOK {
		observer.ObserveInt64(t.gauges[badgerTotalSizeMetric], lsm+vlog)
	}
	total, available, err := t.diskUsage(t.diskPath)
	if err != nil {
		t.recordCollectionError(ctx, "disk")
	} else {
		observer.ObserveInt64(t.gauges[badgerDiskTotalMetric], total)
		observer.ObserveInt64(t.gauges[badgerDiskAvailableMetric], available)
	}
	t.observeDirectoryValue(ctx, observer, badgerPendingWritesExpvar, t.lsmPath, badgerPendingWritesMetric)
	if activeTables, readErr := readExpvarInt(t.lookup(badgerActiveTablesExpvar)); readErr != nil {
		t.recordCollectionError(ctx, badgerActiveTablesExpvar)
	} else {
		observer.ObserveInt64(t.gauges[badgerActiveTablesMetric], activeTables)
	}
	t.observeCompactionWritten(ctx, observer)
	return nil
}

func (t *BadgerMetrics) observeDirectoryValue(ctx context.Context, observer metric.Observer, expvarName, path, metricName string) (int64, bool) {
	root, ok := t.lookup(expvarName).(*expvar.Map)
	if !ok || root == nil {
		t.recordCollectionError(ctx, expvarName)
		return 0, false
	}
	value, err := readExpvarInt(root.Get(path))
	if err != nil {
		t.recordCollectionError(ctx, expvarName)
		return 0, false
	}
	observer.ObserveInt64(t.gauges[metricName], value)
	return value, true
}

func (t *BadgerMetrics) observeCompactionWritten(ctx context.Context, observer metric.Observer) {
	// Serialize the source read with the cumulative-state update. Concurrent OTel
	// collections must not apply an older process-global expvar sample after a
	// newer sample and misclassify it as a Badger counter reset.
	t.compactionMu.Lock()
	defer t.compactionMu.Unlock()
	root, ok := t.lookup(badgerCompactionWrittenExpvar).(*expvar.Map)
	if !ok || root == nil {
		t.recordCollectionError(ctx, badgerCompactionWrittenExpvar)
		return
	}
	root.Do(func(kv expvar.KeyValue) {
		raw, err := readExpvarInt(kv.Value)
		if err != nil || raw < 0 {
			t.recordCollectionError(ctx, badgerCompactionWrittenExpvar)
			return
		}
		value := t.compactionTotals.observe(kv.Key, raw)
		observer.ObserveInt64(t.compactionWritten, value, metric.WithAttributes(attribute.String("level", kv.Key)))
	})
}

func (t *BadgerMetrics) recordCollectionError(ctx context.Context, source string) {
	t.collectionErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("source", source)))
}

// Close unregisters Badger metric callbacks and releases the process-wide
// collector reservation. Close is safe to call more than once.
func (t *BadgerMetrics) Close() error {
	if t == nil {
		return nil
	}
	t.closeOnce.Do(func() {
		if t.registration != nil {
			t.closeErr = t.registration.Unregister()
		}
		if t.releaseCollector != nil {
			t.releaseCollector()
		}
	})
	return t.closeErr
}

func readExpvarInt(value expvar.Var) (int64, error) {
	if value == nil {
		return 0, fmt.Errorf("expvar value is unavailable")
	}
	if intValue, ok := value.(*expvar.Int); ok {
		return intValue.Value(), nil
	}
	parsed, err := strconv.ParseInt(value.String(), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse expvar integer: %w", err)
	}
	return parsed, nil
}
