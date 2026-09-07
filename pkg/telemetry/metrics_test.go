package telemetry

import (
	"context"
	"expvar"
	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/maintenance"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"math"
	"testing"
)

func TestDatabaseMetricsResetSafeAndExclusive(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	var compaction expvar.Map
	compaction.Init()
	raw := new(expvar.Int)
	raw.Set(100)
	compaction.Set("0", raw)
	lookup := func(name string) expvar.Var {
		if name == badgerCompactionWrittenExpvar {
			return &compaction
		}
		if name == badgerActiveTablesExpvar {
			return new(expvar.Int)
		}
		m := new(expvar.Map).Init()
		v := new(expvar.Int)
		v.Set(123)
		m.Set("data", v)
		return m
	}
	create := func() (*BadgerMetrics, error) {
		return newBadgerMetricsWithSources(badger.DefaultOptions("data"), BadgerMetricsOptions{MeterProvider: provider}, lookup, func(string) (int64, int64, error) { return 1000, 500, nil })
	}
	collector, err := create()
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()
	if _, err = create(); err == nil {
		t.Fatal("duplicate collector admitted")
	}
	for i, want := range []int64{100, 150, 170} {
		raw.Set([]int64{100, 150, 20}[i])
		var data metricdata.ResourceMetrics
		if err = reader.Collect(t.Context(), &data); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, scope := range data.ScopeMetrics {
			if scope.Scope.Name != defaultInstrumentationName {
				t.Fatal(scope.Scope.Name)
			}
			for _, m := range scope.Metrics {
				if m.Name == badgerCompactionWrittenMetric {
					sum := m.Data.(metricdata.Sum[int64])
					if !sum.IsMonotonic || sum.DataPoints[0].Value != want {
						t.Fatalf("%+v", sum)
					}
					found = true
				}
			}
		}
		if !found {
			t.Fatal("missing compaction counter")
		}
	}
	if err = collector.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := create()
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
}
func TestMaintenanceMetricsClassifyNoRewrite(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	observer, err := NewBadgerMaintenanceMetrics(BadgerMaintenanceMetricsOptions{MeterProvider: provider})
	if err != nil {
		t.Fatal(err)
	}
	observer.ObserveMaintenance(t.Context(), maintenance.Result{Operation: maintenance.OperationValueLogGC, Err: badger.ErrNoRewrite})
	observer.ObserveMaintenance(t.Context(), maintenance.Result{Operation: maintenance.OperationValueLogGC})
	var data metricdata.ResourceMetrics
	if err = reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == badgerMaintenanceRewritesMetric && m.Data.(metricdata.Sum[int64]).DataPoints[0].Value != 1 {
				t.Fatal("no-rewrite counted as rewrite")
			}
		}
	}
}
func TestDiskConversionBounds(t *testing.T) {
	total, available, err := diskUsageFromBlocks(4096, 10, -1)
	if err != nil || total != 40960 || available != 0 {
		t.Fatalf("%d %d %v", total, available, err)
	}
	if _, _, err = diskUsageFromBlocks(uint64(4096), uint64(math.MaxUint64), uint64(0)); err == nil {
		t.Fatal("overflow accepted")
	}
	if _, _, err = statfs(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

func TestSharedDeliveryFlushMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	observer, err := NewDeliveryObserver(Options{MeterProvider: provider}, "test")
	if err != nil {
		t.Fatal(err)
	}
	observer.ObserveFlush(t.Context(), 0, context.DeadlineExceeded)
	var data metricdata.ResourceMetrics
	if err = reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "badgerbox_delivery_flush_error_total" {
				if m.Data.(metricdata.Sum[int64]).DataPoints[0].Value != 1 {
					t.Fatal(m)
				}
				found = true
			}
		}
	}
	if !found {
		t.Fatal("missing flush failure metric")
	}
}
