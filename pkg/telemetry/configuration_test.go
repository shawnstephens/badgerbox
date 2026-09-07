package telemetry

import (
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestBadgerCollectorRejectsUnavailableSources(t *testing.T) {
	provider := sdkmetric.NewMeterProvider()
	defer provider.Shutdown(t.Context())
	for _, tc := range []struct {
		name string
		opts badger.Options
		want error
	}{
		{"metrics disabled", badger.DefaultOptions(t.TempDir()).WithMetricsEnabled(false), ErrBadgerMetricsDisabled},
		{"in-memory database", badger.DefaultOptions("").WithInMemory(true), ErrBadgerMetricsInMemory},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := badger.Open(tc.opts.WithLogger(nil))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			collector, err := NewBadgerMetrics(db, BadgerMetricsOptions{MeterProvider: provider})
			if collector != nil {
				collector.Close()
			}
			if collector != nil || !errors.Is(err, tc.want) {
				t.Fatalf("collector nil=%t, error=%v, want %v", collector == nil, err, tc.want)
			}
		})
	}
	// Invalid collectors must not reserve the process-wide collection slot.
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	collector, err := NewBadgerMetrics(db, BadgerMetricsOptions{MeterProvider: provider})
	if err != nil {
		t.Fatalf("unavailable sources prevented valid collector construction: %v", err)
	}
	defer collector.Close()
}
