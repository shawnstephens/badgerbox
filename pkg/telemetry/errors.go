package telemetry

import (
	"errors"
	"fmt"
)

const defaultInstrumentationName = "github.com/shawnstephens/badgerbox"

var ErrNilDB = errors.New("badgerbox telemetry: database is nil")

// ErrBadgerMetricsDisabled identifies databases opened without the expvar sources
// required by NewBadgerMetrics. Enable badger.Options.MetricsEnabled before Open.
var ErrBadgerMetricsDisabled = errors.New("badgerbox telemetry: database metrics require Badger MetricsEnabled=true")

// ErrBadgerMetricsInMemory identifies databases without the disk and directory
// sources required by NewBadgerMetrics. Queue telemetry remains usable in memory.
var ErrBadgerMetricsInMemory = errors.New("badgerbox telemetry: database metrics require a disk-backed Badger database; InMemory must be false")

func telemetryErrorf(format string, args ...any) error {
	return fmt.Errorf("badgerbox telemetry: "+format, args...)
}
