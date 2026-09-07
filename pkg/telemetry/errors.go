package telemetry

import (
	"errors"
	"fmt"
)

const defaultInstrumentationName = "github.com/shawnstephens/badgerbox"

var ErrNilDB = errors.New("badgerbox telemetry: database is nil")

func telemetryErrorf(format string, args ...any) error {
	return fmt.Errorf("badgerbox telemetry: "+format, args...)
}
