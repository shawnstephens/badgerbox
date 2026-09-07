//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package telemetry

import (
	"fmt"
	"runtime"
)

func statfs(string) (int64, int64, error) {
	return 0, 0, fmt.Errorf("filesystem capacity metrics are unsupported on %s", runtime.GOOS)
}
