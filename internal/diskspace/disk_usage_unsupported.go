//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package diskspace

import (
	"fmt"
	"runtime"
)

func Read(string) (int64, int64, error) {
	return 0, 0, fmt.Errorf("filesystem capacity queries are unsupported on %s", runtime.GOOS)
}
