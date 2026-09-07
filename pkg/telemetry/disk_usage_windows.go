//go:build windows

package telemetry

import (
	"fmt"
	"math"

	"golang.org/x/sys/windows"
)

func statfs(path string) (int64, int64, error) {
	directory, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, err
	}
	var availableBytes uint64
	var totalBytes uint64
	var totalFreeBytes uint64
	if err := windows.GetDiskFreeSpaceEx(directory, &availableBytes, &totalBytes, &totalFreeBytes); err != nil {
		return 0, 0, err
	}
	total, err := diskBytes(totalBytes)
	if err != nil {
		return 0, 0, err
	}
	available, err := diskBytes(availableBytes)
	if err != nil {
		return 0, 0, err
	}
	return total, available, nil
}

func diskBytes(value uint64) (int64, error) {
	if value > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("filesystem byte count exceeds int64")
	}
	return int64(value), nil //nolint:gosec // The bound above proves the value fits in int64.
}
