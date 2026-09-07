//go:build aix || darwin || dragonfly || freebsd || linux

package telemetry

import "golang.org/x/sys/unix"

func statfs(path string) (int64, int64, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(path, &stats); err != nil {
		return 0, 0, err
	}
	return diskUsageFromBlocks(stats.Bsize, stats.Blocks, stats.Bavail)
}
