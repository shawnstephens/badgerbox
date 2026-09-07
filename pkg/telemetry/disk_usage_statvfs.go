//go:build netbsd || solaris

package telemetry

import "golang.org/x/sys/unix"

func statfs(path string) (int64, int64, error) {
	var stats unix.Statvfs_t
	if err := unix.Statvfs(path, &stats); err != nil {
		return 0, 0, err
	}
	return diskUsageFromBlocks(stats.Frsize, stats.Blocks, stats.Bavail)
}
