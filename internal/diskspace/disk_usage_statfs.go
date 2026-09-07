//go:build aix || darwin || dragonfly || freebsd || linux

package diskspace

import "golang.org/x/sys/unix"

func Read(path string) (int64, int64, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(path, &stats); err != nil {
		return 0, 0, err
	}
	return diskUsageFromBlocks(stats.Bsize, stats.Blocks, stats.Bavail)
}
