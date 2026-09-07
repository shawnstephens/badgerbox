//go:build openbsd

package diskspace

import "golang.org/x/sys/unix"

func Read(path string) (int64, int64, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(path, &stats); err != nil {
		return 0, 0, err
	}
	return diskUsageFromBlocks(stats.F_bsize, stats.F_blocks, stats.F_bavail)
}
