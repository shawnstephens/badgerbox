//go:build netbsd || solaris

package diskspace

import "golang.org/x/sys/unix"

func Read(path string) (int64, int64, error) {
	var stats unix.Statvfs_t
	if err := unix.Statvfs(path, &stats); err != nil {
		return 0, 0, err
	}
	return diskUsageFromBlocks(stats.Frsize, stats.Blocks, stats.Bavail)
}
