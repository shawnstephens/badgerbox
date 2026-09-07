package telemetry

import (
	"fmt"
	"math"
)

type diskInteger interface {
	~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64
}

func diskUsageFromBlocks[Size, Total, Available diskInteger](
	blockSize Size,
	totalBlocks Total,
	availableBlocks Available,
) (int64, int64, error) {
	if blockSize <= 0 {
		return 0, 0, fmt.Errorf("filesystem statistics returned invalid block size %d", blockSize)
	}
	if totalBlocks < 0 {
		return 0, 0, fmt.Errorf("filesystem statistics returned negative total blocks %d", totalBlocks)
	}
	// BSD filesystems may report a negative available-block count when reserved
	// space exceeds free space. Capacity gauges report zero rather than wrapping
	// that signed value into a very large unsigned byte count.
	if availableBlocks < 0 {
		availableBlocks = 0
	}
	size := uint64(blockSize)
	total, err := diskBlocksToBytes(uint64(totalBlocks), size)
	if err != nil {
		return 0, 0, err
	}
	available, err := diskBlocksToBytes(uint64(availableBlocks), size)
	if err != nil {
		return 0, 0, err
	}
	return total, available, nil
}

func diskBlocksToBytes(blocks, blockSize uint64) (int64, error) {
	if blockSize == 0 {
		return 0, fmt.Errorf("filesystem statistics returned an invalid zero block size")
	}
	if blocks > uint64(math.MaxInt64)/blockSize {
		return 0, fmt.Errorf("filesystem byte count exceeds int64")
	}
	return int64(blocks * blockSize), nil //nolint:gosec // The bound above proves the product fits in int64.
}
