package diskspace

import (
	"math"
	"testing"
)

func TestDiskConversionBounds(t *testing.T) {
	total, available, err := diskUsageFromBlocks(4096, 10, -1)
	if err != nil || total != 40960 || available != 0 {
		t.Fatalf("%d %d %v", total, available, err)
	}
	if _, _, err = diskUsageFromBlocks(uint64(4096), uint64(math.MaxUint64), uint64(0)); err == nil {
		t.Fatal("overflow accepted")
	}
	if _, _, err = Read(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}
