package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBenchmarkTimelineBoundsRetainWholeRun(t *testing.T) {
	for _, capacity := range []int{4, 5, 16} {
		timeline := newBenchmarkTimeline(time.Second, capacity)
		for i := range 10001 {
			timeline.add(benchmarkResourcePoint{ElapsedSeconds: float64(i)}, i == 10000)
			if len(timeline.report.Points) > capacity {
				t.Fatal("timeline exceeded its memory bound")
			}
		}
		points := timeline.report.Points
		if points[0].ElapsedSeconds != 0 || points[len(points)-1].ElapsedSeconds != 10000 || timeline.report.ObservedPoints != 10001 || timeline.report.RetainedEvery <= 1 {
			t.Fatalf("timeline omitted boundaries or downsampling information: %+v", timeline.report)
		}
		for i := 1; i < len(points); i++ {
			if points[i].ElapsedSeconds <= points[i-1].ElapsedSeconds {
				t.Fatal("timeline reordered observations")
			}
		}
	}
}

func TestBenchmarkObservationRetainsMaintenanceOutcomes(t *testing.T) {
	report, err := runBenchmarkTest(t, "--messages", "20", "--rate", "200", "--observe-after-drain", "400ms", "--badger-gc-interval", "50ms", "--timeline-interval", "50ms", "--timeline-max-points", "8")
	if err != nil {
		t.Fatal(err)
	}
	if report.SchemaVersion != 2 || !report.Passed || report.Resources.ElapsedSeconds < report.DeliverySeconds+.35 {
		t.Fatalf("maintenance observation polluted throughput or was skipped: delivery=%g resources=%g", report.DeliverySeconds, report.Resources.ElapsedSeconds)
	}
	points := report.Timeline.Points
	if len(points) < 3 || len(points) > 8 || points[len(points)-1].Phase != "stopped" {
		t.Fatalf("missing bounded timeline: %+v", report.Timeline)
	}
	observed := false
	for _, point := range points {
		if point.Timestamp.IsZero() || point.Disk.Total != point.Disk.Vlog+point.Disk.LSM+point.Disk.WAL+point.Disk.Other {
			t.Fatalf("missing timestamp or disk conservation: %+v", point)
		}
		if point.Phase == "observe_after_drain" && point.Accepted == 20 && point.UniqueDelivered == 20 {
			observed = true
		}
	}
	if !observed {
		t.Fatal("maintenance was not observed after delivery drained")
	}
	var outcomes float64
	for _, series := range report.MetricSeries {
		if series.Name == "badgerbox_badger_maintenance_outcomes" && series.Attributes["operation"] == "value_log_gc" {
			if series.Attributes["outcome"] == "" {
				t.Fatal("maintenance outcome label omitted")
			}
			outcomes += series.Value
		}
	}
	last := points[len(points)-1]
	if outcomes < 2 || outcomes != last.ValueLogGCRewrites+last.ValueLogGCNoRewrite+last.ValueLogGCErrors {
		t.Fatalf("maintenance outcome conservation: outcomes=%g last=%+v", outcomes, last)
	}
}

func TestBenchmarkDiskBreakdown(t *testing.T) {
	dir := t.TempDir()
	for filename, size := range map[string]int{"0001.vlog": 12, "0002.sst": 34, "0003.mem": 56, "MANIFEST": 7} {
		if err := os.WriteFile(filepath.Join(dir, filename), make([]byte, size), 0600); err != nil {
			t.Fatal(err)
		}
	}
	disk, err := benchmarkDiskUsage(dir)
	if err != nil || disk.Vlog != 12 || disk.LSM != 34 || disk.WAL != 56 || disk.Other != 7 || disk.Total != 109 {
		t.Fatalf("disk=%+v error=%v", disk, err)
	}
}
