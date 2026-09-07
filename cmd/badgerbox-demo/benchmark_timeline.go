package main

import (
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Each point is an observation, not a claim that all sources were read atomically.
// Disk values are apparent file sizes and include Badger's preallocated files.
type benchmarkResourcePoint struct {
	Timestamp            time.Time              `json:"timestamp"`
	ElapsedSeconds       float64                `json:"elapsed_seconds"`
	Phase                string                 `json:"phase"`
	MeasurementsComplete bool                   `json:"measurements_complete"`
	RSS                  uint64                 `json:"rss_bytes"`
	Heap                 uint64                 `json:"heap_bytes"`
	Allocated            uint64                 `json:"total_allocated_bytes"`
	GCCycles             uint32                 `json:"go_gc_cycles"`
	CPUSeconds           *float64               `json:"cpu_seconds,omitempty"`
	Disk                 benchmarkDiskBreakdown `json:"apparent_disk"`
	Accepted             int64                  `json:"accepted"`
	UniqueDelivered      int64                  `json:"unique_delivered"`
	ValueLogGCRewrites   float64                `json:"value_log_gc_rewrites"`
	ValueLogGCNoRewrite  float64                `json:"value_log_gc_no_rewrite"`
	ValueLogGCErrors     float64                `json:"value_log_gc_errors"`
	CompactionWritten    float64                `json:"compaction_written_bytes"`
}

type benchmarkTimelineReport struct {
	RequestedIntervalSeconds float64                  `json:"requested_interval_seconds"`
	RetainedEvery            uint64                   `json:"retained_every_nth_observation"`
	ObservedPoints           uint64                   `json:"observed_points"`
	MaxPoints                int                      `json:"max_points"`
	Points                   []benchmarkResourcePoint `json:"points"`
}

// Compact evenly over the complete run instead of keeping only its tail. The
// first point and the final point survive, and storage never exceeds maxPoints.
type benchmarkTimeline struct {
	report benchmarkTimelineReport
}

func newBenchmarkTimeline(interval time.Duration, maxPoints int) *benchmarkTimeline {
	return &benchmarkTimeline{report: benchmarkTimelineReport{RequestedIntervalSeconds: interval.Seconds(), RetainedEvery: 1, MaxPoints: maxPoints, Points: make([]benchmarkResourcePoint, 0, maxPoints)}}
}

func (t *benchmarkTimeline) add(point benchmarkResourcePoint, final bool) {
	ordinal := t.report.ObservedPoints
	t.report.ObservedPoints++
	if !final && ordinal%t.report.RetainedEvery != 0 {
		return
	}
	if len(t.report.Points) == t.report.MaxPoints {
		kept := 0
		for i := 0; i < len(t.report.Points); i += 2 {
			t.report.Points[kept] = t.report.Points[i]
			kept++
		}
		t.report.Points = t.report.Points[:kept]
		t.report.RetainedEvery *= 2
		if !final && ordinal%t.report.RetainedEvery != 0 {
			return
		}
	}
	t.report.Points = append(t.report.Points, point)
}

type benchmarkMetricSeries struct {
	Name       string            `json:"name"`
	Scope      string            `json:"scope"`
	Unit       string            `json:"unit,omitempty"`
	Attributes map[string]string `json:"attributes"`
	Value      float64           `json:"value"`
}

// Preserve outcome and operation labels; flattening all counter attributes hides
// the distinction between attempted GC, no_rewrite, errors, and successful GC.
func benchmarkMetricSeriesValues(metrics metricdata.ResourceMetrics) []benchmarkMetricSeries {
	var result []benchmarkMetricSeries
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			add := func(attrs attribute.Set, value float64) {
				labels := make(map[string]string, attrs.Len())
				for _, kv := range attrs.ToSlice() {
					labels[string(kv.Key)] = kv.Value.Emit()
				}
				result = append(result, benchmarkMetricSeries{Name: metric.Name, Scope: scope.Scope.Name, Unit: metric.Unit, Attributes: labels, Value: value})
			}
			switch data := metric.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					add(point.Attributes, float64(point.Value))
				}
			case metricdata.Sum[float64]:
				for _, point := range data.DataPoints {
					add(point.Attributes, point.Value)
				}
			}
		}
	}
	slices.SortFunc(result, func(a, b benchmarkMetricSeries) int {
		if n := strings.Compare(a.Name, b.Name); n != 0 {
			return n
		}
		if n := strings.Compare(a.Scope, b.Scope); n != 0 {
			return n
		}
		return strings.Compare(benchmarkLabelKey(a.Attributes), benchmarkLabelKey(b.Attributes))
	})
	return result
}

func benchmarkLabelKey(labels map[string]string) string {
	values := make([]string, 0, len(labels))
	for key, value := range labels {
		values = append(values, key+"="+value)
	}
	slices.Sort(values)
	return strings.Join(values, ",")
}

func addBenchmarkMaintenance(point *benchmarkResourcePoint, series []benchmarkMetricSeries) {
	for _, metric := range series {
		switch metric.Name {
		case "badgerbox_badger_compaction_written_bytes":
			point.CompactionWritten += metric.Value
		case "badgerbox_badger_maintenance_outcomes":
			if metric.Attributes["operation"] != "value_log_gc" {
				continue
			}
			switch metric.Attributes["outcome"] {
			case "success":
				point.ValueLogGCRewrites += metric.Value
			case "no_rewrite":
				point.ValueLogGCNoRewrite += metric.Value
			case "error":
				point.ValueLogGCErrors += metric.Value
			}
		}
	}
}
