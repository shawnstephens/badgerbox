package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLatencyPanelsUseExpandedPercentilesAndMaxQueries(t *testing.T) {
	t.Parallel()

	path := filepath.Join("observability", "grafana", "dashboards", "badgerbox-demo-observability.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read dashboard JSON: %v", err)
	}

	var dashboard struct {
		Panels []struct {
			Title   string `json:"title"`
			Targets []struct {
				Expr         string `json:"expr"`
				LegendFormat string `json:"legendFormat"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(data, &dashboard); err != nil {
		t.Fatalf("unmarshal dashboard JSON: %v", err)
	}

	tests := []struct {
		title   string
		targets []struct {
			legend string
			expr   string
		}
	}{
		{
			title: "Enqueue Latency",
			targets: []struct {
				legend string
				expr   string
			}{
				{legend: "max", expr: `max(max_over_time(badgerbox_enqueue_duration_seconds_max{namespace="$namespace"}[5m]))`},
				{legend: "p99.9", expr: `histogram_quantile(0.999, sum by (le) (rate(badgerbox_enqueue_duration_seconds_bucket{namespace="$namespace"}[5m])))`},
				{legend: "p99", expr: `histogram_quantile(0.99, sum by (le) (rate(badgerbox_enqueue_duration_seconds_bucket{namespace="$namespace"}[5m])))`},
				{legend: "p95", expr: `histogram_quantile(0.95, sum by (le) (rate(badgerbox_enqueue_duration_seconds_bucket{namespace="$namespace"}[5m])))`},
				{legend: "p90", expr: `histogram_quantile(0.90, sum by (le) (rate(badgerbox_enqueue_duration_seconds_bucket{namespace="$namespace"}[5m])))`},
				{legend: "p75", expr: `histogram_quantile(0.75, sum by (le) (rate(badgerbox_enqueue_duration_seconds_bucket{namespace="$namespace"}[5m])))`},
				{legend: "p50", expr: `histogram_quantile(0.50, sum by (le) (rate(badgerbox_enqueue_duration_seconds_bucket{namespace="$namespace"}[5m])))`},
			},
		},
		{
			title: "Process Latency",
			targets: []struct {
				legend string
				expr   string
			}{
				{legend: "max", expr: `max(max_over_time(badgerbox_process_duration_seconds_max{namespace="$namespace"}[5m]))`},
				{legend: "p99.9", expr: `histogram_quantile(0.999, sum by (le) (rate(badgerbox_process_duration_seconds_bucket{namespace="$namespace"}[5m])))`},
				{legend: "p99", expr: `histogram_quantile(0.99, sum by (le) (rate(badgerbox_process_duration_seconds_bucket{namespace="$namespace"}[5m])))`},
				{legend: "p95", expr: `histogram_quantile(0.95, sum by (le) (rate(badgerbox_process_duration_seconds_bucket{namespace="$namespace"}[5m])))`},
				{legend: "p90", expr: `histogram_quantile(0.90, sum by (le) (rate(badgerbox_process_duration_seconds_bucket{namespace="$namespace"}[5m])))`},
				{legend: "p75", expr: `histogram_quantile(0.75, sum by (le) (rate(badgerbox_process_duration_seconds_bucket{namespace="$namespace"}[5m])))`},
				{legend: "p50", expr: `histogram_quantile(0.50, sum by (le) (rate(badgerbox_process_duration_seconds_bucket{namespace="$namespace"}[5m])))`},
			},
		},
	}

	for _, tt := range tests {
		panel := findDashboardPanel(t, dashboard.Panels, tt.title)
		if len(panel.Targets) != len(tt.targets) {
			t.Fatalf("%s target count = %d, want %d", tt.title, len(panel.Targets), len(tt.targets))
		}
		for i, target := range tt.targets {
			if panel.Targets[i].LegendFormat != target.legend {
				t.Fatalf("%s target %d legend = %q, want %q", tt.title, i, panel.Targets[i].LegendFormat, target.legend)
			}
			if panel.Targets[i].Expr != target.expr {
				t.Fatalf("%s target %d expr = %q, want %q", tt.title, i, panel.Targets[i].Expr, target.expr)
			}
		}
	}
}

func findDashboardPanel(t *testing.T, panels []struct {
	Title   string `json:"title"`
	Targets []struct {
		Expr         string `json:"expr"`
		LegendFormat string `json:"legendFormat"`
	} `json:"targets"`
}, title string) struct {
	Title   string `json:"title"`
	Targets []struct {
		Expr         string `json:"expr"`
		LegendFormat string `json:"legendFormat"`
	} `json:"targets"`
} {
	t.Helper()

	for _, panel := range panels {
		if panel.Title == title {
			return panel
		}
	}

	t.Fatalf("dashboard panel %q not found", title)
	return struct {
		Title   string `json:"title"`
		Targets []struct {
			Expr         string `json:"expr"`
			LegendFormat string `json:"legendFormat"`
		} `json:"targets"`
	}{}
}
