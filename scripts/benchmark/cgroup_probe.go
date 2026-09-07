//go:build ignore

// Build this standalone Linux supervisor with CGO_ENABLED=0 GOOS=linux go build
// cgroup_probe.go. It records cgroup v2 evidence before the container exits.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type sample struct {
	Timestamp     time.Time         `json:"timestamp"`
	Elapsed       float64           `json:"elapsed_seconds"`
	MemoryCurrent uint64            `json:"memory_current_bytes"`
	MemoryPeak    uint64            `json:"memory_peak_bytes"`
	SwapCurrent   uint64            `json:"swap_current_bytes"`
	Events        map[string]uint64 `json:"memory_events"`
	CPU           map[string]uint64 `json:"cpu_stat"`
	Memory        map[string]uint64 `json:"memory_stat"`
}

type report struct {
	Schema            int               `json:"schema_version"`
	Started           time.Time         `json:"started_at"`
	Command           []string          `json:"command"`
	Limits            map[string]string `json:"kernel_limits"`
	Interval          string            `json:"sample_interval"`
	MaxPoints         int               `json:"max_points"`
	Observed          int               `json:"observed_points"`
	Every             int               `json:"retained_every_nth_observation"`
	Points            []sample          `json:"points"`
	ExitCode          int               `json:"child_exit_code"`
	Error             string            `json:"child_error,omitempty"`
	MeasurementErrors []string          `json:"measurement_errors,omitempty"`
	Notes             []string          `json:"notes"`
}

func main() {
	output := flag.String("output", "/output/cgroup.json", "report path")
	interval := time.Second
	timeout := flag.Duration("timeout", 10*time.Minute, "hard child time limit")
	flag.Parse()
	if len(flag.Args()) == 0 || *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "command and positive timeout required")
		os.Exit(2)
	}
	started := time.Now()
	r := report{Schema: 1, Started: started.UTC(), Command: flag.Args(), Limits: map[string]string{}, Interval: interval.String(), MaxPoints: 1024, Every: 1, Notes: []string{
		"Kernel cgroup v2 memory includes benchmark, this supervisor, filesystem page cache and kernel accounting; process RSS is reported separately by the benchmark.",
		"CPU counters include supervisor overhead. memory.peak is the kernel peak, not a sampled maximum. Finite success does not establish a permanent bound or prove crash durability.",
	}}
	recordError := func(err error) {
		if err != nil && len(r.MeasurementErrors) < 8 {
			r.MeasurementErrors = append(r.MeasurementErrors, err.Error())
		}
	}
	read := func(name string) string {
		data, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", name))
		recordError(err)
		return strings.TrimSpace(string(data))
	}
	readUint := func(name string) uint64 {
		value, err := strconv.ParseUint(read(name), 10, 64)
		recordError(err)
		return value
	}
	readMap := func(name string) map[string]uint64 {
		result := map[string]uint64{}
		for _, line := range strings.Split(read(name), "\n") {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				recordError(fmt.Errorf("invalid %s entry: %q", name, line))
				continue
			}
			value, err := strconv.ParseUint(fields[1], 10, 64)
			recordError(err)
			result[fields[0]] = value
		}
		return result
	}
	for _, name := range []string{"cpu.max", "memory.max", "memory.swap.max"} {
		r.Limits[name] = read(name)
		if r.Limits[name] == "" {
			recordError(fmt.Errorf("missing %s limit", name))
		}
	}
	capture := func(final bool) {
		point := sample{Timestamp: time.Now().UTC(), Elapsed: time.Since(started).Seconds(), MemoryCurrent: readUint("memory.current"), MemoryPeak: readUint("memory.peak"), SwapCurrent: readUint("memory.swap.current"), Events: readMap("memory.events"), CPU: readMap("cpu.stat"), Memory: map[string]uint64{}}
		for _, key := range []string{"usage_usec", "nr_periods", "nr_throttled", "throttled_usec"} {
			if _, ok := point.CPU[key]; !ok {
				recordError(fmt.Errorf("missing cpu.stat %s", key))
			}
		}
		for _, key := range []string{"oom", "oom_kill", "high", "max"} {
			if _, ok := point.Events[key]; !ok {
				recordError(fmt.Errorf("missing memory.events %s", key))
			}
		}
		stats := readMap("memory.stat")
		for _, key := range []string{"anon", "file"} {
			if _, ok := stats[key]; !ok {
				recordError(fmt.Errorf("missing memory.stat %s", key))
			}
		}
		for _, key := range []string{"anon", "file", "kernel", "slab", "file_dirty", "file_writeback"} {
			if value, ok := stats[key]; ok {
				point.Memory[key] = value
			}
		}
		ordinal := r.Observed
		r.Observed++
		if !final && ordinal%r.Every != 0 {
			return
		}
		if len(r.Points) == r.MaxPoints {
			kept := 0
			for i := 0; i < len(r.Points); i += 2 {
				r.Points[kept] = r.Points[i]
				kept++
			}
			r.Points = r.Points[:kept]
			r.Every *= 2
			if !final && ordinal%r.Every != 0 {
				return
			}
		}
		r.Points = append(r.Points, point)
	}
	capture(false)
	command := exec.Command(flag.Args()[0], flag.Args()[1:]...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if len(r.MeasurementErrors) > 0 {
		r.Error = "cgroup baseline unavailable"
		r.ExitCode = 1
	} else if err := command.Start(); err != nil {
		r.Error = err.Error()
		r.ExitCode = 1
	} else {
		signals := make(chan os.Signal, 2)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		ticker := time.NewTicker(interval)
		deadline := time.NewTimer(*timeout)
	running:
		for {
			select {
			case err := <-done:
				if err != nil {
					r.Error = err.Error()
					r.ExitCode = 1
					var exit *exec.ExitError
					if errors.As(err, &exit) {
						r.ExitCode = exit.ExitCode()
						if r.ExitCode < 0 {
							r.ExitCode = 1
						}
					}
				}
				break running
			case sig := <-signals:
				_ = command.Process.Signal(sig)
			case <-deadline.C:
				_ = command.Process.Kill()
				r.Error = "supervisor hard timeout"
				r.ExitCode = 1
				_ = <-done
				break running
			case <-ticker.C:
				capture(false)
			}
		}
		ticker.Stop()
		deadline.Stop()
		signal.Stop(signals)
	}
	capture(true)
	data, err := json.MarshalIndent(r, "", "  ")
	if err == nil {
		err = os.WriteFile(*output, append(data, '\n'), 0600)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(r.MeasurementErrors) > 0 {
		fmt.Fprintln(os.Stderr, "cgroup measurements incomplete; see report")
		os.Exit(1)
	}
	os.Exit(r.ExitCode)
}
