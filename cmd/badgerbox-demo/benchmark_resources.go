package main

import (
	"errors"
	"github.com/shirou/gopsutil/v4/process"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

type benchmarkResources struct {
	StartedAt            time.Time `json:"started_at"`
	ElapsedSeconds       float64   `json:"elapsed_seconds"`
	MeasurementsComplete bool      `json:"measurements_complete"`
	Samples              int       `json:"samples"`
	PeakRSS              uint64    `json:"sampled_peak_rss_bytes"`
	PeakHeap             uint64    `json:"sampled_peak_heap_bytes"`
	PeakDisk             int64     `json:"sampled_peak_apparent_disk_bytes"`
	FinalDisk            int64     `json:"final_apparent_disk_bytes"`
	Allocated            uint64    `json:"total_allocated_bytes"`
	GCCycles             uint32    `json:"gc_cycles"`
	GCPauseSeconds       float64   `json:"gc_pause_seconds"`
	CPUSeconds           float64   `json:"cpu_seconds"`
	CPUCores             float64   `json:"average_cpu_cores"`
	MeasurementErrors    []string  `json:"measurement_errors,omitempty"`
}

type benchmarkDiskBreakdown struct {
	Total int64 `json:"total_bytes"`
	Vlog  int64 `json:"value_log_bytes"`
	LSM   int64 `json:"lsm_bytes"`
	WAL   int64 `json:"wal_bytes"`
	Other int64 `json:"other_bytes"`
}

func benchmarkDiskSize(path string) (int64, error) {
	disk, err := benchmarkDiskUsage(path)
	return disk.Total, err
}

func benchmarkDiskUsage(path string) (benchmarkDiskBreakdown, error) {
	var disk benchmarkDiskBreakdown
	err := filepath.WalkDir(path, func(_ string, entry os.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		size := info.Size()
		disk.Total += size
		switch filepath.Ext(entry.Name()) {
		case ".vlog":
			disk.Vlog += size
		case ".sst":
			disk.LSM += size
		case ".mem":
			disk.WAL += size
		default:
			disk.Other += size
		}
		return nil
	})
	return disk, err
}

type benchmarkSampler struct {
	cpuBaselineValid bool
	process          *process.Process
	path             string
	base             runtime.MemStats
	cpuStart         float64
	started          time.Time
	report           benchmarkResources
	current          benchmarkResourcePoint
}

func newBenchmarkSampler(path string) *benchmarkSampler {
	s := &benchmarkSampler{path: path, started: time.Now()}
	runtime.ReadMemStats(&s.base)
	var err error
	s.process, err = process.NewProcess(int32(os.Getpid()))
	s.recordError(err)
	if s.process != nil {
		t, err := s.process.Times()
		s.recordError(err)
		if err == nil {
			s.cpuStart = t.User + t.System
			s.cpuBaselineValid = true
		}
	}
	s.sample()
	return s
}
func (s *benchmarkSampler) recordError(err error) {
	if err != nil && len(s.report.MeasurementErrors) < 5 {
		s.report.MeasurementErrors = append(s.report.MeasurementErrors, err.Error())
	}
}
func (s *benchmarkSampler) sample() {
	s.report.Samples++
	point := benchmarkResourcePoint{Timestamp: time.Now().UTC(), ElapsedSeconds: time.Since(s.started).Seconds(), MeasurementsComplete: true}
	recordError := func(err error) {
		if err != nil {
			point.MeasurementsComplete = false
			s.recordError(err)
		}
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	point.Heap = mem.HeapAlloc
	point.Allocated = mem.TotalAlloc - s.base.TotalAlloc
	point.GCCycles = mem.NumGC - s.base.NumGC
	s.report.PeakHeap = max(s.report.PeakHeap, mem.HeapAlloc)
	if s.process != nil {
		mem, err := s.process.MemoryInfo()
		recordError(err)
		if err == nil {
			point.RSS = mem.RSS
			s.report.PeakRSS = max(s.report.PeakRSS, mem.RSS)
		}
		if s.cpuBaselineValid {
			times, err := s.process.Times()
			recordError(err)
			if err == nil {
				cpu := times.User + times.System - s.cpuStart
				point.CPUSeconds = &cpu
			}
		} else {
			point.MeasurementsComplete = false
		}
	} else {
		point.MeasurementsComplete = false
	}
	disk, err := benchmarkDiskUsage(s.path)
	recordError(err)
	point.Disk = disk
	s.report.PeakDisk = max(s.report.PeakDisk, disk.Total)
	s.report.FinalDisk = disk.Total
	s.current = point
}
func (s *benchmarkSampler) finish() benchmarkResources {
	s.sample()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	s.report.Allocated = mem.TotalAlloc - s.base.TotalAlloc
	s.report.GCCycles = mem.NumGC - s.base.NumGC
	s.report.GCPauseSeconds = float64(mem.PauseTotalNs-s.base.PauseTotalNs) / 1e9
	if s.process != nil && s.cpuBaselineValid {
		t, err := s.process.Times()
		s.recordError(err)
		if err == nil {
			s.report.CPUSeconds = t.User + t.System - s.cpuStart
			s.report.CPUCores = s.report.CPUSeconds / time.Since(s.started).Seconds()
		}
	}
	s.report.MeasurementsComplete = len(s.report.MeasurementErrors) == 0
	s.report.ElapsedSeconds = time.Since(s.started).Seconds()
	s.report.StartedAt = s.started.UTC()
	return s.report
}
