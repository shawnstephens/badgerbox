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
	MeasurementsComplete bool     `json:"measurements_complete"`
	Samples              int      `json:"samples"`
	PeakRSS              uint64   `json:"sampled_peak_rss_bytes"`
	PeakHeap             uint64   `json:"sampled_peak_heap_bytes"`
	PeakDisk             int64    `json:"sampled_peak_apparent_disk_bytes"`
	FinalDisk            int64    `json:"final_apparent_disk_bytes"`
	Allocated            uint64   `json:"total_allocated_bytes"`
	GCCycles             uint32   `json:"gc_cycles"`
	GCPauseSeconds       float64  `json:"gc_pause_seconds"`
	CPUSeconds           float64  `json:"cpu_seconds"`
	CPUCores             float64  `json:"average_cpu_cores"`
	MeasurementErrors    []string `json:"measurement_errors,omitempty"`
}

func benchmarkDiskSize(path string) (int64, error) {
	var total int64
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
		total += info.Size()
		return nil
	})
	return total, err
}

type benchmarkSampler struct {
	cpuBaselineValid bool
	process          *process.Process
	path             string
	base             runtime.MemStats
	cpuStart         float64
	started          time.Time
	report           benchmarkResources
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
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	s.report.PeakHeap = max(s.report.PeakHeap, mem.HeapAlloc)
	if s.process != nil {
		mem, err := s.process.MemoryInfo()
		s.recordError(err)
		if err == nil {
			s.report.PeakRSS = max(s.report.PeakRSS, mem.RSS)
		}
	}
	disk, err := benchmarkDiskSize(s.path)
	s.recordError(err)
	s.report.PeakDisk = max(s.report.PeakDisk, disk)
	s.report.FinalDisk = disk
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
	return s.report
}
