// Package admission provides producer-side guards for resource pressure.
package admission

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/shawnstephens/badgerbox/internal/diskspace"
)

var (
	ErrDiskPressure = errors.New("insufficient free disk space")
	ErrDiskProbe    = errors.New("cannot determine free disk space")
)

// DiskPressureError reports the filesystem that fell below the required margin.
type DiskPressureError struct {
	Path           string
	AvailableBytes int64
	MinFreeBytes   int64
}

func (e *DiskPressureError) Error() string {
	return fmt.Sprintf("%s: %s has %d available bytes; requires %d", ErrDiskPressure, e.Path, e.AvailableBytes, e.MinFreeBytes)
}

func (e *DiskPressureError) Unwrap() error { return ErrDiskPressure }

// DiskGuardOptions configures a free-space check. Paths must already exist;
// include both Badger Dir and ValueDir when they differ.
type DiskGuardOptions struct {
	Paths []string
	// MinFreeBytes must be positive. Leave enough headroom for active writes,
	// filesystem overhead, compaction, and value-log GC before disk exhaustion.
	MinFreeBytes int64
	// RefreshInterval permits reuse of a completed sample (including errors)
	// until this duration since sampling started. Zero samples each check;
	// overlapping checks always share the same in-flight probe.
	RefreshInterval time.Duration
}

// DiskGuard rejects intake when any configured filesystem has insufficient
// free space or cannot be measured. It is safe for concurrent use. Checks are
// advisory: they do not reserve bytes, account for concurrent writers, or impose
// a physical disk cap. Combine this guard with atomic namespace admission limits.
//
// At most one probe goroutine runs per guard. Callers may cancel while waiting,
// but an operating-system filesystem call cannot be interrupted; subsequent
// calls share that probe until it completes. No background ticker is started.
type DiskGuard struct {
	paths    []string
	minimum  int64
	refresh  time.Duration
	probe    func(string) (int64, int64, error)
	mu       sync.Mutex
	inflight *diskSample
	last     *diskSample
}

type diskSample struct {
	started time.Time
	done    chan struct{}
	err     error
}

// NewDiskGuard validates configuration without probing the filesystem.
func NewDiskGuard(opts DiskGuardOptions) (*DiskGuard, error) {
	if len(opts.Paths) == 0 || opts.MinFreeBytes <= 0 || opts.RefreshInterval < 0 {
		return nil, errors.New("disk guard requires paths, positive MinFreeBytes, and nonnegative RefreshInterval")
	}
	seen := make(map[string]struct{}, len(opts.Paths))
	paths := make([]string, 0, len(opts.Paths))
	for _, path := range opts.Paths {
		if path == "" {
			return nil, errors.New("disk guard path must not be empty")
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("disk guard path: %w", err)
		}
		if _, exists := seen[absolute]; exists {
			continue
		}
		seen[absolute] = struct{}{}
		paths = append(paths, absolute)
	}
	return &DiskGuard{paths: paths, minimum: opts.MinFreeBytes, refresh: opts.RefreshInterval, probe: diskspace.Read}, nil
}

// Check can be assigned directly to badgerbox.Options.EnqueueGuard. It returns
// an error wrapping ErrDiskPressure or ErrDiskProbe when admission is denied.
// A nil context is invalid. The zero-value DiskGuard is not configured.
func (g *DiskGuard) Check(ctx context.Context) error {
	if ctx == nil {
		return errors.New("disk guard requires a context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if g == nil || g.probe == nil {
		return errors.New("disk guard is not configured")
	}
	g.mu.Lock()
	if g.last != nil && g.refresh > 0 && time.Since(g.last.started) < g.refresh {
		err := g.last.err
		g.mu.Unlock()
		return err
	}
	sample := g.inflight
	if sample == nil {
		sample = &diskSample{started: time.Now(), done: make(chan struct{})}
		g.inflight = sample
		go g.sample(sample)
	}
	g.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sample.done:
		if err := ctx.Err(); err != nil {
			return err
		}
		return sample.err
	}
}

func (g *DiskGuard) sample(sample *diskSample) {
	for _, path := range g.paths {
		total, available, err := g.probe(path)
		if err != nil {
			sample.err = fmt.Errorf("%w for %s: %w", ErrDiskProbe, path, err)
			break
		}
		if total <= 0 || available < 0 || available > total {
			sample.err = fmt.Errorf("%w for %s: invalid capacity total=%d available=%d", ErrDiskProbe, path, total, available)
			break
		}
		if available < g.minimum {
			sample.err = &DiskPressureError{Path: path, AvailableBytes: available, MinFreeBytes: g.minimum}
			break
		}
	}
	g.mu.Lock()
	g.last = sample
	g.inflight = nil
	close(sample.done)
	g.mu.Unlock()
}
