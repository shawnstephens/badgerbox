package admission

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testGuard(t *testing.T, interval time.Duration) *DiskGuard {
	t.Helper()
	g, err := NewDiskGuard(DiskGuardOptions{Paths: []string{t.TempDir()}, MinFreeBytes: 100, RefreshInterval: interval})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestDiskGuardPressureProbeErrorsAndRecovery(t *testing.T) {
	g := testGuard(t, 0)
	available := int64(99)
	var probeErr error
	g.probe = func(string) (int64, int64, error) { return 1000, available, probeErr }
	err := g.Check(t.Context())
	var pressure *DiskPressureError
	if !errors.Is(err, ErrDiskPressure) || !errors.As(err, &pressure) || pressure.AvailableBytes != 99 {
		t.Fatalf("pressure: %v", err)
	}
	available = 100
	if err := g.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	probeErr = errors.New("unmounted")
	if err := g.Check(t.Context()); !errors.Is(err, ErrDiskProbe) || !errors.Is(err, probeErr) {
		t.Fatalf("probe: %v", err)
	}
	probeErr = nil
	available = -1
	if err := g.Check(t.Context()); !errors.Is(err, ErrDiskProbe) {
		t.Fatalf("negative: %v", err)
	}
	available = 1001
	if err := g.Check(t.Context()); !errors.Is(err, ErrDiskProbe) {
		t.Fatalf("invalid total: %v", err)
	}
}

func TestDiskGuardChecksEveryPathAndCachesFailures(t *testing.T) {
	p1, p2 := t.TempDir(), t.TempDir()
	g, err := NewDiskGuard(DiskGuardOptions{Paths: []string{p1, p1, p2}, MinFreeBytes: 100, RefreshInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	g.probe = func(path string) (int64, int64, error) {
		calls.Add(1)
		if path == p2 {
			return 1000, 0, nil
		}
		return 1000, 500, nil
	}
	for range 2 {
		if err := g.Check(t.Context()); !errors.Is(err, ErrDiskPressure) {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
	g.mu.Lock()
	g.last.started = time.Now().Add(-2 * time.Hour)
	g.mu.Unlock()
	if err := g.Check(t.Context()); !errors.Is(err, ErrDiskPressure) {
		t.Fatal(err)
	}
	if calls.Load() != 4 {
		t.Fatalf("expired cache calls=%d", calls.Load())
	}
}

func TestDiskGuardBlockedProbeIsBoundedAndCancellable(t *testing.T) {
	g := testGuard(t, time.Hour)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	g.probe = func(string) (int64, int64, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return 1000, 500, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	first := make(chan error, 1)
	go func() { first <- g.Check(ctx) }()
	<-entered
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			if err := g.Check(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("waiter: %v", err)
			}
		})
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("concurrent probes=%d", calls.Load())
	}
	close(release)
	if err := g.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("probe result was not shared: %d", calls.Load())
	}
}

func TestDiskGuardConfigurationAndFilesystem(t *testing.T) {
	for _, opts := range []DiskGuardOptions{{}, {Paths: []string{""}, MinFreeBytes: 1}, {Paths: []string{"."}, MinFreeBytes: -1}, {Paths: []string{"."}, MinFreeBytes: 1, RefreshInterval: -time.Second}} {
		if _, err := NewDiskGuard(opts); err == nil {
			t.Fatalf("accepted %+v", opts)
		}
	}
	g := testGuard(t, 0)
	if err := g.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := g.Check(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := g.Check(nil); err == nil {
		t.Fatal("nil context accepted")
	}
	if err := new(DiskGuard).Check(t.Context()); err == nil {
		t.Fatal("unconfigured guard accepted")
	}
}
