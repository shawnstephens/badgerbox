package demo

import (
	"context"
	"expvar"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDefaultExpvarListenAddr(t *testing.T) {
	t.Parallel()

	if DefaultExpvarListenAddr != "0.0.0.0:18080" {
		t.Fatalf("DefaultExpvarListenAddr = %q, want %q", DefaultExpvarListenAddr, "0.0.0.0:18080")
	}
}

func TestStartExpvarServerDisabledWhenAddrBlank(t *testing.T) {
	t.Parallel()

	server, addr, errCh, err := StartExpvarServer("")
	if err != nil {
		t.Fatalf("StartExpvarServer(): %v", err)
	}
	if server != nil || addr != "" || errCh != nil {
		t.Fatalf("expected disabled expvar server, got server=%v addr=%q errCh=%v", server, addr, errCh)
	}
}

func TestStartExpvarServerServesDebugVars(t *testing.T) {
	t.Parallel()

	server, addr, _, err := StartExpvarServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("StartExpvarServer(): %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	resp, err := http.Get("http://" + addr + "/debug/vars")
	if err != nil {
		t.Fatalf("GET /debug/vars: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /debug/vars: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), "\"cmdline\"") {
		t.Fatalf("expected expvar payload, got %q", string(body))
	}
}

func TestStartExpvarServerServesPrometheusBadgerMetrics(t *testing.T) {
	setBadgerExpvarInt(t, "badger_get_num_user", 7)
	setBadgerExpvarInt(t, "badger_put_num_user", 3)
	setBadgerExpvarInt(t, "badger_write_bytes_user", 42)
	setBadgerExpvarMapValue(t, "badger_size_bytes_lsm", "/tmp/badger-test", 12)
	setBadgerExpvarMapValue(t, "badger_size_bytes_vlog", "/tmp/badger-test", 18)
	setBadgerExpvarMapValue(t, "badger_write_pending_num_memtable", "/tmp/badger-test", 2)
	setBadgerExpvarInt(t, "badger_compaction_current_num_lsm", 1)

	server, addr, _, err := StartExpvarServer("127.0.0.1:0")
	if err != nil {
		t.Fatalf("StartExpvarServer(): %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /metrics: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	metrics := string(body)
	if !strings.Contains(metrics, "badger_get_num_user 7") {
		t.Fatalf("expected badger_get_num_user in /metrics, got %q", metrics)
	}
	if !strings.Contains(metrics, "badger_size_bytes_lsm{directory=\"/tmp/badger-test\"} 12") {
		t.Fatalf("expected labeled badger_size_bytes_lsm in /metrics, got %q", metrics)
	}
	if !strings.Contains(metrics, "badger_compaction_current_num_lsm 1") {
		t.Fatalf("expected badger_compaction_current_num_lsm in /metrics, got %q", metrics)
	}
}

func setBadgerExpvarInt(t *testing.T, name string, value int64) {
	t.Helper()

	if v := expvar.Get(name); v != nil {
		intVar, ok := v.(*expvar.Int)
		if !ok {
			t.Fatalf("expvar %s type = %T, want *expvar.Int", name, v)
		}
		intVar.Set(value)
		return
	}

	expvar.NewInt(name).Set(value)
}

func setBadgerExpvarMapValue(t *testing.T, name, key string, value int64) {
	t.Helper()

	if v := expvar.Get(name); v != nil {
		mapVar, ok := v.(*expvar.Map)
		if !ok {
			t.Fatalf("expvar %s type = %T, want *expvar.Map", name, v)
		}
		mapVar.Init()
		mapVar.Add(key, value)
		return
	}

	mapVar := expvar.NewMap(name)
	mapVar.Add(key, value)
}
