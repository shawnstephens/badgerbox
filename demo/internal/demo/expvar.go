package demo

import (
	"errors"
	"expvar"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func StartExpvarServer(listenAddr string) (*http.Server, string, <-chan error, error) {
	if listenAddr == "" {
		return nil, "", nil, nil
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, "", nil, err
	}

	registry := prometheus.NewRegistry()
	if err := registry.Register(collectors.NewExpvarCollector(badgerExpvarExports())); err != nil {
		_ = listener.Close()
		return nil, "", nil, err
	}

	mux := http.NewServeMux()
	mux.Handle("/debug/vars", expvar.Handler())
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	server := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	return server, listener.Addr().String(), errCh, nil
}

func badgerExpvarExports() map[string]*prometheus.Desc {
	return map[string]*prometheus.Desc{
		"badger_get_num_user": prometheus.NewDesc(
			"badger_get_num_user",
			"Badger user get operations exported from expvar.",
			nil,
			nil,
		),
		"badger_put_num_user": prometheus.NewDesc(
			"badger_put_num_user",
			"Badger user put operations exported from expvar.",
			nil,
			nil,
		),
		"badger_write_bytes_user": prometheus.NewDesc(
			"badger_write_bytes_user",
			"Badger user write bytes exported from expvar.",
			nil,
			nil,
		),
		"badger_size_bytes_lsm": prometheus.NewDesc(
			"badger_size_bytes_lsm",
			"Badger LSM size in bytes exported from expvar.",
			[]string{"directory"},
			nil,
		),
		"badger_size_bytes_vlog": prometheus.NewDesc(
			"badger_size_bytes_vlog",
			"Badger value log size in bytes exported from expvar.",
			[]string{"directory"},
			nil,
		),
		"badger_write_pending_num_memtable": prometheus.NewDesc(
			"badger_write_pending_num_memtable",
			"Badger pending memtable writes exported from expvar.",
			[]string{"directory"},
			nil,
		),
		"badger_compaction_current_num_lsm": prometheus.NewDesc(
			"badger_compaction_current_num_lsm",
			"Badger current LSM compactions exported from expvar.",
			nil,
			nil,
		),
	}
}
