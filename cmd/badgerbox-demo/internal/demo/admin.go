package demo

import (
	"errors"
	"net"
	"net/http"
	"time"
)

func StartAdminServer(address string, handler http.Handler) (*http.Server, string, <-chan error, error) {
	if address == "" {
		return nil, "", nil, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, "", nil, err
	}
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10}
	errorsCh := make(chan error, 1)
	go func() {
		defer close(errorsCh)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errorsCh <- err
		}
	}()
	return server, listener.Addr().String(), errorsCh, nil
}
