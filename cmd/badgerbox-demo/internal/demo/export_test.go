package demo

import (
	"context"
	metricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type metricServer struct {
	metricpb.UnimplementedMetricsServiceServer
	calls *atomic.Int32
}

func (s metricServer) Export(context.Context, *metricpb.ExportMetricsServiceRequest) (*metricpb.ExportMetricsServiceResponse, error) {
	s.calls.Add(1)
	return &metricpb.ExportMetricsServiceResponse{}, nil
}

type traceServer struct {
	tracepb.UnimplementedTraceServiceServer
	calls *atomic.Int32
}

func (s traceServer) Export(context.Context, *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	s.calls.Add(1)
	return &tracepb.ExportTraceServiceResponse{}, nil
}
func TestOTLPExportsMetricsAndTracesOverBothTransports(t *testing.T) {
	for _, protocol := range []string{"http/protobuf", "grpc"} {
		t.Run(protocol, func(t *testing.T) {
			var metricCalls, traceCalls atomic.Int32
			endpoint := ""
			if protocol == "http/protobuf" {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/metrics":
						metricCalls.Add(1)
					case "/v1/traces":
						traceCalls.Add(1)
					default:
						t.Errorf("path %s", r.URL.Path)
					}
					w.Header().Set("Content-Type", "application/x-protobuf")
					w.WriteHeader(200)
				}))
				defer server.Close()
				endpoint = strings.TrimPrefix(server.URL, "http://")
			} else {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				server := grpc.NewServer()
				metricpb.RegisterMetricsServiceServer(server, metricServer{calls: &metricCalls})
				tracepb.RegisterTraceServiceServer(server, traceServer{calls: &traceCalls})
				go server.Serve(listener)
				defer server.Stop()
				endpoint = listener.Addr().String()
			}
			obs, shutdown, err := SetupOTel(t.Context(), OTelConfig{Endpoint: endpoint, Protocol: protocol, Insecure: true})
			if err != nil {
				t.Fatal(err)
			}
			counter, err := obs.MeterProvider.Meter("test").Int64Counter("test_count")
			if err != nil {
				t.Fatal(err)
			}
			counter.Add(t.Context(), 1)
			_, span := obs.TracerProvider.Tracer("test").Start(t.Context(), "test")
			span.End()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err = shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			if metricCalls.Load() == 0 || traceCalls.Load() == 0 {
				t.Fatalf("exports: metrics=%d traces=%d", metricCalls.Load(), traceCalls.Load())
			}
		})
	}
}
