package demo

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/shawnstephens/badgerbox/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type OTelConfig struct {
	Endpoint    string
	Protocol    string
	Insecure    bool
	ServiceName string
}

var latencyHistogramBoundaries = []float64{
	0.0005,
	0.001,
	0.0025,
	0.005,
	0.01,
	0.025,
	0.05,
	0.1,
	0.25,
	0.5,
	1,
	2.5,
	5,
	10,
}

func (c OTelConfig) Enabled() bool {
	return c.Endpoint != ""
}

func SetupOTel(ctx context.Context, cfg OTelConfig) (telemetry.Options, func(context.Context) error, error) {
	if !cfg.Enabled() {
		return telemetry.Options{}, func(context.Context) error { return nil }, nil
	}

	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = DefaultOTelServiceName
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", serviceName),
		attribute.String("service.namespace", "badgerbox-demo"),
	))
	if err != nil {
		return telemetry.Options{}, nil, err
	}

	var metricExporter sdkmetric.Exporter
	var traceExporter sdktrace.SpanExporter
	switch cfg.Protocol {
	case "", "http/protobuf":
		mopts := []otlpmetrichttp.Option{}
		topts := []otlptracehttp.Option{}
		if strings.Contains(cfg.Endpoint, "://") {
			mopts = append(mopts, otlpmetrichttp.WithEndpointURL(cfg.Endpoint))
			topts = append(topts, otlptracehttp.WithEndpointURL(cfg.Endpoint))
		} else {
			mopts = append(mopts, otlpmetrichttp.WithEndpoint(cfg.Endpoint))
			topts = append(topts, otlptracehttp.WithEndpoint(cfg.Endpoint))
		}
		if cfg.Insecure {
			mopts = append(mopts, otlpmetrichttp.WithInsecure())
			topts = append(topts, otlptracehttp.WithInsecure())
		}
		metricExporter, err = otlpmetrichttp.New(ctx, mopts...)
		if err == nil {
			traceExporter, err = otlptracehttp.New(ctx, topts...)
		}
	case "grpc":
		mopts := []otlpmetricgrpc.Option{}
		topts := []otlptracegrpc.Option{}
		if strings.Contains(cfg.Endpoint, "://") {
			mopts = append(mopts, otlpmetricgrpc.WithEndpointURL(cfg.Endpoint))
			topts = append(topts, otlptracegrpc.WithEndpointURL(cfg.Endpoint))
		} else {
			mopts = append(mopts, otlpmetricgrpc.WithEndpoint(cfg.Endpoint))
			topts = append(topts, otlptracegrpc.WithEndpoint(cfg.Endpoint))
		}
		if cfg.Insecure {
			mopts = append(mopts, otlpmetricgrpc.WithInsecure())
			topts = append(topts, otlptracegrpc.WithInsecure())
		}
		metricExporter, err = otlpmetricgrpc.New(ctx, mopts...)
		if err == nil {
			traceExporter, err = otlptracegrpc.New(ctx, topts...)
		}
	default:
		return telemetry.Options{}, nil, fmt.Errorf("unsupported OTLP protocol %q", cfg.Protocol)
	}
	if err != nil {
		if metricExporter != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = metricExporter.Shutdown(cleanupCtx)
		}
		return telemetry.Options{}, nil, err
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(5*time.Second))),
		sdkmetric.WithView(latencyHistogramView("badgerbox_enqueue_duration_seconds")),
		sdkmetric.WithView(latencyHistogramView("badgerbox_process_duration_seconds")),
		sdkmetric.WithView(latencyHistogramView("badgerbox_process_batch_duration_seconds")),
		sdkmetric.WithView(latencyHistogramView("badgerbox_kafka_promise_duration_seconds")),
	)
	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
	)

	obs := telemetry.Options{
		MeterProvider:  meterProvider,
		TracerProvider: traceProvider,
	}

	shutdown := func(ctx context.Context) error {
		return errors.Join(
			meterProvider.Shutdown(ctx),
			traceProvider.Shutdown(ctx),
		)
	}
	return obs, shutdown, nil
}

func latencyHistogramView(name string) sdkmetric.View {
	return sdkmetric.NewView(
		sdkmetric.Instrument{Name: name, Kind: sdkmetric.InstrumentKindHistogram},
		sdkmetric.Stream{
			Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: slices.Clone(latencyHistogramBoundaries),
			},
		},
	)
}
