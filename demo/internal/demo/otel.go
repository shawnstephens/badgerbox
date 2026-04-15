package demo

import (
	"context"
	"errors"
	"time"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type OTelConfig struct {
	Endpoint    string
	ServiceName string
}

func (c OTelConfig) Enabled() bool {
	return c.Endpoint != ""
}

func SetupOTel(ctx context.Context, cfg OTelConfig) (badgerbox.ObservabilityOptions, func(context.Context) error, error) {
	if !cfg.Enabled() {
		return badgerbox.ObservabilityOptions{}, func(context.Context) error { return nil }, nil
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
		return badgerbox.ObservabilityOptions{}, nil, err
	}

	metricExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(cfg.Endpoint),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		return badgerbox.ObservabilityOptions{}, nil, err
	}
	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(cfg.Endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return badgerbox.ObservabilityOptions{}, nil, err
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(5*time.Second))),
	)
	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
	)

	obs := badgerbox.ObservabilityOptions{
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
