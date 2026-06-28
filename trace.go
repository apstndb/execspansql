package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apstndb/spannerotel/tracing"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const traceServiceName = "execspansql"

func tracingEnabled(o opts) bool {
	return o.TraceStdout || o.TraceProject != "" || o.TraceOTLP
}

func traceConfig(o opts) (tracing.Config, error) {
	n := 0
	if o.TraceOTLP {
		n++
	}
	if o.TraceStdout {
		n++
	}
	if o.TraceProject != "" {
		n++
	}
	if n != 1 {
		return tracing.Config{}, fmt.Errorf("exactly one of --experimental-trace-otlp, --experimental-trace-stdout, or --experimental-trace-project must be set")
	}
	switch {
	case o.TraceOTLP:
		return tracing.Config{
			Exporter:     tracing.ExporterOTLP,
			ServiceName:  traceServiceName,
			OTLPEndpoint: o.TraceOTLPEndpoint,
			OTLPInsecure: true,
		}, nil
	case o.TraceStdout:
		return tracing.Config{
			Exporter:     tracing.ExporterStdout,
			ServiceName:  traceServiceName,
			StdoutWriter: os.Stderr,
			PrettyStdout: true,
		}, nil
	case o.TraceProject != "":
		return tracing.Config{
			Exporter:          tracing.ExporterCloudTrace,
			ServiceName:       traceServiceName,
			CloudTraceProject: o.TraceProject,
		}, nil
	default:
		return tracing.Config{}, fmt.Errorf("tracing is not configured")
	}
}

func enableTracing(ctx context.Context, o opts) (context.Context, *sdktrace.TracerProvider, context.CancelFunc, error) {
	if !tracingEnabled(o) {
		return ctx, nil, nil, nil
	}
	cfg, err := traceConfig(o)
	if err != nil {
		return ctx, nil, nil, err
	}
	tp, err := tracing.NewTracerProvider(cfg)
	if err != nil {
		return ctx, nil, nil, err
	}

	traceCtx, traceCancel := context.WithCancel(ctx)
	return traceCtx, tp, traceCancel, nil
}

func shutdownTracing(ctx context.Context, tp *sdktrace.TracerProvider) error {
	if tp == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return tracing.Shutdown(shutdownCtx, tp)
}
