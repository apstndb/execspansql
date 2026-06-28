package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apstndb/spannerotel/tracing"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func tracingEnabled(o opts) bool {
	return o.TraceStdout || o.TraceProject != ""
}

func traceConfig(o opts) (tracing.Config, error) {
	switch {
	case o.TraceStdout && o.TraceProject != "":
		return tracing.Config{}, fmt.Errorf("use either --experimental-trace-stdout or --experimental-trace-project, not both")
	case o.TraceStdout:
		return tracing.Config{
			Exporter:     tracing.ExporterStdout,
			StdoutWriter: os.Stderr,
			PrettyStdout: true,
		}, nil
	case o.TraceProject != "":
		return tracing.Config{
			Exporter:          tracing.ExporterCloudTrace,
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
