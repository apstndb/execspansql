package main

import (
	"context"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/apstndb/spannerotel/tracing"
)

func TestTraceConfigStdout(t *testing.T) {
	t.Parallel()

	cfg, err := traceConfig(opts{TraceStdout: true})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Exporter != tracing.ExporterStdout {
		t.Fatalf("exporter = %q, want %q", cfg.Exporter, tracing.ExporterStdout)
	}
}

func TestTraceConfigMutuallyExclusive(t *testing.T) {
	t.Parallel()

	_, err := traceConfig(opts{TraceStdout: true, TraceProject: "p"})
	if err == nil {
		t.Fatal("expected error when both trace flags are set")
	}
}

func TestTraceConfigOTLP(t *testing.T) {
	t.Parallel()

	cfg, err := traceConfig(opts{TraceOTLP: true, TraceOTLPEndpoint: "127.0.0.1:4317"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Exporter != tracing.ExporterOTLP {
		t.Fatalf("exporter = %q, want %q", cfg.Exporter, tracing.ExporterOTLP)
	}
	if cfg.OTLPEndpoint != "127.0.0.1:4317" {
		t.Fatalf("endpoint = %q", cfg.OTLPEndpoint)
	}
	if cfg.ServiceName != "execspansql" {
		t.Fatalf("service name = %q", cfg.ServiceName)
	}
	if !cfg.OTLPInsecure {
		t.Fatal("expected insecure otlp for local collector")
	}
}

func TestTraceFlagsMutuallyExclusiveViaKong(t *testing.T) {
	t.Parallel()

	parser, err := kong.New(&opts{}, kong.Name("execspansql"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = parser.Parse([]string{
		"db", "--project", "p", "--instance", "i",
		"--experimental-trace-stdout",
		"--experimental-trace-project", "trace-project",
		"--sql", "SELECT 1",
	})
	if err == nil {
		t.Fatal("expected kong parse error when both trace flags are set")
	}
}

func TestEnableTracingStdout(t *testing.T) {
	ctx, tp, traceCancel, err := enableTracing(context.Background(), opts{TraceStdout: true})
	if err != nil {
		t.Fatal(err)
	}
	if tp == nil || traceCancel == nil {
		t.Fatal("expected tracer provider and cancel func")
	}
	defer traceCancel()

	_, span := tp.Tracer("execspansql").Start(ctx, "test-query")
	span.End()

	if err := shutdownTracing(context.Background(), tp); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownTracingNil(t *testing.T) {
	t.Parallel()

	if err := shutdownTracing(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestTracingEnabled(t *testing.T) {
	t.Parallel()

	if !tracingEnabled(opts{TraceStdout: true}) {
		t.Fatal("stdout should enable tracing")
	}
	if !tracingEnabled(opts{TraceProject: "demo"}) {
		t.Fatal("project should enable tracing")
	}
	if !tracingEnabled(opts{TraceOTLP: true}) {
		t.Fatal("otlp should enable tracing")
	}
	if tracingEnabled(opts{}) {
		t.Fatal("expected tracing disabled by default")
	}
}

func TestTraceConfigCloudTrace(t *testing.T) {
	t.Parallel()

	cfg, err := traceConfig(opts{TraceProject: "demo-project"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Exporter != tracing.ExporterCloudTrace {
		t.Fatalf("exporter = %q, want %q", cfg.Exporter, tracing.ExporterCloudTrace)
	}
	if cfg.CloudTraceProject != "demo-project" {
		t.Fatalf("project = %q", cfg.CloudTraceProject)
	}
}

func TestTraceConfigStdoutUsesStderr(t *testing.T) {
	t.Parallel()

	cfg, err := traceConfig(opts{TraceStdout: true})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StdoutWriter == nil {
		t.Fatal("expected default stderr writer")
	}
}
