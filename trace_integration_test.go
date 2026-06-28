package main

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spanemuboost"
	"github.com/apstndb/spannerotel/interceptor"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func TestSpannerQueryTracingWithoutOpenCensusBridge(t *testing.T) {
	ctx := context.Background()

	env, err := spanemuboost.RunEmulatorWithClients(ctx,
		spanemuboost.WithSetupDDLs(mustSplitSQLStatements(t, ddl)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close() //nolint:errcheck

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	oldTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(oldTP)
	})

	client, err := spanner.NewClientWithConfig(ctx, env.DatabasePath(), spanner.ClientConfig{}, append(env.ClientOptions(),
		option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(
			interceptor.StreamInterceptor(interceptor.WithDefaultDecorators()),
		)),
	)...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	iter := client.Single().Query(ctx, spanner.NewStatement("SELECT 1 AS n"))
	if err := iter.Do(func(*spanner.Row) error { return nil }); err != nil {
		t.Fatal(err)
	}

	spans := sr.Ended()
	if len(spans) == 0 {
		t.Fatal("expected Spanner client spans without OpenCensus bridge")
	}
	if !containsSpanName(spans, "cloud.google.com/go/spanner.Query") {
		t.Fatalf("missing Spanner Query span; got %d spans: %v", len(spans), spanNames(spans))
	}
}

func containsSpanName(spans []sdktrace.ReadOnlySpan, want string) bool {
	for _, span := range spans {
		if span.Name() == want {
			return true
		}
	}
	return false
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	names := make([]string, len(spans))
	for i, span := range spans {
		names[i] = span.Name()
	}
	return names
}
