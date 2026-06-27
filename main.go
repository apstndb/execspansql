package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/apstndb/execspansql/params"
	"io"
	"regexp"
	"time"

	"fmt"
	"log"
	"os"

	"encoding/json"
	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	oteloc "go.opentelemetry.io/otel/bridge/opencensus"

	"go.opentelemetry.io/otel"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"gopkg.in/yaml.v3"

	"google.golang.org/api/option"
	"google.golang.org/grpc"

	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/alecthomas/kong"
	"github.com/apstndb/execspansql/jqresult"
	"github.com/apstndb/execspansql/resultset"
	"github.com/apstndb/spaniter"
	"github.com/apstndb/spannerotel/interceptor"
	svwriter "github.com/apstndb/spanvalue/writer"
	"github.com/wader/gojq"
)

func main() {
	if err := _main(); err != nil {
		log.Fatalln(err)
	}
}

var dmlRe = regexp.MustCompile(`(?is)^\s*(INSERT|UPDATE|DELETE)\s+.+$`)

var debuglog *log.Logger

func init() {
	if os.Getenv("DEBUG") != "" {
		debuglog = log.New(os.Stderr, "", log.LstdFlags)
	} else {
		debuglog = log.New(io.Discard, "", log.LstdFlags)
	}
	// suppress
	_ = debuglog
}

type opts struct {
	Database             string        `arg:"" required:"" help:"ID of the database."`
	Sql                  string        `name:"sql" help:"SQL query text; exclusive with --sql-file."`
	SqlFile              string        `name:"sql-file" help:"File name contains SQL query; exclusive with --sql"`
	Project              string        `name:"project" short:"p" env:"CLOUDSDK_CORE_PROJECT" required:"" help:"ID of the project."`
	Instance             string        `name:"instance" short:"i" env:"CLOUDSDK_SPANNER_INSTANCE" required:"" help:"ID of the instance."`
	QueryMode            string        `name:"query-mode" enum:"NORMAL,PLAN,PROFILE" default:"NORMAL" help:"Query mode."`
	Format               string        `name:"format" enum:"json,yaml,experimental_csv" default:"json" help:"Output format."`
	RedactRows           bool          `name:"redact-rows" help:"Redact result rows from output"`
	CompactOutput        bool          `name:"compact-output" short:"c" help:"Compact JSON output (--compact-output of jq)"`
	JqFilter             string        `name:"filter" help:"jq filter"`
	JqRawOutput          bool          `name:"raw-output" short:"r" help:"(--raw-output of jq)"`
	JqFromFile           string        `name:"filter-file" help:"(--from-file of jq)"`
	JqInputMode          string        `name:"jq-input-mode" enum:"eager,lazy" default:"eager" help:"How query rows are passed to jq (json/yaml only): eager (full ResultSet), lazy (JQValue root)."`
	ParamFlags           []string      `name:"param" help:"[name]:[Cloud Spanner type (PLAN only) or literal]"`
	ParamFile            string        `name:"param-file" help:"YAML or JSON file of query parameters (name to type/literal string)"`
	LogGrpc              bool          `name:"log-grpc" help:"Show gRPC logs"`
	TraceProject         string        `name:"experimental-trace-project" help:"Export traces to Cloud Trace in the given project."`
	EnablePartitionedDML bool          `name:"enable-partitioned-dml" help:"Execute DML statement using Partitioned DML"`
	Timeout              time.Duration `name:"timeout" default:"10m" help:"Maximum time to wait for the SQL query to complete"`
	TryPartitionQuery    bool          `name:"try-partition-query" help:"(Experimental) Check whether the query can be executed as partition query or not"`
	TimestampBound       struct {
		Strong        bool   `name:"strong" help:"Perform a strong query."`
		ReadTimestamp string `name:"read-timestamp" help:"Perform a query at the given timestamp. (micro-seconds precision)"`
	} `embed:"" prefix:""`
}

func (o opts) mergedParams() (map[string]string, error) {
	cliParams, err := params.ParseParamFlags(o.ParamFlags)
	if err != nil {
		return nil, err
	}
	if o.ParamFile == "" {
		return cliParams, nil
	}
	fileParams, err := params.LoadParamFile(o.ParamFile)
	if err != nil {
		return nil, err
	}
	return params.MergeParams(fileParams, cliParams), nil
}

func processFlags() (o opts, err error) {
	parser, err := kong.New(&o,
		kong.Name("execspansql"),
		kong.Description("Yet another gcloud spanner databases execute-sql replacement"),
	)
	if err != nil {
		return o, err
	}
	var ctx *kong.Context
	defer func() {
		if err == nil {
			return
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		if ctx != nil {
			_ = ctx.PrintUsage(false)
			return
		}
		var parseErr *kong.ParseError
		if errors.As(err, &parseErr) && parseErr.Context != nil {
			_ = parseErr.Context.PrintUsage(false)
		}
	}()
	ctx, err = parser.Parse(os.Args[1:])
	if err != nil {
		return o, err
	}

	if _, err := time.Parse(time.RFC3339Nano, o.TimestampBound.ReadTimestamp); o.TimestampBound.ReadTimestamp != "" && err != nil {
		return o, fmt.Errorf("--read-timestamp is supplied but wrong: %w", err)
	}

	if o.TimestampBound.Strong && o.TimestampBound.ReadTimestamp != "" {
		return o, errors.New("--strong and --read-timestamp are exclusive")
	}

	if o.Sql != "" && o.SqlFile != "" {
		return o, errors.New("--sql and --sql-file are exclusive")
	}

	if o.Sql == "" && o.SqlFile == "" {
		return o, errors.New("--sql or --sql-file is required")
	}

	if o.JqFilter != "" && o.JqFromFile != "" {
		return o, errors.New("--filter and --filter-file are exclusive")
	}

	if _, err := jqresult.ParseInputMode(o.JqInputMode); err != nil {
		return o, err
	}
	return o, nil
}

// readFileOrDefault returns content of filename or s if filename is empty
func readFileOrDefault(filename, s string) (string, error) {
	if filename == "" {
		return s, nil
	}
	b, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func logGrpcClientOptions() []option.ClientOption {
	zapDevelopmentConfig := zap.NewDevelopmentConfig()
	zapDevelopmentConfig.DisableCaller = true
	zapLogger, _ := zapDevelopmentConfig.Build(zap.Fields())

	return []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(
			grpczap.PayloadUnaryClientInterceptor(zapLogger, func(ctx context.Context, fullMethodName string) bool {
				return true
			}),
			grpczap.UnaryClientInterceptor(zapLogger),
		)),
		option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(
			grpczap.PayloadStreamClientInterceptor(zapLogger, func(ctx context.Context, fullMethodName string) bool {
				return true
			}),
			grpczap.StreamClientInterceptor(zapLogger),
		)),
	}
}

type queryMode interface{ isQueryMode() }

type single struct{ spanner.TimestampBound }
type readWrite struct{}
type partitionedDML struct{}

func (s single) isQueryMode()         {}
func (r readWrite) isQueryMode()      {}
func (p partitionedDML) isQueryMode() {}

// dmlRowCountForMode reports whether read-write results should encode exact DML
// row counts. PLAN mode returns false because execution does not produce a count.
func dmlRowCountForMode(mode queryMode, opts spanner.QueryOptions) bool {
	if _, ok := mode.(readWrite); !ok {
		return false
	}
	if opts.Mode != nil && *opts.Mode == sppb.ExecuteSqlRequest_PLAN {
		return false
	}
	return true
}

func spaniterStatsOpts(mode queryMode, opts spanner.QueryOptions) []spaniter.Option {
	if dmlRowCountForMode(mode, opts) {
		return []spaniter.Option{spaniter.WithStatsEncoding(spaniter.StatsEncodingDMLExact)}
	}
	return nil
}

func runInNewTransaction(ctx context.Context, client *spanner.Client, stmt spanner.Statement, opts spanner.QueryOptions, mode queryMode, reductRows bool) (*sppb.ResultSet, error) {
	statOpts := spaniterStatsOpts(mode, opts)
	var rs *sppb.ResultSet
	switch mode := mode.(type) {
	case readWrite:
		_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) (err error) {
			rs, err = resultset.Materialize(tx.QueryWithOptions(ctx, stmt, opts), reductRows, statOpts...)
			return err
		})
		return rs, err
	case single:
		return resultset.Materialize(client.Single().WithTimestampBound(mode.TimestampBound).QueryWithOptions(ctx, stmt, opts), reductRows, statOpts...)
	case partitionedDML:
		count, err := client.PartitionedUpdateWithOptions(ctx, stmt, opts)
		return &sppb.ResultSet{
			Metadata: &sppb.ResultSetMetadata{
				RowType: &sppb.StructType{},
			},
			Stats: &sppb.ResultSetStats{
				RowCount: &sppb.ResultSetStats_RowCountLowerBound{RowCountLowerBound: count},
			},
		}, err
	default:
		panic(fmt.Sprintf("unknown mode: %T", mode))
	}
}

func cloudOperationsExporter(project string) (sdktrace.SpanExporter, error) {
	exporter, err := texporter.New(texporter.WithProjectID(project))
	if err != nil {
		return nil, fmt.Errorf("texporter.New: %v", err)
	}
	return exporter, err
}

func TracerProvider(project string) (*sdktrace.TracerProvider, error) {
	exp, err := cloudOperationsExporter(project)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		// Always be sure to batch in production.
		sdktrace.WithBatcher(exp),
	)
	return tp, nil
}

func _main() error {
	o, err := processFlags()
	if err != nil {
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), o.Timeout)
	defer cancel()

	jqMode, err := jqresult.ParseInputMode(o.JqInputMode)
	if err != nil {
		return err
	}
	if err := jqMode.ValidateFormat(o.Format); err != nil {
		return err
	}

	jqFilter, err := readFileOrDefault(o.JqFromFile, o.JqFilter)
	if err != nil {
		return err
	}
	if jqFilter == "" {
		jqFilter = jqresult.DefaultFilter(jqMode)
	}

	jqCode, err := jqresult.Compile(jqFilter, jqMode)
	if err != nil {
		return err
	}

	mode := sppb.ExecuteSqlRequest_QueryMode(sppb.ExecuteSqlRequest_QueryMode_value[o.QueryMode])

	query, err := readFileOrDefault(o.SqlFile, o.Sql)
	if err != nil {
		return err
	}

	doTrace := o.TraceProject != ""
	if doTrace {
		tp, err := TracerProvider(o.TraceProject)
		if err != nil {
			return err
		}

		otel.SetTracerProvider(tp)
		oteloc.InstallTraceBridge()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Cleanly shutdown and flush telemetry when the application exits.
		defer func(ctx context.Context) {
			// Do not make the application hang when it is shutdown.
			ctx, cancel = context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			if err := tp.Shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}(ctx)
	}

	logGrpc := o.LogGrpc

	client, err := newClient(ctx, o.Project, o.Instance, o.Database, logGrpc, doTrace)
	if err != nil {
		return err
	}
	defer client.Close()

	paramStrMap, err := o.mergedParams()
	if err != nil {
		return err
	}
	paramMap, err := params.GenerateParams(paramStrMap, mode == sppb.ExecuteSqlRequest_PLAN)
	if err != nil {
		return err
	}

	var tb spanner.TimestampBound
	if o.TimestampBound.ReadTimestamp != "" {
		ts, err := time.Parse(time.RFC3339Nano, o.TimestampBound.ReadTimestamp)
		if err != nil {
			return err
		}
		tb = spanner.ReadTimestamp(ts)
	} else {
		tb = spanner.StrongRead()
	}

	var m queryMode
	switch {
	case o.EnablePartitionedDML:
		m = partitionedDML{}
	case dmlRe.MatchString(query):
		m = readWrite{}
	default:
		m = single{tb}
	}

	stmt := spanner.Statement{SQL: query, Params: paramMap}

	if o.TryPartitionQuery {
		bt, err := client.BatchReadOnlyTransaction(ctx, spanner.StrongRead())
		if err != nil {
			return err
		}
		defer bt.Close()

		_, err = bt.PartitionQuery(ctx, stmt, spanner.PartitionOptions{})
		if err != nil {
			return err
		}

		bt.Cleanup(ctx)
		fmt.Println("success")
		return nil
	}

	if o.Format == "experimental_csv" {
		return runAndWriteCsv(ctx, client, stmt, spanner.QueryOptions{Mode: &mode}, m, o.RedactRows)
	}

	return runJqOutput(ctx, client, stmt, spanner.QueryOptions{Mode: &mode}, m, o, jqMode, jqCode)
}

func runAndWriteCsv(ctx context.Context, client *spanner.Client, stmt spanner.Statement, opts spanner.QueryOptions, mode queryMode, redactRows bool) error {
	switch mode := mode.(type) {
	case readWrite:
		var buf bytes.Buffer
		_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			buf.Reset()
			return writeCsvFromRowIter(&buf, tx.QueryWithOptions(ctx, stmt, opts), redactRows)
		})
		if err != nil {
			return err
		}
		_, err = io.Copy(os.Stdout, &buf)
		return err
	case single:
		return writeCsvFromRowIter(
			os.Stdout,
			client.Single().WithTimestampBound(mode.TimestampBound).QueryWithOptions(ctx, stmt, opts),
			redactRows,
		)
	case partitionedDML:
		count, err := client.PartitionedUpdateWithOptions(ctx, stmt, opts)
		if err != nil {
			return err
		}
		return writeCsvFromResultSet(os.Stdout, &sppb.ResultSet{
			Metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{}},
			Stats: &sppb.ResultSetStats{
				RowCount: &sppb.ResultSetStats_RowCountLowerBound{RowCountLowerBound: count},
			},
		})
	default:
		panic(fmt.Sprintf("unknown mode: %T", mode))
	}
}

// csvRedactRowIteratorWriter implements [svwriter.RowIteratorWriter] for --redact-rows CSV:
// it registers schema and flushes the header via the embedded [svwriter.DelimitedWriter] but
// discards row bodies in WriteRow while WriteRowIterator drains the iterator.
type csvRedactRowIteratorWriter struct {
	*svwriter.DelimitedWriter
}

func (csvRedactRowIteratorWriter) WriteRow(*spanner.Row) error { return nil }

// writeCsvFromRowIter streams query rows to CSV without materializing a ResultSet.
// Pass the query iterator directly to WriteRowIterator (it owns Stop); do not defer Stop at the call site.
func writeCsvFromRowIter(writer io.Writer, rowIter *spanner.RowIterator, redactRows bool) error {
	csvWriter, err := svwriter.NewCSVWriter(writer)
	if err != nil {
		return err
	}
	iterWriter := svwriter.RowIteratorWriter(csvWriter)
	if redactRows {
		iterWriter = csvRedactRowIteratorWriter{csvWriter}
	}
	_, err = svwriter.WriteRowIterator(rowIter, iterWriter)
	return err
}

func prepareCsvRowType(csvWriter *svwriter.DelimitedWriter, metadata *sppb.ResultSetMetadata) error {
	if metadata == nil || metadata.GetRowType() == nil {
		return errors.New("result set metadata is missing or invalid")
	}
	return csvWriter.PrepareRowType(metadata.GetRowType())
}

// writeCsvFromResultSet writes CSV from an in-memory ResultSet. Used by unit tests
// and partitioned DML (no RowIterator). WithMetadata at construction is appropriate here.
func writeCsvFromResultSet(writer io.Writer, rs *sppb.ResultSet) error {
	if rs == nil || rs.GetMetadata() == nil || rs.GetMetadata().GetRowType() == nil {
		return errors.New("result set metadata is missing or invalid")
	}

	csvWriter, err := svwriter.NewCSVWriter(writer, svwriter.WithMetadata(rs.GetMetadata()))
	if err != nil {
		return err
	}
	for _, row := range rs.GetRows() {
		if row == nil {
			return fmt.Errorf("nil row in result set")
		}
		if err := csvWriter.WriteStructValues(row.GetValues()); err != nil {
			return err
		}
	}
	return csvWriter.Flush()
}

func newClient(ctx context.Context, project, instance, database string, logGrpc bool, doTrace bool) (*spanner.Client, error) {
	name := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)

	var copts []option.ClientOption
	if logGrpc {
		copts = logGrpcClientOptions()
	}

	if doTrace {
		copts = append(copts, option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(interceptor.StreamInterceptor(interceptor.WithDefaultDecorators()))))
	}

	return spanner.NewClientWithConfig(ctx, name, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MaxOpened:           1,
			MinOpened:           1,
			TrackSessionHandles: true,
		},
	}, copts...)
}

type encoder interface {
	Encode(v any) error
}

type stringPassThroughEncoderWrapper struct {
	Writer io.Writer
	Enc    encoder
}

func (enc *stringPassThroughEncoderWrapper) Encode(v any) error {
	if s, ok := v.(string); ok {
		_, err := fmt.Fprintln(enc.Writer, s)
		return err
	}
	return enc.Enc.Encode(v)
}

func runJqOutput(
	ctx context.Context,
	client *spanner.Client,
	stmt spanner.Statement,
	opts spanner.QueryOptions,
	mode queryMode,
	o opts,
	jqMode jqresult.InputMode,
	jqCode *gojq.Code,
) error {
	useEager := jqMode == jqresult.InputEager
	if _, ok := mode.(readWrite); ok {
		useEager = true
	}
	if useEager {
		rs, err := runInNewTransaction(ctx, client, stmt, opts, mode, o.RedactRows)
		if err != nil {
			return err
		}
		enc, err := newEncoder(os.Stdout, o.Format, o.CompactOutput, o.JqRawOutput)
		if err != nil {
			return err
		}
		iter, cleanup, err := jqresult.Execute(jqCode, jqresult.InputEager, nil, rs, o.RedactRows)
		if err != nil {
			return err
		}
		defer cleanup()
		return jqresult.Print(enc, iter)
	}

	switch mode := mode.(type) {
	case readWrite:
		panic("read-write jq uses eager materialization")
	case single:
		enc, err := newEncoder(os.Stdout, o.Format, o.CompactOutput, o.JqRawOutput)
		if err != nil {
			return err
		}
		rowIter := client.Single().WithTimestampBound(mode.TimestampBound).QueryWithOptions(ctx, stmt, opts)
		return runJqOnRowIter(rowIter, o.RedactRows, jqCode, enc)
	case partitionedDML:
		return fmt.Errorf("--jq-input-mode=lazy is not supported for partitioned DML")
	default:
		panic(fmt.Sprintf("unknown mode: %T", mode))
	}
}

func runJqOnRowIter(
	rowIter *spanner.RowIterator,
	redactRows bool,
	jqCode *gojq.Code,
	enc encoder,
) error {
	iter, cleanup, err := jqresult.Execute(jqCode, jqresult.InputLazy, rowIter, nil, redactRows)
	if err != nil {
		return err
	}
	defer cleanup()
	return jqresult.Print(enc, iter)
}

func newEncoder(writer io.Writer, format string, compactOutput bool, rawOutput bool) (encoder, error) {
	switch format {
	case "yaml":
		return yaml.NewEncoder(writer), nil
	case "json":
		jsonenc := json.NewEncoder(writer)
		jsonenc.SetEscapeHTML(false)
		if !compactOutput {
			jsonenc.SetIndent("", "  ")
		}
		if rawOutput {
			return &stringPassThroughEncoderWrapper{Writer: writer, Enc: jsonenc}, nil
		}
		return jsonenc, nil
	default:
		return nil, fmt.Errorf("unknown format: %s", format)
	}
}
