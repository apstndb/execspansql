package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"regexp"
	"time"

	"fmt"
	"log"
	"os"

	"encoding/json"

	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	octrace "go.opencensus.io/trace"
	oteloc "go.opentelemetry.io/otel/bridge/opencensus"

	"go.opentelemetry.io/otel"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"google.golang.org/api/option"
	"google.golang.org/grpc"

	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spannerotel/interceptor"
	"github.com/itchyny/gojq"
	"github.com/jessevdk/go-flags"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
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
}

type opts struct {
	Positional struct {
		Database string `positional-arg-name:"database" description:"(required) ID of the database." required:"true"`
	} `positional-args:"yes"`
	Sql                  string            `long:"sql" description:"SQL query text; exclusive with --sql-file."`
	SqlFile              string            `long:"sql-file" description:"File name contains SQL query; exclusive with --sql"`
	Project              string            `long:"project" short:"p" description:"(required) ID of the project." required:"true" env:"CLOUDSDK_CORE_PROJECT"`
	Instance             string            `long:"instance" short:"i" description:"(required) ID of the instance." required:"true" env:"CLOUDSDK_SPANNER_INSTANCE"`
	QueryMode            string            `long:"query-mode" description:"Query mode." default:"NORMAL" choice:"NORMAL" choice:"PLAN" choice:"PROFILE"`
	Format               string            `long:"format" description:"Output format." default:"json" choice:"json" choice:"yaml" choice:"experimental_csv"`
	RedactRows           bool              `long:"redact-rows" description:"Redact result rows from output"`
	CompactOutput        bool              `long:"compact-output" short:"c" description:"Compact JSON output(--compact-output of jq)"`
	JqFilter             string            `long:"filter" description:"jq filter"`
	JqRawOutput          bool              `long:"raw-output" short:"r" description:"(--raw-output of jq)"`
	JqFromFile           string            `long:"filter-file" description:"(--from-file of jq)"`
	Param                map[string]string `long:"param" description:"[name]:[Cloud Spanner type(PLAN only) or literal]"`
	LogGrpc              bool              `long:"log-grpc" description:"Show gRPC logs"`
	TraceProject         string            `long:"experimental-trace-project"`
	EnablePartitionedDML bool              `long:"enable-partitioned-dml" description:"Execute DML statement using Partitioned DML"`
	Timeout              time.Duration     `long:"timeout" default:"10m" description:"Maximum time to wait for the SQL query to complete"`
	TimestampBound       struct {
		Strong        bool   `long:"strong" description:"Perform a strong query."`
		ReadTimestamp string `long:"read-timestamp" description:"Perform a query at the given timestamp. (micro-seconds precision)" value-name:"TIMESTAMP"`
	} `group:"Timestamp Bound"`
}

func processFlags() (o opts, err error) {
	flagParser := flags.NewParser(&o, flags.Default)
	defer func() {
		if err == nil {
			return
		}
		if e, ok := err.(*flags.Error); ok && e.Type == flags.ErrHelp {
			return
		}
		log.Print(err)
		flagParser.WriteHelp(os.Stderr)
	}()
	_, err = flagParser.Parse()
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
		return o, errors.New("--jq-filter and --jq-from-file are exclusive")
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

func runInNewTransaction(ctx context.Context, client *spanner.Client, stmt spanner.Statement, opts spanner.QueryOptions, mode queryMode, reductRows bool) (*spannerpb.ResultSet, error) {
	var rs *spannerpb.ResultSet
	switch mode := mode.(type) {
	case readWrite:
		_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) (err error) {
			rs, err = consumeRowIter(tx.QueryWithOptions(ctx, stmt, opts), reductRows)
			return err
		})
		return rs, err
	case single:
		return consumeRowIter(client.Single().WithTimestampBound(mode.TimestampBound).QueryWithOptions(ctx, stmt, opts), reductRows)
	case partitionedDML:
		count, err := client.PartitionedUpdateWithOptions(ctx, stmt, opts)
		return &spannerpb.ResultSet{
			Metadata: &spannerpb.ResultSetMetadata{
				RowType: &spannerpb.StructType{},
			},
			Stats: &spannerpb.ResultSetStats{
				RowCount: &spannerpb.ResultSetStats_RowCountLowerBound{RowCountLowerBound: count},
			},
		}, err
	default:
		panic(fmt.Sprintf("unknown mode: %d", mode))
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

	// Use jqQuery even if empty filter
	jqFilter, err := readFileOrDefault(o.JqFromFile, o.JqFilter)
	if err != nil {
		return err
	}
	jqQuery, err := gojq.Parse(jqFilter)

	mode := spannerpb.ExecuteSqlRequest_QueryMode(spannerpb.ExecuteSqlRequest_QueryMode_value[o.QueryMode])

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
		octrace.DefaultTracer = oteloc.NewTracer(tp.Tracer("bridge"))

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

	client, err := newClient(ctx, o.Project, o.Instance, o.Positional.Database, logGrpc, doTrace)
	if err != nil {
		return err
	}
	defer client.Close()

	paramMap, err := generateParams(o.Param, mode == spannerpb.ExecuteSqlRequest_PLAN)
	if err != nil {
		return err
	}
	var m queryMode
	switch {
	case o.EnablePartitionedDML:
		m = partitionedDML{}
	case dmlRe.MatchString(query):
		m = readWrite{}
	case o.TimestampBound.ReadTimestamp != "":
		ts, err := time.Parse(time.RFC3339Nano, o.TimestampBound.ReadTimestamp)
		if err != nil {
			return err
		}
		m = single{spanner.ReadTimestamp(ts)}
	default:
		m = single{spanner.StrongRead()}
	}

	rs, err := runInNewTransaction(ctx, client, spanner.Statement{SQL: query, Params: paramMap}, spanner.QueryOptions{Mode: &mode}, m, o.RedactRows)
	if err != nil {
		return err
	}

	if o.Format == "experimental_csv" {
		return writeCsv(os.Stdout, rs)
	}

	object, err := toProtojsonObject(rs)
	if err != nil {
		return err
	}

	enc, err := newEncoder(os.Stdout, o.Format, o.CompactOutput, o.JqRawOutput)
	if err != nil {
		return err
	}

	return printResult(enc, jqQuery.Run(object))
}

func writeCsv(writer io.Writer, rs *spannerpb.ResultSet) error {
	var header []string
	fields := rs.GetMetadata().GetRowType().GetFields()
	for _, field := range fields {
		header = append(header, field.GetName())
	}

	var records [][]string
	for _, row := range rs.GetRows() {
		var record []string
		for i, value := range row.Values {
			s, err := gcvToStringExperimental(&spanner.GenericColumnValue{
				Type:  fields[i].GetType(),
				Value: value,
			})
			if err != nil {
				return err
			}
			record = append(record, s)
		}
		records = append(records, record)
	}
	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()
	err := csvWriter.Write(header)
	if err != nil {
		return err
	}
	err = csvWriter.WriteAll(records)
	if err != nil {
		return err
	}
	return nil
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

func toProtojsonObject(m proto.Message) (map[string]interface{}, error) {
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()

	var object map[string]interface{}
	err = dec.Decode(&object)
	if err != nil {
		return nil, err
	}
	return object, nil
}

type encoder interface{ Encode(v interface{}) error }

type stringPassThroughEncoderWrapper struct {
	Writer io.Writer
	Enc    encoder
}

func (enc *stringPassThroughEncoderWrapper) Encode(v interface{}) error {
	if s, ok := v.(string); ok {
		_, err := fmt.Fprintln(enc.Writer, s)
		return err
	}
	return enc.Enc.Encode(v)
}

func printResult(enc encoder, iter gojq.Iter) error {
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			return err
		}
		err := enc.Encode(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func newEncoder(writer io.Writer, format string, compactOutput bool, rawOutput bool) (encoder, error) {
	switch {
	case format == "yaml":
		return yaml.NewEncoder(writer), nil
	case format == "json":
		jsonenc := json.NewEncoder(writer)
		jsonenc.SetEscapeHTML(false)
		if !compactOutput {
			jsonenc.SetIndent("", "  ")
		}
		if rawOutput {
			return &stringPassThroughEncoderWrapper{Writer: writer, Enc: jsonenc}, nil
		} else {
			return jsonenc, nil
		}
	default:
		return nil, fmt.Errorf("unknown format: %s", format)
	}
}

func consumeRowIter(rowIter *spanner.RowIterator, redactRows bool) (*spannerpb.ResultSet, error) {
	consumeResult, err := consumeRowIterImpl(rowIter, redactRows)
	if err != nil {
		return nil, err
	}

	rs, err := convertToResultSet(consumeResult)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func convertToResultSet(consumeResult *consumeRowIterResult) (*spannerpb.ResultSet, error) {
	// Leave null if fields are not populated
	rs := &spannerpb.ResultSet{
		Rows:     consumeResult.Rows,
		Metadata: consumeResult.Metadata,
	}

	var queryStats *structpb.Struct
	if consumeResult.QueryStats != nil {
		qs, err := structpb.NewStruct(consumeResult.QueryStats)
		if err != nil {
			return nil, err
		}
		queryStats = qs
	}

	if consumeResult.QueryPlan != nil || queryStats != nil || consumeResult.RowCount != 0 {
		rs.Stats = &spannerpb.ResultSetStats{
			QueryPlan:  consumeResult.QueryPlan,
			QueryStats: queryStats,
		}
		if consumeResult.RowCount != 0 {
			rs.Stats.RowCount = &spannerpb.ResultSetStats_RowCountExact{RowCountExact: consumeResult.RowCount}
		}
	}

	return rs, nil
}

type consumeRowIterResult struct {
	Metadata   *spannerpb.ResultSetMetadata
	QueryPlan  *spannerpb.QueryPlan
	QueryStats map[string]interface{}
	RowCount   int64
	Rows       []*structpb.ListValue
}

func consumeRowIterImpl(rowIter *spanner.RowIterator, redactRows bool) (*consumeRowIterResult, error) {
	var rows []*structpb.ListValue
	err := rowIter.Do(func(r *spanner.Row) error {
		if redactRows {
			return nil
		}

		vs, err := rowValues(r)
		if err != nil {
			return err
		}

		rows = append(rows, &structpb.ListValue{Values: vs})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &consumeRowIterResult{
		Rows:       rows,
		QueryPlan:  rowIter.QueryPlan,
		QueryStats: rowIter.QueryStats,
		RowCount:   rowIter.RowCount,
		Metadata:   rowIter.Metadata,
	}, nil
}

func generateParams(ss map[string]string, permitType bool) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for name, code := range ss {
		if typ, err := parseType(code); permitType && err == nil {
			debuglog.Println(name, "ast.Type.SQL():", typ.SQL())
			value, err := astTypeToGenericColumnValue(typ)
			if err != nil {
				return nil, fmt.Errorf("error on processing param `%s`: %w", name, err)
			}

			debuglog.Println(name, "spannerpb.Type:", value.Type)
			result[name] = value
			continue
		} else if expr, err := parseExpr(code); err == nil {
			debuglog.Println(name, "ast.Expr.SQL():", expr.SQL())
			value, err := astExprToGenericColumnValue(expr)
			if err != nil {
				return nil, fmt.Errorf("error on processing param `%s`: %w", name, err)
			}

			debuglog.Println(name, "spannerpb.Type:", value.Type)
			result[name] = value
			continue
		} else {
			return nil, fmt.Errorf("error on parsing param `%s`: %w", name, err)
		}
	}
	return result, nil
}
