package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"fmt"
	"log"
	"os"
	"os/signal"

	"encoding/json"

	"github.com/goccy/go-yaml"

	"google.golang.org/api/option"
	"google.golang.org/grpc"

	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/alecthomas/kong"
	"github.com/apstndb/execspansql/jqresult"
	"github.com/apstndb/execspansql/params"
	"github.com/apstndb/execspansql/resultset"
	"github.com/apstndb/gsqlutils/stmtkind"
	"github.com/apstndb/spaniter"
	"github.com/apstndb/spannerotel/interceptor"
	svwriter "github.com/apstndb/spanvalue/writer"
	"github.com/wader/gojq"
)

const (
	logGrpcModeOff      = "off"
	logGrpcModeMetadata = "metadata"
	logGrpcModePayload  = "payload"
)

func main() {
	if err := _main(); err != nil {
		log.Fatalln(err)
	}
}

type opts struct {
	Database             string        `arg:"" required:"" help:"ID or fully qualified resource name of the database."`
	Sql                  string        `name:"sql" xor:"sql" required:"" help:"SQL query text; exclusive with --sql-file."`
	SqlFile              string        `name:"sql-file" xor:"sql" required:"" help:"File name contains SQL query; exclusive with --sql"`
	Project              string        `name:"project" short:"p" env:"CLOUDSDK_CORE_PROJECT" help:"ID of the project; required for a database ID."`
	Instance             string        `name:"instance" short:"i" env:"CLOUDSDK_SPANNER_INSTANCE" help:"ID of the instance; required for a database ID."`
	DatabaseRole         string        `name:"database-role" help:"Database role to assume for all operations."`
	QueryMode            string        `name:"query-mode" enum:"NORMAL,PLAN,PROFILE,WITH_PLAN_AND_STATS,WITH_STATS" default:"NORMAL" help:"Query mode: NORMAL, PLAN, PROFILE, WITH_PLAN_AND_STATS, or WITH_STATS."`
	Priority             string        `name:"priority" enum:"high,low,medium,unspecified" default:"unspecified" help:"Priority for the execute SQL request."`
	Format               string        `name:"format" enum:"json,yaml,experimental_csv" default:"json" help:"Output format of the primary document."`
	Output               string        `name:"output" short:"o" default:"-" help:"Destination of the primary document. Use - for stdout; /dev/stdout and /dev/stderr are mapped in-process."`
	PlanOutput           string        `name:"plan-output" help:"Write the query-plan artifact here and strip stats.queryPlan from the primary document. Enables split mode."`
	PlanFormat           string        `name:"plan-format" help:"Format of the plan artifact: json, yaml, text, dot, mermaid, d2, svg, or png. Defaults to --format when that is json or yaml, otherwise json. Requires --plan-output."`
	PlanTextStyle        string        `name:"plan-text-style" help:"Text plan style: current, traditional, or compact. Defaults to current. Requires --plan-format=text." group:"Plan rendering"`
	PlanWrapWidth        int           `name:"plan-wrap-width" help:"Wrap width for text plans. 0 disables wrapping. Requires --plan-format=text." group:"Plan rendering"`
	PlanPrint            string        `name:"plan-print" help:"Text plan sections: basic, enhanced, full, none, or a comma-separated section list. Defaults to basic. Requires --plan-format=text." group:"Plan rendering"`
	PlanFull             bool          `name:"plan-full" help:"Include full graph node detail. Requires a graph --plan-format (dot, mermaid, d2, svg, png)." group:"Plan rendering"`
	PlanShowQuery        bool          `name:"plan-show-query" help:"Add a query-text node to graph output. Requires a graph --plan-format." group:"Plan rendering"`
	PlanShowQueryStats   bool          `name:"plan-show-query-stats" help:"Add query statistics to the query-text node. Requires a graph --plan-format." group:"Plan rendering"`
	DiscardResults       bool          `name:"discard-results" help:"Do not write the primary document (plan-only). Requires --plan-output."`
	RedactRows           bool          `name:"redact-rows" help:"Redact result rows from output"`
	CompactOutput        bool          `name:"compact-output" short:"c" help:"Compact JSON output (--compact-output of jq)"`
	JqFilter             string        `name:"filter" xor:"filter" help:"jq filter"`
	JqRawOutput          bool          `name:"raw-output" short:"r" help:"(--raw-output of jq)"`
	JqFromFile           string        `name:"filter-file" xor:"filter" help:"(--from-file of jq)"`
	JqInputMode          string        `name:"jq-input-mode" enum:"eager,lazy" default:"eager" help:"How query rows are passed to jq (json/yaml only): eager (full ResultSet), lazy (JQValue root)."`
	ParamFlags           []string      `name:"param" help:"[name]=[type or literal]; legacy [name]:[...] also accepted"`
	ParamFile            string        `name:"param-file" help:"YAML or JSON file of query parameters (name to type/literal string)"`
	LogGrpc              logGrpcFlag   `name:"log-grpc" enum:"off,metadata,payload" default:"off" help:"gRPC logging: --log-grpc means payload; use --log-grpc=off|metadata|payload to select a mode (payload may include request and response payloads)"`
	TraceProject         string        `name:"experimental-trace-project" xor:"trace" help:"Export traces to Cloud Trace in the given project."`
	TraceStdout          bool          `name:"experimental-trace-stdout" xor:"trace" help:"Export spans to stderr as pretty JSON (local debugging)."`
	TraceOTLP            bool          `name:"experimental-trace-otlp" xor:"trace" help:"Export spans via OTLP/gRPC to a local OpenTelemetry collector."`
	TraceOTLPEndpoint    string        `name:"experimental-trace-otlp-endpoint" default:"localhost:4317" help:"OTLP/gRPC endpoint used with --experimental-trace-otlp."`
	EnablePartitionedDML bool          `name:"enable-partitioned-dml" help:"Execute DML statement using Partitioned DML"`
	Timeout              time.Duration `name:"timeout" default:"10m" help:"Maximum time to wait for the SQL query to complete"`
	Reauth               string        `name:"reauth" enum:"off,auto" default:"off" env:"EXECSPANSQL_REAUTH" help:"When auto, run gcloud application-default login once if local user ADC needs reauthentication; off only prints a hint."`
	TryPartitionQuery    bool          `name:"try-partition-query" help:"(Experimental) Check whether the query can be executed as partition query or not"`
	TimestampBound       struct {
		Strong        bool   `name:"strong" xor:"timestamp" help:"Perform a strong query."`
		ReadTimestamp string `name:"read-timestamp" xor:"timestamp" help:"Perform a query at the given timestamp. (micro-seconds precision)"`
	} `embed:"" prefix:"" group:"Timestamp Bound"`
}

func (o opts) Validate() error {
	_, err := databaseResourceName(o.Project, o.Instance, o.Database)
	return err
}

func databaseResourceName(project, instance, database string) (string, error) {
	if strings.Contains(database, "/") {
		parts := strings.Split(database, "/")
		if len(parts) != 6 || parts[0] != "projects" || parts[1] == "" ||
			parts[2] != "instances" || parts[3] == "" || parts[4] != "databases" || parts[5] == "" {
			return "", fmt.Errorf("invalid database resource name %q; expected projects/PROJECT/instances/INSTANCE/databases/DATABASE", database)
		}
		// Like gcloud resource arguments, an explicit full name takes precedence
		// over project and instance flags or environment defaults.
		return database, nil
	}
	if database == "" {
		return "", errors.New("database ID is required")
	}
	if project == "" {
		return "", errors.New("--project is required when database is an ID")
	}
	if instance == "" {
		return "", errors.New("--instance is required when database is an ID")
	}
	if strings.ContainsAny(project+instance, "/") {
		return "", errors.New("--project and --instance must be IDs, not resource names")
	}
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database), nil
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
		kong.ExplicitGroups([]kong.Group{
			{Key: "Timestamp Bound", Title: "Timestamp Bound"},
		}),
	)
	if err != nil {
		return o, err
	}
	defer func() {
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
	}()
	ctx, err := parser.Parse(os.Args[1:])
	if err != nil {
		var parseErr *kong.ParseError
		if errors.As(err, &parseErr) {
			ctx = parseErr.Context
		}
		if ctx != nil {
			prev := parser.Stdout
			parser.Stdout = os.Stderr
			_ = ctx.PrintUsage(false)
			parser.Stdout = prev
		}
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

func parseTimestampBound(rawReadTimestamp string) (spanner.TimestampBound, error) {
	if rawReadTimestamp == "" {
		return spanner.StrongRead(), nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, rawReadTimestamp)
	if err != nil {
		return spanner.TimestampBound{}, err
	}
	return spanner.ReadTimestamp(parsed), nil
}

func stripLeadingComments(query string) string {
	for {
		query = strings.TrimLeft(query, " \t\r\n")
		if query == "" {
			return ""
		}

		switch {
		case strings.HasPrefix(query, "--"):
			if i := strings.IndexAny(query[2:], "\r\n"); i >= 0 {
				query = query[2+i+1:]
				continue
			}
			return ""
		case strings.HasPrefix(query, "#"):
			if i := strings.IndexAny(query[1:], "\r\n"); i >= 0 {
				query = query[1+i+1:]
				continue
			}
			return ""
		case strings.HasPrefix(query, "/*"):
			if i := strings.Index(query[2:], "*/"); i >= 0 {
				query = query[i+4:]
				continue
			}
			return ""
		default:
			return query
		}
	}
}

func isReadWriteStatement(query string) bool {
	return stmtkind.IsDMLLexical(stripLeadingComments(query))
}

func queryModeForQuery(query string, enablePartitionedDML bool, tb spanner.TimestampBound) queryMode {
	if isReadWriteStatement(query) {
		if enablePartitionedDML {
			return partitionedDML{}
		}
		return readWrite{}
	}
	return single{tb}
}

func validateExecutionOptions(o opts, mode queryMode) error {
	if err := validatePlanOutputOptions(o); err != nil {
		return err
	}
	if o.TryPartitionQuery {
		if _, ok := mode.(single); !ok {
			if o.EnablePartitionedDML {
				return fmt.Errorf("--try-partition-query cannot be combined with --enable-partitioned-dml")
			}
			return fmt.Errorf("--try-partition-query cannot be used with DML statements")
		}
		if o.Priority != "" && o.Priority != "unspecified" {
			// The v1.90.0 client only puts QueryOptions.Priority on ExecuteSqlRequest
			// objects returned for later partition execution. This probe only calls
			// PartitionQuery, whose request has no priority field, so accepting the
			// flag here would silently drop it.
			return fmt.Errorf("--priority cannot be used with --try-partition-query")
		}
	}
	if o.TimestampBound.ReadTimestamp != "" || o.TimestampBound.Strong {
		if _, ok := mode.(single); !ok {
			flagName := "--read-timestamp"
			if o.TimestampBound.Strong {
				flagName = "--strong"
			}
			if o.EnablePartitionedDML {
				return fmt.Errorf("%s cannot be combined with --enable-partitioned-dml", flagName)
			}
			return fmt.Errorf("%s cannot be used with DML statements", flagName)
		}
	}
	if o.EnablePartitionedDML {
		if _, ok := mode.(partitionedDML); !ok {
			return fmt.Errorf("--enable-partitioned-dml can only be used with DML statements")
		}
		// PartitionedUpdateWithOptions does not copy QueryOptions.Mode. Every
		// non-NORMAL query mode would therefore execute writes instead of
		// returning its requested plan and/or statistics.
		switch o.QueryMode {
		case "PLAN", "PROFILE", "WITH_PLAN_AND_STATS", "WITH_STATS":
			return fmt.Errorf("--query-mode=%s cannot be combined with --enable-partitioned-dml", o.QueryMode)
		}
	}
	if _, ok := mode.(partitionedDML); ok && o.JqInputMode == "lazy" {
		return fmt.Errorf("--jq-input-mode=lazy is not supported for partitioned DML")
	}
	return nil
}

func validateJqOutputOptions(o opts, mode jqresult.InputMode) error {
	if o.Format == "experimental_csv" {
		if o.JqFilter != "" || o.JqFromFile != "" || o.JqRawOutput || o.CompactOutput || mode == jqresult.InputLazy {
			return fmt.Errorf("--format=experimental_csv does not support jq filtering options")
		}
		return nil
	}

	if o.TryPartitionQuery {
		if o.JqFilter != "" || o.JqFromFile != "" || o.JqRawOutput || o.CompactOutput || mode == jqresult.InputLazy {
			return fmt.Errorf("--try-partition-query does not support jq filtering options")
		}
	}
	if o.Format != "json" && (o.JqRawOutput || o.CompactOutput) {
		return fmt.Errorf("--raw-output and --compact-output are only supported with --format=json")
	}
	return nil
}

func buildGrpcZapLogger(config zap.Config) *zap.Logger {
	zapLogger, err := config.Build()
	if err != nil {
		return zap.NewNop()
	}
	return zapLogger
}

func logGrpcClientOptions(logGrpcMode string) []option.ClientOption {
	zapDevelopmentConfig := zap.NewDevelopmentConfig()
	zapDevelopmentConfig.DisableCaller = true
	zapLogger := buildGrpcZapLogger(zapDevelopmentConfig)

	switch logGrpcMode {
	case logGrpcModeMetadata:
		return []option.ClientOption{
			option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(
				grpczap.UnaryClientInterceptor(zapLogger),
			)),
			option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(
				grpczap.StreamClientInterceptor(zapLogger),
			)),
		}
	case logGrpcModePayload:
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
	default:
		return nil
	}
}

type queryMode interface{ isQueryMode() }

type single struct{ spanner.TimestampBound }
type readWrite struct{}
type partitionedDML struct{}

func (s single) isQueryMode()         {}
func (r readWrite) isQueryMode()      {}
func (p partitionedDML) isQueryMode() {}

func queryOptionsFor(mode sppb.ExecuteSqlRequest_QueryMode, priority string) spanner.QueryOptions {
	priorities := map[string]sppb.RequestOptions_Priority{
		"high":        sppb.RequestOptions_PRIORITY_HIGH,
		"low":         sppb.RequestOptions_PRIORITY_LOW,
		"medium":      sppb.RequestOptions_PRIORITY_MEDIUM,
		"unspecified": sppb.RequestOptions_PRIORITY_UNSPECIFIED,
	}
	return spanner.QueryOptions{Mode: &mode, Priority: priorities[priority]}
}

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

func _main() error {
	return runCLI()
}

// runCLI accepts client options so transport tests can inspect outgoing RPCs.
// Non-empty clientOptions skip the ADC reauth preflight (tests inject insecure
// dial options that bypass application-default credentials).
func runCLI(clientOptions ...option.ClientOption) (err error) {
	o, err := processFlags()
	if err != nil {
		os.Exit(1)
	}
	defer func() { err = wrapWithHint(err) }()

	// The first interrupt cancels ctx so an in-progress gcloud login or query
	// unwinds cleanly; stop() then restores default signal handling so a
	// second interrupt still terminates the process if shutdown hangs.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
	}()

	jqMode, err := jqresult.ParseInputMode(o.JqInputMode)
	if err != nil {
		return err
	}
	if err := jqMode.ValidateFormat(o.Format); err != nil {
		return err
	}
	if err := validateJqOutputOptions(o, jqMode); err != nil {
		return err
	}

	var (
		jqCode *gojq.Code
	)
	if !o.TryPartitionQuery && o.Format != "experimental_csv" {
		jqFilter, err := readFileOrDefault(o.JqFromFile, o.JqFilter)
		if err != nil {
			return err
		}
		if jqFilter == "" {
			jqFilter = jqresult.DefaultFilter(jqMode)
		}

		jqCode, err = jqresult.Compile(jqFilter, jqMode)
		if err != nil {
			return err
		}
	}

	mode := sppb.ExecuteSqlRequest_QueryMode(sppb.ExecuteSqlRequest_QueryMode_value[o.QueryMode])
	queryOpts := queryOptionsFor(mode, o.Priority)

	query, err := readFileOrDefault(o.SqlFile, o.Sql)
	if err != nil {
		return err
	}
	o.Sql = query

	tb, err := parseTimestampBound(o.TimestampBound.ReadTimestamp)
	if err != nil {
		return fmt.Errorf("--read-timestamp is supplied but wrong: %w", err)
	}

	m := queryModeForQuery(query, o.EnablePartitionedDML, tb)
	if err := validateExecutionOptions(o, m); err != nil {
		return err
	}

	// Freeze the statement (SQL and parameters) before any interactive step so
	// a parameter file edited during a browser login cannot change what runs.
	paramStrMap, err := o.mergedParams()
	if err != nil {
		return err
	}
	paramMap, err := params.GenerateParams(paramStrMap, mode == sppb.ExecuteSqlRequest_PLAN)
	if err != nil {
		return err
	}
	stmt := spanner.Statement{SQL: query, Params: paramMap}

	sinks, err := newOutputSinks(o)
	if err != nil {
		return err
	}
	defer sinks.Abort()

	authOpts, err := maybeAuthPreflight(ctx, o, clientOptions, newReauthHooks())
	if err != nil {
		return err
	}

	// Query execution timeout starts after authentication. Login is
	// human-paced and must not consume --timeout.
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()

	ctx, tp, err := enableTracing(ctx, o)
	if err != nil {
		return err
	}
	if tp != nil {
		defer func() {
			if err := shutdownTracing(context.Background(), tp); err != nil {
				log.Printf("trace provider shutdown: %v", err)
			}
		}()
	}

	client, err := newClient(ctx, o.Project, o.Instance, o.Database, o.DatabaseRole, string(o.LogGrpc), tracingEnabled(o), append(clientOptions, authOpts...)...)
	if err != nil {
		return err
	}
	defer client.Close()

	if o.TryPartitionQuery {
		bt, err := client.BatchReadOnlyTransaction(ctx, tb)
		if err != nil {
			return err
		}
		defer bt.Close()
		defer func() { bt.Cleanup(ctx) }()

		_, err = bt.PartitionQuery(ctx, stmt, spanner.PartitionOptions{})
		if err != nil {
			return err
		}

		if sinks.primary != nil {
			if _, err := fmt.Fprintln(sinks.primary, "success"); err != nil {
				return err
			}
		}
		sinks.MarkPrimaryComplete()
		return sinks.Finish(nil)
	}

	var workErr error
	if o.Format == "experimental_csv" {
		workErr = runAndWriteCsv(ctx, client, stmt, queryOpts, m, o, sinks)
	} else {
		workErr = runJqOutput(ctx, client, stmt, queryOpts, m, o, jqMode, jqCode, sinks)
	}
	finishErr := sinks.Finish(workErr)
	if finishErr != nil && workErr == nil && isCommittedMode(m) {
		// The statement completed (a read-write transaction committed or a
		// partitioned DML finished) and only file publication failed.
		// Say so explicitly so nobody replays the DML to repair an output file.
		return wrapCommittedOutputError(finishErr)
	}
	return finishErr
}

// isCommittedMode reports whether a successful run of mode leaves a committed
// write behind, which changes how later output failures must be described.
func isCommittedMode(mode queryMode) bool {
	switch mode.(type) {
	case readWrite, partitionedDML:
		return true
	default:
		return false
	}
}

// materializeWithoutRows reports whether the eager path may drop row values
// while materializing: both --redact-rows and --discard-results never emit
// rows, so reading them into memory would only cost time and memory.
func materializeWithoutRows(o opts) bool {
	return o.RedactRows || o.DiscardResults
}

func runAndWriteCsv(ctx context.Context, client *spanner.Client, stmt spanner.Statement, opts spanner.QueryOptions, mode queryMode, o opts, sinks *outputSinks) error {
	encodeRowCount := dmlRowCountForMode(mode, opts)
	statOpts := spaniterStatsOpts(mode, opts)
	planFmt := effectivePlanFormat(o)
	writePlanFromCSV := func(result *svwriter.RowIteratorResult) error {
		if sinks.plan == nil {
			return nil
		}
		stats, err := statsFromWriterResult(result, encodeRowCount)
		if err != nil {
			return err
		}
		var md *sppb.ResultSetMetadata
		if result != nil {
			md = result.Metadata
		}
		return writePlan(ctx, sinks.plan, planFmt, md, stats, o)
	}
	writePlanFromDrain := func(result *spaniter.RowIteratorResult) error {
		if sinks.plan == nil {
			return nil
		}
		if result == nil {
			return errNoQueryPlan
		}
		stats, err := result.StatsProto()
		if err != nil {
			return err
		}
		return writePlan(ctx, sinks.plan, planFmt, result.Metadata, stats, o)
	}

	switch mode := mode.(type) {
	case readWrite:
		var buf bytes.Buffer
		var csvResult *svwriter.RowIteratorResult
		var drainResult *spaniter.RowIteratorResult
		_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			buf.Reset()
			rowIter := tx.QueryWithOptions(ctx, stmt, opts)
			if o.DiscardResults {
				var err error
				drainResult, err = spaniter.DrainRowIterator(rowIter, statOpts...)
				return err
			}
			var err error
			csvResult, err = writeCsvFromRowIter(&buf, rowIter, o.RedactRows)
			return err
		})
		if err != nil {
			return err
		}
		if sinks.primary != nil {
			if _, err := io.Copy(sinks.primary, &buf); err != nil {
				return wrapCommittedOutputError(err)
			}
		}
		sinks.MarkPrimaryComplete()
		var planErr error
		if o.DiscardResults {
			planErr = writePlanFromDrain(drainResult)
		} else {
			planErr = writePlanFromCSV(csvResult)
		}
		if planErr != nil {
			return wrapCommittedOutputError(planErr)
		}
		return nil
	case single:
		rowIter := client.Single().WithTimestampBound(mode.TimestampBound).QueryWithOptions(ctx, stmt, opts)
		if o.DiscardResults {
			result, err := spaniter.DrainRowIterator(rowIter, statOpts...)
			if err != nil {
				return err
			}
			sinks.MarkPrimaryComplete()
			return writePlanFromDrain(result)
		}
		writer := sinks.primary
		if writer == nil {
			writer = io.Discard
		}
		result, err := writeCsvFromRowIter(writer, rowIter, o.RedactRows)
		if err != nil {
			return err
		}
		sinks.MarkPrimaryComplete()
		return writePlanFromCSV(result)
	case partitionedDML:
		count, err := client.PartitionedUpdateWithOptions(ctx, stmt, opts)
		if err != nil {
			return err
		}
		rs := &sppb.ResultSet{
			Metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{}},
			Stats: &sppb.ResultSetStats{
				RowCount: &sppb.ResultSetStats_RowCountLowerBound{RowCountLowerBound: count},
			},
		}
		if sinks.primary != nil {
			if err := writeCsvFromResultSet(sinks.primary, rs); err != nil {
				return wrapCommittedOutputError(err)
			}
		}
		sinks.MarkPrimaryComplete()
		return nil
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
func writeCsvFromRowIter(writer io.Writer, rowIter *spanner.RowIterator, redactRows bool) (*svwriter.RowIteratorResult, error) {
	csvWriter, err := svwriter.NewCSVWriter(writer)
	if err != nil {
		return nil, err
	}
	iterWriter := svwriter.RowIteratorWriter(csvWriter)
	if redactRows {
		iterWriter = csvRedactRowIteratorWriter{csvWriter}
	}
	return svwriter.WriteRowIterator(rowIter, iterWriter)
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

func newClient(ctx context.Context, project, instance, database, databaseRole string, logGrpcMode string, doTrace bool, clientOptions ...option.ClientOption) (*spanner.Client, error) {
	name, err := databaseResourceName(project, instance, database)
	if err != nil {
		return nil, err
	}

	var copts []option.ClientOption
	if logGrpcMode != logGrpcModeOff {
		copts = logGrpcClientOptions(logGrpcMode)
	}

	if doTrace {
		copts = append(copts, option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(interceptor.StreamInterceptor(interceptor.WithDefaultDecorators()))))
	}
	return spanner.NewClientWithConfig(ctx, name, spanner.ClientConfig{DatabaseRole: databaseRole}, append(copts, clientOptions...)...)
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

func (enc *stringPassThroughEncoderWrapper) Close() error {
	return closeEncoder(enc.Enc)
}

func closeEncoder(enc encoder) error {
	if closer, ok := enc.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
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
	sinks *outputSinks,
) error {
	committed := isCommittedMode(mode)
	wrap := func(err error) error {
		if err != nil && committed {
			return wrapCommittedOutputError(err)
		}
		return err
	}
	useEager := jqMode == jqresult.InputEager
	// Read-write DML always materializes the full result set before jq runs.
	if _, ok := mode.(readWrite); ok {
		useEager = true
	}
	planFmt := effectivePlanFormat(o)
	if useEager {
		rs, err := runInNewTransaction(ctx, client, stmt, opts, mode, materializeWithoutRows(o))
		if err != nil {
			return err
		}
		var planStats *sppb.ResultSetStats
		var metadata *sppb.ResultSetMetadata
		if sinks.hasPlan {
			planStats, metadata = stripQueryPlanForPrimary(rs)
		} else if rs != nil {
			metadata = rs.Metadata
			planStats = rs.Stats
		}
		if sinks.primary != nil {
			enc, err := newEncoder(sinks.primary, o.Format, o.CompactOutput, o.JqRawOutput)
			if err != nil {
				return wrap(err)
			}
			iter, cleanup, err := jqresult.Execute(jqCode, jqresult.InputEager, nil, rs, o.RedactRows)
			if err != nil {
				_ = closeEncoder(enc)
				return wrap(err)
			}
			printErr := jqresult.Print(enc, iter)
			cleanup()
			closeErr := closeEncoder(enc)
			if printErr != nil {
				return wrap(printErr)
			}
			if closeErr != nil {
				return wrap(closeErr)
			}
		}
		sinks.MarkPrimaryComplete()
		if sinks.plan != nil {
			return wrap(writePlan(ctx, sinks.plan, planFmt, metadata, planStats, o))
		}
		return nil
	}

	switch mode := mode.(type) {
	case single:
		rowIter := client.Single().WithTimestampBound(mode.TimestampBound).QueryWithOptions(ctx, stmt, opts)
		if o.DiscardResults {
			result, err := spaniter.DrainRowIterator(rowIter, spaniterStatsOpts(mode, opts)...)
			if err != nil {
				return err
			}
			sinks.MarkPrimaryComplete()
			if sinks.plan == nil {
				return nil
			}
			stats, err := result.StatsProto()
			if err != nil {
				return err
			}
			return writePlan(ctx, sinks.plan, planFmt, result.Metadata, stats, o)
		}
		writer := sinks.primary
		if writer == nil {
			writer = io.Discard
		}
		enc, err := newEncoder(writer, o.Format, o.CompactOutput, o.JqRawOutput)
		if err != nil {
			return err
		}
		return runJqOnRowIter(ctx, rowIter, o.RedactRows, jqCode, enc, sinks, planFmt, o)
	case partitionedDML:
		return fmt.Errorf("--jq-input-mode=lazy is not supported for partitioned DML")
	default:
		panic(fmt.Sprintf("unknown mode: %T", mode))
	}
}

func runJqOnRowIter(
	ctx context.Context,
	rowIter *spanner.RowIterator,
	redactRows bool,
	jqCode *gojq.Code,
	enc encoder,
	sinks *outputSinks,
	planFmt string,
	o opts,
) error {
	var lazyOpts []jqresult.LazyOption
	if sinks.hasPlan {
		lazyOpts = append(lazyOpts, jqresult.WithOmitQueryPlan())
	}
	lazy := jqresult.NewLazy(rowIter, redactRows, lazyOpts...)
	defer lazy.Stop()
	printErr := jqresult.Print(enc, jqCode.Run(lazy))
	closeErr := closeEncoder(enc)
	if printErr != nil {
		return printErr
	}
	if closeErr != nil {
		return closeErr
	}
	sinks.MarkPrimaryComplete()
	if sinks.plan == nil {
		return nil
	}
	if err := lazy.Drain(); err != nil {
		return err
	}
	result := lazy.Result()
	stats, err := result.StatsProto()
	if err != nil {
		return err
	}
	return writePlan(ctx, sinks.plan, planFmt, result.Metadata, stats, o)
}

func newEncoder(writer io.Writer, format string, compactOutput bool, rawOutput bool) (encoder, error) {
	switch format {
	case "yaml":
		return yaml.NewEncoder(writer, yaml.Indent(4)), nil
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
