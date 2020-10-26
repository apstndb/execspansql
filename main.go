package main

import (
	"context"
	"io"

	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/goccy/go-json"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"google.golang.org/api/option"
	"google.golang.org/grpc"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap"

	"cloud.google.com/go/spanner"
	"github.com/itchyny/gojq"
	"github.com/jessevdk/go-flags"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func main() {
	if err := _main(); err != nil {
		log.Fatalln(err)
	}
}

var debuglog *log.Logger

func init() {
	if os.Getenv("DEBUG") != "" {
		debuglog = log.New(os.Stderr, "", log.LstdFlags)
	} else {
		debuglog = log.New(ioutil.Discard, "", log.LstdFlags)
	}
}

type opts struct {
	Positional struct {
		Database string `positional-arg-name:"database" description:"(required) ID of the database." required:"true"`
	} `positional-args:"yes"`
	Sql           string            `long:"sql" description:"SQL query text; exclusive with --sql-file."`
	SqlFile       string            `long:"sql-file" description:"File name contains SQL query; exclusive with --sql"`
	Project       string            `long:"project" short:"p" description:"(required) ID of the project." required:"true" env:"CLOUDSDK_CORE_PROJECT"`
	Instance      string            `long:"instance" short:"i" description:"(required) ID of the instance." required:"true" env:"CLOUDSDK_SPANNER_INSTANCE"`
	QueryMode     string            `long:"query-mode" description:"Query mode." default:"NORMAL" choice:"NORMAL" choice:"PLAN" choice:"PROFILE"`
	Format        string            `long:"format" description:"Output format." default:"json" choice:"json" choice:"yaml"`
	RedactRows    bool              `long:"redact-rows" description:"Redact result rows from output"`
	CompactOutput bool              `long:"compact-output" short:"c" description:"Compact JSON output(--compact-output of jq)"`
	JqFilter      string            `long:"filter" description:"jq filter"`
	JqRawOutput   bool              `long:"raw-output" short:"r" description:"(--raw-output of jq)"`
	JqFromFile    string            `long:"filter-file" description:"(--from-file of jq)"`
	Param         map[string]string `long:"param" description:"[name]:[Cloud Spanner type(PLAN only) or literal]"`
	LogGrpc       bool              `long:"log-grpc" description:"Show gRPC logs"`
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
		flagParser.WriteHelp(os.Stderr)
	}()
	_, err = flagParser.Parse()
	if err != nil {
		return o, err
	}

	if (o.Sql != "" && o.SqlFile != "") || (o.Sql == "" && o.SqlFile == "") {
		return o, err
	}

	if o.JqFilter != "" && o.JqFromFile != "" {
		fmt.Fprintln(os.Stderr, "--jq-filter and --jq-from-file are exclusive")
		return o, err
	}
	return o, nil
}

// readFileOrDefault returns content of filename or s if filename is empty
func readFileOrDefault(filename, s string) (string, error) {
	if filename == "" {
		return s, nil
	}
	b, err := ioutil.ReadFile(filename)
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
			grpc_zap.PayloadUnaryClientInterceptor(zapLogger, func(ctx context.Context, fullMethodName string) bool {
				return true
			}),
			grpc_zap.UnaryClientInterceptor(zapLogger),
		)),
		option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(
			grpc_zap.PayloadStreamClientInterceptor(zapLogger, func(ctx context.Context, fullMethodName string) bool {
				return true
			}),
			grpc_zap.StreamClientInterceptor(zapLogger),
		)),
	}
}

func _main() error {
	ctx := context.Background()

	o, err := processFlags()
	if err != nil {
		os.Exit(1)
	}

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

	logGrpc := o.LogGrpc

	client, err := newClient(ctx, o.Project, o.Instance, o.Positional.Database, logGrpc)
	if err != nil {
		return err
	}
	defer client.Close()

	paramMap, err := generateParams(o.Param, mode == spannerpb.ExecuteSqlRequest_PLAN)
	if err != nil {
		return err
	}

	rs, err := consumeRowIter(client.Single().QueryWithOptions(ctx, spanner.Statement{SQL: query, Params: paramMap}, spanner.QueryOptions{Mode: &mode}), o.RedactRows)
	if err != nil {
		return err
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

func newClient(ctx context.Context, project, instance, database string, logGrpc bool) (*spanner.Client, error) {
	name := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)

	var copts []option.ClientOption
	if logGrpc {
		copts = logGrpcClientOptions()
	}

	return spanner.NewClientWithConfig(ctx, name, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MaxOpened:           1,
			MinOpened:           1,
			WriteSessions:       0,
			TrackSessionHandles: true,
		},
	}, copts...)
}

func toProtojsonObject(m proto.Message) (map[string]interface{}, error) {
	var object map[string]interface{}
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &object)
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
	var metadata *spannerpb.ResultSetMetadata
	if consumeResult.RowType != nil {
		metadata = &spannerpb.ResultSetMetadata{
			RowType: &spannerpb.StructType{Fields: consumeResult.RowType},
		}
	}

	rs := &spannerpb.ResultSet{
		Rows:     consumeResult.Rows,
		Metadata: metadata,
	}

	var queryStats *structpb.Struct
	if consumeResult.QueryStats != nil {
		qs, err := structpb.NewStruct(consumeResult.QueryStats)
		if err != nil {
			return nil, err
		}
		queryStats = qs

	}

	if consumeResult.QueryPlan != nil || queryStats != nil {
		rs.Stats = &spannerpb.ResultSetStats{
			QueryPlan:  consumeResult.QueryPlan,
			QueryStats: queryStats,
		}
	}

	if consumeResult.RowCount != 0 {
		rs.Stats.RowCount = &spannerpb.ResultSetStats_RowCountExact{RowCountExact: consumeResult.RowCount}
	}
	return rs, nil
}

type consumeRowIterResult struct {
	RowType    []*spannerpb.StructType_Field
	Rows       []*structpb.ListValue
	QueryPlan  *spannerpb.QueryPlan
	QueryStats map[string]interface{}
	RowCount   int64
}

func consumeRowIterImpl(rowIter *spanner.RowIterator, redactRows bool) (*consumeRowIterResult, error) {
	var rowType []*spannerpb.StructType_Field
	var rows []*structpb.ListValue
	err := rowIter.Do(func(r *spanner.Row) error {
		var err error
		if rowType == nil {
			rowType, err = extractRowType(r)
			if err != nil {
				return err
			}
		}

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
		RowType:    rowType,
		Rows:       rows,
		QueryPlan:  rowIter.QueryPlan,
		QueryStats: rowIter.QueryStats,
		RowCount:   rowIter.RowCount,
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
