package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"

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
	Project       string            `long:"project" description:"(required) ID of the project." required:"true" env:"CLOUDSDK_CORE_PROJECT"`
	Instance      string            `long:"instance" description:"(required) ID of the instance." required:"true" env:"CLOUDSDK_SPANNER_INSTANCE"`
	QueryMode     string            `long:"query-mode" description:"Query mode." default:"NORMAL" choice:"NORMAL" choice:"PLAN" choice:"PROFILE"`
	Format        string            `long:"format" description:"Output format." default:"json" choice:"json" choice:"yaml"`
	RedactRows    bool              `long:"redact-rows" description:"Redact result rows from output"`
	JqFilter      string            `long:"jq-filter" description:"jq filter"`
	CompactOutput bool              `long:"compact-output" description:"Compact JSON output(--compact-output of jq)"`
	JqRawOutput   bool              `long:"jq-raw-output" description:"(--raw-output of jq)"`
	JqFromFile    string            `long:"jq-from-file" description:"(--from-file of jq)"`
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
	var jqQuery *gojq.Query
	if o.JqFromFile != "" {
		b, err := ioutil.ReadFile(o.JqFromFile)
		if err != nil {
			return err
		}
		jq, err := gojq.Parse(string(b))
		if err != nil {
			return err
		}
		jqQuery = jq
	} else {
		jq, err := gojq.Parse(o.JqFilter)
		if err != nil {
			return err
		}
		jqQuery = jq
	}
	var mode spannerpb.ExecuteSqlRequest_QueryMode
	switch o.QueryMode {
	case "PLAN":
		mode = spannerpb.ExecuteSqlRequest_PLAN
	case "PROFILE":
		mode = spannerpb.ExecuteSqlRequest_PROFILE
	case "NORMAL":
		mode = spannerpb.ExecuteSqlRequest_NORMAL
	default:
		return fmt.Errorf("unknown query-mode: %s", o.QueryMode)
	}

	var query string
	switch {
	case o.Sql != "":
		query = o.Sql
	case o.SqlFile != "":
		if b, err := ioutil.ReadFile(o.SqlFile); err != nil {
			return err
		} else {
			query = string(b)
		}
	}

	var copts []option.ClientOption
	if o.LogGrpc {
		copts = logGrpcClientOptions()
	}

	name := fmt.Sprintf("projects/%s/instances/%s/databases/%s", o.Project, o.Instance, o.Positional.Database)
	client, err := spanner.NewClientWithConfig(ctx, name, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MaxOpened:           1,
			MinOpened:           1,
			WriteSessions:       0,
			TrackSessionHandles: true,
		},
	}, copts...)
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

	if jqQuery != nil {
		// input := map[string]interface{}{"foo": []interface{}{1, 2, 3}}
		var object map[string]interface{}
		b, err := protojson.Marshal(rs)
		if err != nil {
			return err
		}
		err = json.Unmarshal(b, &object)
		if err != nil {
			return err
		}

		iter := jqQuery.Run(object) // or query.RunWithContext

		yamlenc := yaml.NewEncoder(os.Stdout)
		jsonenc := json.NewEncoder(os.Stdout)
		if !o.CompactOutput {
			jsonenc.SetIndent("", "  ")
		}
		for {
			v, ok := iter.Next()
			if !ok {
				break
			}
			if err, ok := v.(error); ok {
				return err
			}
			switch o.Format {
			case "yaml":
				err := yamlenc.Encode(v)
				if err != nil {
					return err
				}
			case "json":
				if s, ok := v.(string); ok && o.JqRawOutput {
					fmt.Println(s)
				} else {
					err := jsonenc.Encode(v)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
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
				return nil, err
			}

			debuglog.Println(name, "spannerpb.Type:", value.Type)
			result[name] = value
			continue
		} else if expr, err := parseExpr(code); err == nil {
			debuglog.Println(name, "ast.Expr.SQL():", expr.SQL())
			value, err := astExprToGenericColumnValue(expr)
			if err != nil {
				return nil, err
			}

			debuglog.Println(name, "spannerpb.Type:", value.Type)
			result[name] = value
			continue
		} else {
			return nil, err
		}
	}
	return result, nil
}
