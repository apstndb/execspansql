package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/apstndb/execspansql/internal/protoyaml"
	"gopkg.in/yaml.v2"

	"cloud.google.com/go/spanner"
	"github.com/MakeNowJust/memefish/pkg/ast"
	"github.com/MakeNowJust/memefish/pkg/parser"
	"github.com/MakeNowJust/memefish/pkg/token"
	"github.com/itchyny/gojq"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

type stringList []string

func (s *stringList) String() string {
	return fmt.Sprint(*s)
}

func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

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

func _main() error {
	ctx := context.Background()
	sql := flag.String("sql", "", "SQL query text; exclusive with --file.")
	file := flag.String("file", "", "File name contains SQL query; exclusive with --sql")
	project := flag.String("project", "", "(required) ID of the project.")
	instance := flag.String("instance", "", "(required) ID of the instance.")
	database := flag.String("database", "", "(required) ID of the database.")
	queryMode := flag.String("query-mode", "", "Query mode; possible values(case-insensitive): NORMAL, PLAN, PROFILE; default=PLAN")
	format := flag.String("format", "", "Output format; possible values(case-insensitive): json, json-compact, yaml; default=json")
	redactRows := flag.Bool("redact-rows", false, "Redact result rows from output")
	jqFilter := flag.String("jq-filter", "", "jq filter")
	compactOutput := flag.Bool("compact-output", false, "Compact JSON output(--compact-output of jq)")
	jqRawOutput := flag.Bool("jq-raw-output", false, "(--raw-output of jq)")
	jqFromFile := flag.String("jq-from-file", "", "(--from-file of jq)")

	var params stringList
	flag.Var(&params, "param", "[name]=[Cloud Spanner type(PLAN only) or literal]")

	flag.Parse()

	if *project == "" || *instance == "" || *database == "" {
		flag.Usage()
		os.Exit(1)
	}

	var query string
	switch {
	case *sql != "" && *file != "":
		flag.Usage()
		os.Exit(1)
	case *sql != "":
		query = *sql
	case *file != "":
		if b, err := ioutil.ReadFile(*file); err != nil {
			return err
		} else {
			query = string(b)
		}
	default:
		flag.Usage()
		os.Exit(1)
	}

	if *jqFilter != "" && *jqFromFile != "" {
		fmt.Fprintln(os.Stderr, "--jq-filter and --jq-from-file are exclusive")
		flag.Usage()
		os.Exit(1)
	}

	var jqQuery *gojq.Query
	if *jqFilter != "" {
		var err error
		jqQuery, err = gojq.Parse(*jqFilter)
		if err != nil {
			return err
		}
	}
	if *jqFromFile != "" {
		b, err := ioutil.ReadFile(*jqFromFile)
		if err != nil {
			return err
		}
		jqQuery, err = gojq.Parse(string(b))
		if err != nil {
			return err
		}
	}
	var mode spannerpb.ExecuteSqlRequest_QueryMode
	switch strings.ToUpper(*queryMode) {
	// default is PLAN
	case "PLAN", "":
		mode = spannerpb.ExecuteSqlRequest_PLAN
	case "PROFILE":
		mode = spannerpb.ExecuteSqlRequest_PROFILE
	case "NORMAL":
		mode = spannerpb.ExecuteSqlRequest_NORMAL
	default:
		fmt.Fprintln(os.Stderr, "unknown query-mode:", *queryMode)
		flag.Usage()
		os.Exit(1)
	}

	switch strings.ToLower(*format) {
	case "":
		*format = "json"
	case "json", "yaml":
	default:
		debuglog.Println("unknown format:", *format)
		flag.Usage()
		os.Exit(1)
	}

	name := fmt.Sprintf("projects/%s/instances/%s/databases/%s", *project, *instance, *database)
	client, err := spanner.NewClientWithConfig(ctx, name, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MaxOpened:           1,
			MinOpened:           1,
			WriteSessions:       0,
			TrackSessionHandles: true,
		},
	},
	)
	if err != nil {
		return err
	}
	defer client.Close()

	paramMap, err := generateParams(params, mode == spannerpb.ExecuteSqlRequest_PLAN)
	if err != nil {
		return err
	}

	rowIter := client.Single().QueryWithOptions(ctx, spanner.Statement{SQL: query, Params: paramMap}, spanner.QueryOptions{
		Mode: &mode,
	})
	var rowType []*spannerpb.StructType_Field
	var rows []*structpb.ListValue
	err = rowIter.Do(func(r *spanner.Row) error {
		isFirst := rowType == nil
		if !isFirst && *redactRows {
			return nil
		}
		var row []*structpb.Value
		for i, name := range r.ColumnNames() {
			var v spanner.GenericColumnValue
			err := r.Column(i, &v)
			if err != nil {
				return err
			}
			row = append(row, v.Value)
			if isFirst {
				rowType = append(rowType, &spannerpb.StructType_Field{
					Name: name,
					Type: v.Type,
				})
			}
		}
		if !*redactRows {
			rows = append(rows, &structpb.ListValue{Values: row})
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Leave null if fields are not populated
	var metadata *spannerpb.ResultSetMetadata
	if rowType != nil {
		metadata = &spannerpb.ResultSetMetadata{
			RowType: &spannerpb.StructType{Fields: rowType},
		}
	}

	rs := &spannerpb.ResultSet{
		Rows:     rows,
		Metadata: metadata,
	}

	var queryStats *structpb.Struct
	if rowIter.QueryStats != nil {
		queryStats, err = structpb.NewStruct(rowIter.QueryStats)
		if err != nil {
			return err
		}
	}

	if rowIter.QueryPlan != nil || queryStats != nil {
		rs.Stats = &spannerpb.ResultSetStats{
			QueryPlan:  rowIter.QueryPlan,
			QueryStats: queryStats,
		}
	}

	if rowIter.RowCount != 0 {
		rs.Stats.RowCount = &spannerpb.ResultSetStats_RowCountExact{RowCountExact: rowIter.RowCount}
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
		if !*compactOutput {
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
			switch *format {
			case "yaml":
				err := yamlenc.Encode(v)
				if err != nil {
					return err
				}
			case "json":
				if s, ok := v.(string); ok && *jqRawOutput {
					fmt.Println(s)
				} else {
					err := jsonenc.Encode(v)
					if err != nil {
						return err
					}
				}
			}
		}
	} else {
		var str string
		switch *format {
		case "json", "":
			if *compactOutput {
				b, err := protojson.Marshal(rs)
				if err != nil {
					return err
				}
				str = string(b) + "\n"
			} else {
				str = protojson.Format(rs) + "\n"
			}
		case "yaml":
			b, err := protoyaml.Marshal(rs)
			if err != nil {
				return err
			}
			str = string(b)
		}
		fmt.Print(str)
	}
	return nil
}

func parseExpr(s string) (expr ast.Expr, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recover from panic: %v", r)
		}
	}()
	file := &token.File{
		Buffer: s,
	}
	p := &parser.Parser{
		Lexer: &parser.Lexer{File: file},
	}
	return p.ParseExpr()
}

func parseType(s string) (typ ast.Type, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recover from panic: %v", r)
		}
	}()
	file := &token.File{
		Buffer: s,
	}
	p := &parser.Parser{
		Lexer: &parser.Lexer{File: file},
	}
	return p.ParseType()
}

func generateParams(ss []string, permitType bool) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for _, s := range ss {
		split := strings.SplitN(s, "=", 2)
		name := split[0]
		code := split[1]
		if typ, err := parseType(code); permitType && err == nil {
			debuglog.Println(name, "ast.Type.SQL():", typ.SQL())
			value, err := typeToGenericColumnValue(typ)
			if err != nil {
				return nil, err
			}

			debuglog.Println(name, "spannerpb.Type:", value.Type)
			result[name] = value
			continue
		} else if expr, err := parseExpr(code); err == nil {
			debuglog.Println(name, "ast.Expr.SQL():", expr.SQL())
			value, err := exprToGenericColumnValue(expr)
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

func exprToGenericColumnValue(t ast.Expr) (spanner.GenericColumnValue, error) {
	switch t := t.(type) {
	case *ast.NullLiteral:
		return spanner.GenericColumnValue{
			Type:  &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
			Value: structpb.NewNullValue(),
		}, nil
	case *ast.BoolLiteral:
		return spanner.GenericColumnValue{
			Type:  &spannerpb.Type{Code: spannerpb.TypeCode_BOOL},
			Value: structpb.NewBoolValue(t.Value),
		}, nil
	case *ast.IntLiteral:
		i, err := strconv.ParseInt(t.Value, t.Base, 64)
		if err != nil {
			return spanner.GenericColumnValue{}, err
		}
		return spanner.GenericColumnValue{
			Type:  &spannerpb.Type{Code: spannerpb.TypeCode_INT64},
			Value: structpb.NewStringValue(strconv.FormatInt(i, 10)),
		}, nil
	case *ast.FloatLiteral:
		i, err := strconv.ParseFloat(t.Value, 64)
		if err != nil {
			return spanner.GenericColumnValue{}, err
		}
		return spanner.GenericColumnValue{
			Type:  &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT64},
			Value: structpb.NewNumberValue(i),
		}, nil
	case *ast.StringLiteral:
		return spanner.GenericColumnValue{
			Type:  &spannerpb.Type{Code: spannerpb.TypeCode_STRING},
			Value: structpb.NewStringValue(t.Value),
		}, nil
	case *ast.BytesLiteral:
		return spanner.GenericColumnValue{
			Type:  &spannerpb.Type{Code: spannerpb.TypeCode_BYTES},
			Value: structpb.NewStringValue(base64.StdEncoding.EncodeToString(t.Value)),
		}, nil
	case *ast.DateLiteral:
		return spanner.GenericColumnValue{
			Type:  &spannerpb.Type{Code: spannerpb.TypeCode_DATE},
			Value: structpb.NewStringValue(t.Value.Value),
		}, nil
	case *ast.TimestampLiteral:
		return spanner.GenericColumnValue{
			Type:  &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP},
			Value: structpb.NewStringValue(t.Value.Value),
		}, nil
	case *ast.NumericLiteral:
		return spanner.GenericColumnValue{
			Type:  &spannerpb.Type{Code: spannerpb.TypeCode_NUMERIC},
			Value: structpb.NewStringValue(t.Value.Value),
		}, nil
	case *ast.ArrayLiteral:
		var values []*structpb.Value
		var typ *spannerpb.Type
		for _, v := range t.Values {
			value, err := exprToGenericColumnValue(v)
			if err != nil {
				return spanner.GenericColumnValue{}, err
			}
			values = append(values, value.Value)
		}
		if t.Type != nil {
			var err error
			typ, err = astTypeToSpannerpbType(t.Type)
			if err != nil {
				return spanner.GenericColumnValue{}, err
			}
		} else if len(t.Values) > 0 {
			value, err := exprToGenericColumnValue(t.Values[0])
			if err != nil {
				return spanner.GenericColumnValue{}, err
			}
			typ = value.Type
		}
		return spanner.GenericColumnValue{
			Type:  &spannerpb.Type{ArrayElementType: typ, Code: spannerpb.TypeCode_ARRAY},
			Value: structpb.NewListValue(&structpb.ListValue{Values: values}),
		}, nil
	case *ast.StructLiteral:
		var fields []*spannerpb.StructType_Field
		var values []*structpb.Value
		for i, v := range t.Values {
			genValue, err := exprToGenericColumnValue(v)
			if err != nil {
				return spanner.GenericColumnValue{}, err
			}
			var name string
			var typ *spannerpb.Type
			if len(t.Fields) > i {
				field := t.Fields[i]
				if field.Ident != nil {
					name = field.Ident.Name
				}
				if field.Type != nil {
					genType, err := typeToGenericColumnValue(field.Type)
					if err != nil {
						return spanner.GenericColumnValue{}, err
					}
					typ = genType.Type
				}
			}
			if typ == nil {
				typ = genValue.Type
			}
			fields = append(fields, &spannerpb.StructType_Field{
				Name: name,
				Type: typ,
			})
			values = append(values, genValue.Value)
		}
		return spanner.GenericColumnValue{
			Type: &spannerpb.Type{
				StructType: &spannerpb.StructType{Fields: fields},
				Code:       spannerpb.TypeCode_STRUCT},
			Value: structpb.NewListValue(&structpb.ListValue{Values: values}),
		}, nil
	default:
		return spanner.GenericColumnValue{}, fmt.Errorf("not implemented: %s", t.SQL())
	}
}

func typeToGenericColumnValue(t ast.Type) (spanner.GenericColumnValue, error) {
	typ, err := astTypeToSpannerpbType(t)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}
	v, err := valueFromSpannerpbType(typ)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}
	return spanner.GenericColumnValue{Type: typ, Value: v}, nil
}

func astTypeToSpannerpbType(t ast.Type) (*spannerpb.Type, error) {
	switch t := t.(type) {
	case *ast.SimpleType:
		return astSimpleTypeToSpannerpbType(t)
	case *ast.ArrayType:
		var typ *spannerpb.Type
		if t.Item != nil {
			var err error
			typ, err = astTypeToSpannerpbType(t.Item)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("t is unknown")
		}
		return &spannerpb.Type{ArrayElementType: typ, Code: spannerpb.TypeCode_ARRAY}, nil
	case *ast.StructType:
		var fields []*spannerpb.StructType_Field
		for _, f := range t.Fields {
			t, err := astTypeToSpannerpbType(f.Type)
			if err != nil {
				return nil, err
			}
			var name string
			if f.Ident != nil {
				name = f.Ident.Name
			}
			fields = append(fields, &spannerpb.StructType_Field{
				Name: name,
				Type: t,
			})
		}
		return &spannerpb.Type{StructType: &spannerpb.StructType{Fields: fields}, Code: spannerpb.TypeCode_STRUCT}, nil
	default:
		return nil, fmt.Errorf("not implemented: %s", t.SQL())
	}
}

func astSimpleTypeToSpannerpbType(t *ast.SimpleType) (*spannerpb.Type, error) {
	if t == nil {
		return nil, fmt.Errorf("t is nil")
	}
	switch t.Name {
	case ast.BoolTypeName:
		return &spannerpb.Type{Code: spannerpb.TypeCode_BOOL}, nil
	case ast.Int64TypeName:
		return &spannerpb.Type{Code: spannerpb.TypeCode_INT64}, nil
	case ast.Float64TypeName:
		return &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT64}, nil
	case ast.StringTypeName:
		return &spannerpb.Type{Code: spannerpb.TypeCode_STRING}, nil
	case ast.BytesTypeName:
		return &spannerpb.Type{Code: spannerpb.TypeCode_BYTES}, nil
	case ast.DateTypeName:
		return &spannerpb.Type{Code: spannerpb.TypeCode_DATE}, nil
	case ast.TimestampTypeName:
		return &spannerpb.Type{Code: spannerpb.TypeCode_TIMESTAMP}, nil
	case ast.NumericTypeName:
		return &spannerpb.Type{Code: spannerpb.TypeCode_NUMERIC}, nil
	default:
		return nil, fmt.Errorf("t.Name is unknown: %s", t.Name)
	}
}

func valueFromSpannerpbType(typ *spannerpb.Type) (*structpb.Value, error) {
	switch typ.GetCode() {
	case spannerpb.TypeCode_BOOL:
		return structpb.NewBoolValue(false), nil
	case spannerpb.TypeCode_INT64:
		return structpb.NewStringValue("0"), nil
	case spannerpb.TypeCode_FLOAT64:
		return structpb.NewNumberValue(0), nil
	case spannerpb.TypeCode_STRING:
		return structpb.NewStringValue(""), nil
	case spannerpb.TypeCode_BYTES:
		return structpb.NewStringValue(""), nil
	case spannerpb.TypeCode_DATE:
		return structpb.NewStringValue("1970-01-01"), nil
	case spannerpb.TypeCode_TIMESTAMP:
		return structpb.NewStringValue("1970-01-01T00:00:00Z"), nil
	case spannerpb.TypeCode_NUMERIC:
		return structpb.NewStringValue("0"), nil
	case spannerpb.TypeCode_ARRAY:
		return structpb.NewListValue(&structpb.ListValue{}), nil
	case spannerpb.TypeCode_STRUCT:
		var values []*structpb.Value
		for _, f := range typ.StructType.GetFields() {
			v, err := valueFromSpannerpbType(f.GetType())
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
		return structpb.NewListValue(&structpb.ListValue{Values: values}), nil
	default:
		return structpb.NewNullValue(), nil
	}
}
