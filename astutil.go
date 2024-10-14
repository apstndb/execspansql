package main

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/cloudspannerecosystem/memefish/token"
	"google.golang.org/protobuf/types/known/structpb"
)

func astExprToGenericColumnValue(t ast.Expr) (spanner.GenericColumnValue, error) {
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
	case *ast.JSONLiteral:
		return spanner.GenericColumnValue{
			Type:  &spannerpb.Type{Code: spannerpb.TypeCode_JSON},
			Value: structpb.NewStringValue(t.Value.Value),
		}, nil
	case *ast.ArrayLiteral:
		var values []*structpb.Value
		var typ *spannerpb.Type
		for _, v := range t.Values {
			value, err := astExprToGenericColumnValue(v)
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
			value, err := astExprToGenericColumnValue(t.Values[0])
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
			genValue, err := astExprToGenericColumnValue(v)
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
					genType, err := astTypeToGenericColumnValue(field.Type)
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

func astTypeToGenericColumnValue(t ast.Type) (spanner.GenericColumnValue, error) {
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
	case ast.Float32TypeName:
		return &spannerpb.Type{Code: spannerpb.TypeCode_FLOAT32}, nil
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
	case ast.JSONTypeName:
		return &spannerpb.Type{Code: spannerpb.TypeCode_JSON}, nil
	default:
		return nil, fmt.Errorf("t.Name is unknown: %s", t.Name)
	}
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
	p := &memefish.Parser{
		Lexer: &memefish.Lexer{File: file},
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
	p := &memefish.Parser{
		Lexer: &memefish.Lexer{File: file},
	}
	return p.ParseType()
}
