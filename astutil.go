package main

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/cloudspannerecosystem/memefish/token"
	"google.golang.org/protobuf/types/known/structpb"
)

func astExprToGenericColumnValue(t ast.Expr) (*spanner.GenericColumnValue, error) {
	switch t := t.(type) {
	case *ast.NullLiteral:
		return &spanner.GenericColumnValue{
			Type:  &sppb.Type{Code: sppb.TypeCode_INT64},
			Value: structpb.NewNullValue(),
		}, nil
	case *ast.BoolLiteral:
		return &spanner.GenericColumnValue{
			Type:  &sppb.Type{Code: sppb.TypeCode_BOOL},
			Value: structpb.NewBoolValue(t.Value),
		}, nil
	case *ast.IntLiteral:
		i, err := strconv.ParseInt(t.Value, t.Base, 64)
		if err != nil {
			return nil, err
		}
		return &spanner.GenericColumnValue{
			Type:  &sppb.Type{Code: sppb.TypeCode_INT64},
			Value: structpb.NewStringValue(strconv.FormatInt(i, 10)),
		}, nil
	case *ast.FloatLiteral:
		i, err := strconv.ParseFloat(t.Value, 64)
		if err != nil {
			return nil, err
		}
		return &spanner.GenericColumnValue{
			Type:  &sppb.Type{Code: sppb.TypeCode_FLOAT64},
			Value: structpb.NewNumberValue(i),
		}, nil
	case *ast.StringLiteral:
		return &spanner.GenericColumnValue{
			Type:  &sppb.Type{Code: sppb.TypeCode_STRING},
			Value: structpb.NewStringValue(t.Value),
		}, nil
	case *ast.BytesLiteral:
		return &spanner.GenericColumnValue{
			Type:  &sppb.Type{Code: sppb.TypeCode_BYTES},
			Value: structpb.NewStringValue(base64.StdEncoding.EncodeToString(t.Value)),
		}, nil
	case *ast.DateLiteral:
		return &spanner.GenericColumnValue{
			Type:  &sppb.Type{Code: sppb.TypeCode_DATE},
			Value: structpb.NewStringValue(t.Value.Value),
		}, nil
	case *ast.TimestampLiteral:
		return &spanner.GenericColumnValue{
			Type:  &sppb.Type{Code: sppb.TypeCode_TIMESTAMP},
			Value: structpb.NewStringValue(t.Value.Value),
		}, nil
	case *ast.NumericLiteral:
		return &spanner.GenericColumnValue{
			Type:  &sppb.Type{Code: sppb.TypeCode_NUMERIC},
			Value: structpb.NewStringValue(t.Value.Value),
		}, nil
	case *ast.JSONLiteral:
		return &spanner.GenericColumnValue{
			Type:  &sppb.Type{Code: sppb.TypeCode_JSON},
			Value: structpb.NewStringValue(t.Value.Value),
		}, nil
	case *ast.ArrayLiteral:
		var values []*structpb.Value
		var typ *sppb.Type
		for _, v := range t.Values {
			value, err := astExprToGenericColumnValue(v)
			if err != nil {
				return nil, err
			}
			values = append(values, value.Value)
		}
		if t.Type != nil {
			var err error
			typ, err = astTypeToSpannerpbType(t.Type)
			if err != nil {
				return nil, err
			}
		} else if len(t.Values) > 0 {
			value, err := astExprToGenericColumnValue(t.Values[0])
			if err != nil {
				return nil, err
			}
			typ = value.Type
		}
		return &spanner.GenericColumnValue{
			Type:  &sppb.Type{ArrayElementType: typ, Code: sppb.TypeCode_ARRAY},
			Value: structpb.NewListValue(&structpb.ListValue{Values: values}),
		}, nil
	case *ast.StructLiteral:
		var fields []*sppb.StructType_Field
		var values []*structpb.Value
		for i, v := range t.Values {
			genValue, err := astExprToGenericColumnValue(v)
			if err != nil {
				return nil, err
			}
			var name string
			var typ *sppb.Type
			if len(t.Fields) > i {
				field := t.Fields[i]
				if field.Ident != nil {
					name = field.Ident.Name
				}
				if field.Type != nil {
					genType, err := astTypeToGenericColumnValue(field.Type)
					if err != nil {
						return nil, err
					}
					typ = genType.Type
				}
			}
			if typ == nil {
				typ = genValue.Type
			}
			fields = append(fields, &sppb.StructType_Field{
				Name: name,
				Type: typ,
			})
			values = append(values, genValue.Value)
		}
		return &spanner.GenericColumnValue{
			Type: &sppb.Type{
				StructType: &sppb.StructType{Fields: fields},
				Code:       sppb.TypeCode_STRUCT},
			Value: structpb.NewListValue(&structpb.ListValue{Values: values}),
		}, nil
	default:
		return nil, fmt.Errorf("not implemented: %s", t.SQL())
	}
}

func astTypeToGenericColumnValue(t ast.Type) (*spanner.GenericColumnValue, error) {
	typ, err := astTypeToSpannerpbType(t)
	if err != nil {
		return nil, err
	}
	return &spanner.GenericColumnValue{Type: typ, Value: valueFromSpannerpbType(typ)}, nil
}

func astTypeToSpannerpbType(t ast.Type) (*sppb.Type, error) {
	switch t := t.(type) {
	case *ast.SimpleType:
		return astSimpleTypeToSpannerpbType(t)
	case *ast.ArrayType:
		var typ *sppb.Type
		if t.Item != nil {
			var err error
			typ, err = astTypeToSpannerpbType(t.Item)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("t is unknown")
		}
		return &sppb.Type{ArrayElementType: typ, Code: sppb.TypeCode_ARRAY}, nil
	case *ast.StructType:
		var fields []*sppb.StructType_Field
		for _, f := range t.Fields {
			t, err := astTypeToSpannerpbType(f.Type)
			if err != nil {
				return nil, err
			}
			var name string
			if f.Ident != nil {
				name = f.Ident.Name
			}
			fields = append(fields, &sppb.StructType_Field{
				Name: name,
				Type: t,
			})
		}
		return &sppb.Type{StructType: &sppb.StructType{Fields: fields}, Code: sppb.TypeCode_STRUCT}, nil
	default:
		return nil, fmt.Errorf("not implemented: %s", t.SQL())
	}
}

func astSimpleTypeToSpannerpbType(t *ast.SimpleType) (*sppb.Type, error) {
	if t == nil {
		return nil, fmt.Errorf("t is nil")
	}
	switch t.Name {
	case ast.BoolTypeName:
		return &sppb.Type{Code: sppb.TypeCode_BOOL}, nil
	case ast.Int64TypeName:
		return &sppb.Type{Code: sppb.TypeCode_INT64}, nil
	case ast.Float64TypeName:
		return &sppb.Type{Code: sppb.TypeCode_FLOAT64}, nil
	case ast.Float32TypeName:
		return &sppb.Type{Code: sppb.TypeCode_FLOAT32}, nil
	case ast.StringTypeName:
		return &sppb.Type{Code: sppb.TypeCode_STRING}, nil
	case ast.BytesTypeName:
		return &sppb.Type{Code: sppb.TypeCode_BYTES}, nil
	case ast.DateTypeName:
		return &sppb.Type{Code: sppb.TypeCode_DATE}, nil
	case ast.TimestampTypeName:
		return &sppb.Type{Code: sppb.TypeCode_TIMESTAMP}, nil
	case ast.NumericTypeName:
		return &sppb.Type{Code: sppb.TypeCode_NUMERIC}, nil
	case ast.JSONTypeName:
		return &sppb.Type{Code: sppb.TypeCode_JSON}, nil
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
