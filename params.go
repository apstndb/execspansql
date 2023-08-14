package main

import (
	"fmt"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/cloudspannerecosystem/memefish/ast"
	"google.golang.org/protobuf/types/known/structpb"
)

func generateParams(ss map[string]string, permitType bool) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for name, code := range ss {
		var err error
		var value spanner.GenericColumnValue
		var typ ast.Type
		var expr ast.Expr

		if typ, err = parseType(code); permitType && err == nil {
			debuglog.Println(name, "ast.Type.SQL():", typ.SQL())
			value, err = astTypeToGenericColumnValue(typ)
		} else if permitType && code == "JSON" {
			// workaround for JSON type(not literal)
			value = spanner.GenericColumnValue{Type: &spannerpb.Type{Code: spannerpb.TypeCode_JSON}, Value: structpb.NewNullValue()}
			// reset err
			err = nil
		} else if expr, err = parseExpr(code); err == nil {
			debuglog.Println(name, "ast.Expr.SQL():", expr.SQL())
			value, err = astExprToGenericColumnValue(expr)
		} else {
			return nil, fmt.Errorf("error on parsing param `%s`: %w", name, err)
		}

		if err != nil {
			return nil, fmt.Errorf("error on processing param `%s`: %w", name, err)
		}

		debuglog.Println(name, "spannerpb.Type:", value.Type)
		result[name] = value
	}
	return result, nil
}
