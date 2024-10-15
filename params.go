package main

import (
	"fmt"

	"cloud.google.com/go/spanner"
)

func generateParams(ss map[string]string, permitType bool) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for name, code := range ss {
		value, err := generateParam(code, permitType)
		if err != nil {
			return nil, fmt.Errorf("error on %v: %w", name, err)
		}
		result[name] = value
	}
	return result, nil
}

func generateParam(code string, permitType bool) (*spanner.GenericColumnValue, error) {
	if permitType {
		typ, err := parseType(code)
		if err == nil {
			debuglog.Println("ast.Type.SQL():", typ.SQL())
			value, err := astTypeToGenericColumnValue(typ)
			if err != nil {
				return nil, fmt.Errorf("error on generating value for type: %w", err)
			}
			return value, err
		}
		// ignore parse err
		debuglog.Println("ignore parse error:", err)
	}

	expr, err := parseExpr(code)
	if err != nil {
		return nil, fmt.Errorf("error on parsing expr: %w", err)
	}

	debuglog.Println("ast.Expr.SQL():", expr.SQL())

	value, err := astExprToGenericColumnValue(expr)

	if err != nil {
		return nil, fmt.Errorf("error on generating value: %w", err)
	}

	debuglog.Println("spannerpb.Type:", value.Type)
	return value, nil
}
