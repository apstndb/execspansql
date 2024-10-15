package main

import (
	"cloud.google.com/go/spanner"
	"fmt"
)

func generateParams(ss map[string]string, permitType bool) (map[string]interface{}, error) {
	return tryMapMap(ss, func(k, v string) (interface{}, error) {
		value, err := generateParam(v, permitType)
		if err != nil {
			return nil, fmt.Errorf("error on %v: %w", k, err)
		}
		return value, nil
	})
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
