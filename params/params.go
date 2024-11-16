package params

import (
	"cloud.google.com/go/spanner"
	"fmt"
	"github.com/apstndb/execspansql/internal"
	"github.com/apstndb/go-spannulls"
	"github.com/apstndb/memebridge"
	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
)

// generateParams returns map for spanner.Statement.Params.
// All values of map are spanner.GenericColumnValue.
func GenerateParams(ss map[string]string, permitType bool) (map[string]interface{}, error) {
	return internal.TryMapMap(ss, func(k, v string) (interface{}, error) {
		value, err := generateParam(v, permitType)
		if err != nil {
			return nil, fmt.Errorf("error on %v: %w", k, err)
		}
		// Note: google-cloud-go supports only spanner.GenericColumnValue, not *spanner.GenericColumnValue.
		return value, nil
	})
}

func generateParam(code string, permitType bool) (spanner.GenericColumnValue, error) {
	var zeroGCV spanner.GenericColumnValue
	if permitType {
		typ, err := memefish.ParseType("", code)
		if err == nil {
			value, err := astTypeToGenericColumnValue(typ)
			if err != nil {
				return zeroGCV, fmt.Errorf("error on generating value for type: %w", err)
			}
			return value, err
		}
		// ignore parse err
	}

	expr, err := memefish.ParseExpr("", code)
	if err != nil {
		return zeroGCV, fmt.Errorf("error on parsing expr: %w", err)
	}

	value, err := memebridge.MemefishExprToGCV(expr)

	if err != nil {
		return zeroGCV, fmt.Errorf("error on generating value: %w", err)
	}

	return value, nil
}

func astTypeToGenericColumnValue(t ast.Type) (spanner.GenericColumnValue, error) {
	typ, err := memebridge.MemefishTypeToSpannerpbType(t)
	if err != nil {
		return spanner.GenericColumnValue{}, err
	}
	return spannulls.NullGenericColumnValueFromType(typ), nil
}
