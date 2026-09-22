package jqresult

import "github.com/wader/gojq"

// execspansqlToJSON is the body of the prelude's tojson. Materialization
// errors are returned as the original error so try/catch and the caller both
// see them. Plain values are encoded with gojq.Marshal, which keeps jq's
// number and string formatting.
func execspansqlToJSON(v any, _ []any) any {
	normalized, err := normalizeJQValue(v)
	if err != nil {
		return err
	}
	b, err := gojq.Marshal(normalized)
	if err != nil {
		return err
	}
	return string(b)
}

func normalizeJQValue(v any) (any, error) {
	if jqv, ok := v.(gojq.JQValue); ok {
		converted := jqv.JQValueToGoJQ()
		if err, ok := converted.(error); ok {
			return nil, err
		}
		return normalizeJQValue(converted)
	}
	switch x := v.(type) {
	case []any:
		out := make([]any, len(x))
		for i, elem := range x {
			n, err := normalizeJQValue(elem)
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, elem := range x {
			n, err := normalizeJQValue(elem)
			if err != nil {
				return nil, err
			}
			out[k] = n
		}
		return out, nil
	default:
		return v, nil
	}
}
