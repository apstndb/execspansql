package jqresult

import (
	"github.com/wader/gojq"
)

// NormalizeForEncode walks v and expands gojq.Iter and gojq.JQValue so encoding/json or yaml can encode the result.
// jq-compatible containers ([]any, map[string]any) are recursed; other concrete values are left as leaves.
func NormalizeForEncode(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	if err, ok := v.(error); ok {
		return nil, err
	}
	switch v := v.(type) {
	case gojq.Iter:
		return normalizeIter(v)
	case gojq.JQValue:
		return NormalizeForEncode(v.JQValueToGoJQ())
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			n, err := NormalizeForEncode(e)
			if err != nil {
				return nil, err
			}
			out[i] = n
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, e := range v {
			n, err := NormalizeForEncode(e)
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

func normalizeIter(it gojq.Iter) ([]any, error) {
	out := make([]any, 0)
	for {
		x, ok := it.Next()
		if !ok {
			break
		}
		n, err := NormalizeForEncode(x)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}
