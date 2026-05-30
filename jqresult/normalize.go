package jqresult

import (
	"encoding/json"
	"math/big"

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
		if isEncodeLeaf(v) {
			return v, nil
		}
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

// isEncodeLeaf reports values that cannot contain gojq.Iter and need no further normalization.
func isEncodeLeaf(v any) bool {
	switch v.(type) {
	case nil, bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, string,
		json.Number, *big.Int:
		return true
	default:
		return false
	}
}
