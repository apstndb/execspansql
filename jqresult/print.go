package jqresult

import "github.com/wader/gojq"

// Encoder writes one jq emission (JSON or YAML document).
type Encoder interface {
	Encode(v any) error
}

// Print writes all values from a jq iterator.
// Top-level gojq.Iter values are expanded to one Encode per element (JSONL-friendly).
// Other values are normalized (including nested Iter) then encoded once.
func Print(enc Encoder, iter gojq.Iter) error {
	for {
		v, ok := iter.Next()
		if !ok {
			return nil
		}
		if err, ok := v.(error); ok {
			return err
		}
		if err := encodeOne(enc, v, true); err != nil {
			return err
		}
	}
}

func encodeOne(enc Encoder, v any, topLevel bool) error {
	if topLevel {
		if it, ok := v.(gojq.Iter); ok {
			for {
				x, ok := it.Next()
				if !ok {
					return nil
				}
				if err, ok := x.(error); ok {
					return err
				}
				if err := encodeOne(enc, x, false); err != nil {
					return err
				}
			}
		}
	}
	n, err := NormalizeForEncode(v)
	if err != nil {
		return err
	}
	return enc.Encode(n)
}
