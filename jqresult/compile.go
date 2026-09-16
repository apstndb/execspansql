package jqresult

import (
	"github.com/wader/gojq"
)

// Compile parses filter and returns executable jq code.
func Compile(filter string) (*gojq.Code, error) {
	q, err := gojq.Parse(filter)
	if err != nil {
		return nil, err
	}
	return gojq.Compile(q)
}
