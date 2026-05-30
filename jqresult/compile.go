package jqresult

import (
	"github.com/wader/gojq"
)

// Compile parses filter and returns executable jq code for the given input mode.
func Compile(filter string, mode InputMode) (*gojq.Code, error) {
	q, err := gojq.Parse(filter)
	if err != nil {
		return nil, err
	}
	return gojq.Compile(q)
}
