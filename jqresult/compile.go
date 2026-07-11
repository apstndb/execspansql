package jqresult

import (
	"github.com/wader/gojq"
)

// Compile parses filter and returns executable jq code.
// The mode parameter is retained for source compatibility; compilation is mode-independent.
func Compile(filter string, _ InputMode) (*gojq.Code, error) {
	q, err := gojq.Parse(filter)
	if err != nil {
		return nil, err
	}
	return gojq.Compile(q)
}
