package jqresult

import (
	"github.com/wader/gojq"
)

// tojsonPrelude is parsed once and inserted before the user's function
// definitions, so a later user def tojson or def format still wins.
// The native format helper is bound first, so format("text") and format("html")
// keep the builtin. format("json"), tojson, and @json use the helper that
// returns materialization errors instead of panicking.
var tojsonPrelude = syncOncePrelude()

func syncOncePrelude() *gojq.Query {
	q, err := gojq.Parse(`def _execspansql_native_format($f): format($f); def tojson: _execspansql_tojson; def format($f): if $f == "json" then _execspansql_tojson else _execspansql_native_format($f) end;`)
	if err != nil {
		panic(err)
	}
	return q
}

// Compile parses filter and returns executable jq code.
func Compile(filter string) (*gojq.Code, error) {
	q, err := gojq.Parse(filter)
	if err != nil {
		return nil, err
	}
	q.FuncDefs = append(append([]*gojq.FuncDef{}, tojsonPrelude.FuncDefs...), q.FuncDefs...)
	return gojq.Compile(q, gojq.WithFunction("_execspansql_tojson", 0, 0, execspansqlToJSON))
}
