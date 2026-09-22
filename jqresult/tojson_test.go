package jqresult

import (
	"strings"
	"testing"
)

func TestFormatAliasesKeepNonJSONBehavior(t *testing.T) {
	t.Parallel()

	cases := []struct {
		filter string
		want   string
		isErr  bool
	}{
		{filter: `format("text")`, want: "hi"},
		{filter: `format("html")`, want: "hi"},
		{filter: `format(3)`, isErr: true},
		{filter: `def tojson: "user-defined"; tojson`, want: "user-defined"},
		{filter: `def format($f): "user-format"; format("json")`, want: "user-format"},
	}
	for _, tc := range cases {
		t.Run(tc.filter, func(t *testing.T) {
			code, err := Compile(tc.filter)
			if err != nil {
				t.Fatal(err)
			}
			v, ok := code.Run("hi").Next()
			if !ok {
				t.Fatal("no output")
			}
			if tc.isErr {
				if _, ok := v.(error); !ok {
					t.Fatalf("got %#v, want a format type error", v)
				}
				return
			}
			if err, ok := v.(error); ok {
				t.Fatal(err)
			}
			if !strings.Contains(anyString(v), tc.want) {
				t.Fatalf("got %#v, want %q", v, tc.want)
			}
		})
	}
}

func anyString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	default:
		return ""
	}
}
