package jqresult

import (
	"testing"
)

func TestExecuteLazyNilRowIter(t *testing.T) {
	t.Parallel()

	code, err := Compile(".", InputLazy)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = Execute(code, InputLazy, nil, nil, false, false, false)
	if err == nil {
		t.Fatal("expected error for nil rowIter in lazy mode")
	}
}

func TestExecuteEagerNilResultSet(t *testing.T) {
	t.Parallel()

	code, err := Compile(".", InputEager)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = Execute(code, InputEager, nil, nil, false, false, false)
	if err == nil {
		t.Fatal("expected error for nil ResultSet in eager mode")
	}
}
