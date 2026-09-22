package jqresult

import (
	"strings"
	"testing"

	"github.com/wader/gojq"
)

type boomIter struct{}

func (boomIter) Next() (any, bool) { panic("unrelated boom") }

func TestPrintRethrowsUnrelatedPanic(t *testing.T) {
	defer func() {
		p := recover()
		if p == nil || !strings.Contains(p.(string), "unrelated boom") {
			t.Fatalf("panic = %#v, want unrelated boom", p)
		}
	}()
	_ = Print(nopEncoder{}, boomIter{})
}

type nopEncoder struct{}

func (nopEncoder) Encode(any) error { return nil }

var _ gojq.Iter = boomIter{}
