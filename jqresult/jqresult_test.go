package jqresult

import (
	"encoding/json"
	"testing"

	"github.com/wader/gojq"
)

type stubIter struct {
	vals []int
	i    int
}

func (s *stubIter) Next() (any, bool) {
	if s.i >= len(s.vals) {
		return nil, false
	}
	v := s.vals[s.i]
	s.i++
	return v, true
}

func TestNormalizeForEncodeIter(t *testing.T) {
	t.Parallel()
	var it gojq.Iter = &stubIter{vals: []int{1, 2}}
	got, err := NormalizeForEncode(it)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{1, 2}
	if len(got.([]any)) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestNormalizeForEncodeInterfaceMapWithIter(t *testing.T) {
	t.Parallel()
	it := &stubIter{vals: []int{1, 2}}
	got, err := NormalizeForEncode(map[string]interface{}{"rows": it})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(got)
	if string(b) != `{"rows":[1,2]}` {
		t.Fatalf("got %s", b)
	}
}

func TestNormalizeForEncodeLeafSlice(t *testing.T) {
	t.Parallel()
	got, err := NormalizeForEncode([]float64{1.5, 2.5})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.([]float64); !ok {
		t.Fatalf("got %T", got)
	}
}

func TestLazyRowsAfterStatsDrain(t *testing.T) {
	t.Parallel()
	l := &Lazy{
		metadata:         map[string]any{"c": 1},
		stats:            map[string]any{"n": 2},
		drained:          true,
		materializedRows: []any{[]any{int64(1)}, []any{int64(2)}},
	}
	q, err := gojq.Parse(".rows[]")
	if err != nil {
		t.Fatal(err)
	}
	iter := q.Run(l)
	var got []any
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, isErr := v.(error); isErr {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows want 2: %v", len(got), got)
	}
}

func TestLazyStopDoesNotDrain(t *testing.T) {
	t.Parallel()
	l := &Lazy{
		rows: &RowIter{stopped: true},
	}
	l.Stop()
	if l.drained {
		t.Fatal("Stop() must not drain rows")
	}
}

func TestLazyStatsKey(t *testing.T) {
	t.Parallel()
	l := &Lazy{
		metadata:      map[string]any{"c": 1},
		metadataReady: true,
		stats:         map[string]any{"n": 2},
		drained:       true,
	}
	q, err := gojq.Parse(".stats.n")
	if err != nil {
		t.Fatal(err)
	}
	iter := q.Run(l)
	v, ok := iter.Next()
	if !ok {
		t.Fatal("no output")
	}
	if v != 2 {
		t.Fatalf("got %v want 2", v)
	}
}

func TestDefaultFilter(t *testing.T) {
	t.Parallel()
	if DefaultFilter(InputEager) != "." {
		t.Fatal(InputEager)
	}
}

func TestParseInputMode(t *testing.T) {
	t.Parallel()
	if _, err := ParseInputMode("nope"); err == nil {
		t.Fatal("expected error")
	}
	if _, err := ParseInputMode("stream"); err == nil {
		t.Fatal("stream mode is not supported")
	}
}

type enc struct {
	vals []any
}

func (e *enc) Encode(v any) error {
	e.vals = append(e.vals, v)
	return nil
}

func TestPrintTopLevelIterJSONL(t *testing.T) {
	t.Parallel()
	e := &enc{}
	// jq emitted a single value that is itself a gojq.Iter (e.g. filter "." with Iter root input).
	out := gojq.NewIter[any](&stubIter{vals: []int{1, 2, 3}})
	if err := Print(e, out); err != nil {
		t.Fatal(err)
	}
	if len(e.vals) != 3 {
		t.Fatalf("got %d encodes want 3", len(e.vals))
	}
}
