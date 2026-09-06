package jqresult

import (
	"errors"
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spaniter"
	"github.com/google/go-cmp/cmp"
	"github.com/wader/gojq"
)

func TestLazyFreshViewReplaysCachedPrefixThenLive(t *testing.T) {
	t.Parallel()

	l := newSyntheticLazy(t, 3)
	defer l.Stop()
	first, ok := l.rowsJQValue().(*lazyRowsField)
	if !ok {
		t.Fatal("first view")
	}
	v, ok := first.Next()
	if !ok {
		t.Fatal("expected first row")
	}
	if rowID(t, v) != 1 {
		t.Fatalf("first = %v, want 1", v)
	}

	fresh, ok := l.rowsJQValue().(*lazyRowsField)
	if !ok {
		t.Fatal("fresh view")
	}
	var ids []int64
	for {
		v, ok := fresh.Next()
		if !ok {
			break
		}
		if err, isErr := v.(error); isErr {
			t.Fatal(err)
		}
		ids = append(ids, rowID(t, v))
	}
	if diff := cmp.Diff([]int64{1, 2, 3}, ids); diff != "" {
		t.Fatalf("fresh view ids (-want +got)\n%s", diff)
	}
}

func TestLazyFirstThenLengthAndFullArray(t *testing.T) {
	t.Parallel()

	got := runLazyFilter(t, `{first: first(.rows[]), count: (.rows|length), rest: [.rows[]]}`)
	obj, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("got %T", got)
	}
	if rowID(t, obj["first"]) != 1 {
		t.Fatalf("first = %#v, want row 1", obj["first"])
	}
	if count, ok := obj["count"].(int); !ok || count != 3 {
		t.Fatalf("count = %#v (%T), want 3", obj["count"], obj["count"])
	}
	rest, ok := obj["rest"].([]any)
	if !ok {
		t.Fatalf("rest %T", obj["rest"])
	}
	if diff := cmp.Diff([]int64{1, 2, 3}, rowIDs(t, rest)); diff != "" {
		t.Fatalf("rest ids (-want +got)\n%s", diff)
	}
}

func TestLazyIndependentCapturedRowsViews(t *testing.T) {
	t.Parallel()

	got := runLazyFilter(t, `{a: [.rows[]], b: [.rows[]]}`)
	obj, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("got %T", got)
	}
	for _, key := range []string{"a", "b"} {
		rows, ok := obj[key].([]any)
		if !ok {
			t.Fatalf("%s %T", key, obj[key])
		}
		if diff := cmp.Diff([]int64{1, 2, 3}, rowIDs(t, rows)); diff != "" {
			t.Fatalf("%s ids (-want +got)\n%s", key, diff)
		}
	}
}

func TestLazyCapturedViewFirstThenLength(t *testing.T) {
	t.Parallel()

	got := runLazyFilter(t, `.rows as $r | {first: first($r[]), count: ($r|length)}`)
	obj, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("got %T", got)
	}
	if rowID(t, obj["first"]) != 1 {
		t.Fatalf("first = %#v, want row 1", obj["first"])
	}
	if count, ok := obj["count"].(int); !ok || count != 3 {
		t.Fatalf("count = %#v (%T), want 3", obj["count"], obj["count"])
	}
}

func TestLazyRedactRowsViews(t *testing.T) {
	t.Parallel()

	l := newSyntheticLazy(t, 3)
	l.redact = true
	l.rows.redact = true
	defer l.Stop()

	code, err := Compile("{n: (.rows|length), rows: [.rows[]]}", InputLazy)
	if err != nil {
		t.Fatal(err)
	}
	v, err := firstJQValue(t, code.Run(l))
	if err != nil {
		t.Fatal(err)
	}
	obj, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("got %T", v)
	}
	if count, ok := obj["n"].(int); !ok || count != 0 {
		t.Fatalf("redact length = %#v, want 0", obj["n"])
	}
	rows, ok := obj["rows"].([]any)
	if !ok || len(rows) != 0 {
		t.Fatalf("redact rows = %#v, want empty", obj["rows"])
	}
}

func TestLazyReplayLiveError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("row boom")
	i := 0
	l := &Lazy{
		metadataReady: true,
		encodeStats: func(spaniter.Stats) (map[string]any, error) {
			return map[string]any{"n": 1}, nil
		},
	}
	l.rows = &RowIter{
		seqActive: true,
		stopSeq:   func() {},
		ioMu:      &l.ioMu,
		pull: func() (*spanner.Row, error, bool) {
			i++
			if i == 1 {
				r, err := spanner.NewRow([]string{"id"}, []any{int64(1)})
				return r, err, true
			}
			return nil, wantErr, false
		},
		rowToJSON: RowToJSON,
	}
	defer l.Stop()

	first, ok := l.rowsJQValue().(*lazyRowsField)
	if !ok {
		t.Fatal("first view")
	}
	if _, ok := first.Next(); !ok {
		t.Fatal("expected first row")
	}

	fresh, ok := l.rowsJQValue().(*lazyRowsField)
	if !ok {
		t.Fatal("fresh view")
	}
	v, ok := fresh.Next()
	if !ok || rowID(t, v) != 1 {
		t.Fatalf("fresh prefix = %#v ok=%v, want row 1", v, ok)
	}
	v, ok = fresh.Next()
	if !ok {
		t.Fatal("expected error from live pull")
	}
	err, isErr := v.(error)
	if !isErr || !errors.Is(err, wantErr) {
		t.Fatalf("live error = %#v, want %v", v, wantErr)
	}
}

func runLazyFilter(t *testing.T, filter string) any {
	t.Helper()
	l := newSyntheticLazy(t, 3)
	defer l.Stop()
	code, err := Compile(filter, InputLazy)
	if err != nil {
		t.Fatal(err)
	}
	v, err := firstJQValue(t, code.Run(l))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func firstJQValue(t *testing.T, iter gojq.Iter) (any, error) {
	t.Helper()
	v, ok := iter.Next()
	if !ok {
		return nil, fmt.Errorf("no output")
	}
	if err, isErr := v.(error); isErr {
		return nil, err
	}
	return v, nil
}
