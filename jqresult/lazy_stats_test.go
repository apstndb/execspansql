package jqresult

import (
	"encoding/json"
	"strconv"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spaniter"
)

func TestLazyRowsKeepsPositionAfterStatsDrain(t *testing.T) {
	t.Parallel()

	l := newSyntheticLazy(t, 3)
	defer l.Stop()
	f, ok := l.rowsJQValue().(*lazyRowsField)
	if !ok {
		t.Fatalf("rowsJQValue type %T", l.rowsJQValue())
	}

	var got []any
	for range 2 {
		v, ok := f.Next()
		if !ok {
			t.Fatal("expected a live row before stats drain")
		}
		if err, isErr := v.(error); isErr {
			t.Fatal(err)
		}
		got = append(got, v)
	}

	if _, err := l.statsMap(); err != nil {
		t.Fatal(err)
	}

	for {
		v, ok := f.Next()
		if !ok {
			break
		}
		if err, isErr := v.(error); isErr {
			t.Fatal(err)
		}
		got = append(got, v)
	}

	ids := rowIDs(t, got)
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("ids after stats drain mid-iteration: got %v, want [1 2 3]", ids)
	}
}

func TestLazyOmitQueryPlanLeavesQueryStats(t *testing.T) {
	t.Parallel()

	l := newSyntheticLazy(t, 2)
	l.omitQueryPlan = true
	l.encodeStats = func(spaniter.Stats) (map[string]any, error) {
		return map[string]any{
			"queryPlan":  map[string]any{"planNodes": []any{map[string]any{"displayName": "Scan"}}},
			"queryStats": map[string]any{"elapsed_time": "1 msecs"},
		}, nil
	}
	defer l.Stop()

	code, err := Compile(".stats", InputLazy)
	if err != nil {
		t.Fatal(err)
	}
	iter := code.Run(l)
	v, ok := iter.Next()
	if !ok {
		t.Fatal("no stats")
	}
	if err, isErr := v.(error); isErr {
		t.Fatal(err)
	}
	stats, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("stats type %T", v)
	}
	if _, ok := stats["queryPlan"]; ok {
		t.Fatalf("queryPlan present: %#v", stats)
	}
	qs, ok := stats["queryStats"].(map[string]any)
	if !ok || qs["elapsed_time"] != "1 msecs" {
		t.Fatalf("queryStats = %#v", stats["queryStats"])
	}
}

func TestLazyDrainDiscardsRemainingRows(t *testing.T) {
	t.Parallel()

	l := newSyntheticLazy(t, 5)
	defer l.Stop()
	f, ok := l.rowsJQValue().(*lazyRowsField)
	if !ok {
		t.Fatal("rows view")
	}
	if _, ok := f.Next(); !ok {
		t.Fatal("expected first row")
	}
	if err := l.Drain(); err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	n := len(l.materializedRows)
	l.mu.Unlock()
	if n != 1 {
		t.Fatalf("retained %d rows after Drain, want 1", n)
	}
}

func TestLazyDrainIsNoopAfterStats(t *testing.T) {
	t.Parallel()

	l := newSyntheticLazy(t, 3)
	defer l.Stop()
	if _, err := l.statsMap(); err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	before := len(l.materializedRows)
	l.mu.Unlock()
	if err := l.Drain(); err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	after := len(l.materializedRows)
	l.mu.Unlock()
	if after != before {
		t.Fatalf("Drain after stats changed retained rows %d -> %d", before, after)
	}
}

func TestLazyStatsInterleavedFilterIDs(t *testing.T) {
	t.Parallel()

	l := newSyntheticLazy(t, 3)
	defer l.Stop()
	code, err := Compile(`. as $root | [.rows[] | {row: ., stats: $root.stats}]`, InputLazy)
	if err != nil {
		t.Fatal(err)
	}
	iter := code.Run(l)
	v, ok := iter.Next()
	if !ok {
		t.Fatal("no output")
	}
	if err, isErr := v.(error); isErr {
		t.Fatal(err)
	}
	rows, ok := v.([]any)
	if !ok {
		t.Fatalf("got %T", v)
	}
	var ids []int64
	for _, item := range rows {
		obj, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("item %T", item)
		}
		ids = append(ids, rowID(t, obj["row"]))
	}
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("interleaved stats filter ids: got %v, want [1 2 3]", ids)
	}
}

func newSyntheticLazy(t *testing.T, n int) *Lazy {
	t.Helper()
	i := 0
	l := &Lazy{
		metadataReady: true,
		encodeStats: func(spaniter.Stats) (map[string]any, error) {
			return map[string]any{"n": n}, nil
		},
	}
	l.rows = &RowIter{
		seqActive: true,
		stopSeq:   func() {},
		ioMu:      &l.ioMu,
		pull: func() (*spanner.Row, error, bool) {
			i++
			if i > n {
				return nil, nil, false
			}
			r, err := spanner.NewRow([]string{"id"}, []any{int64(i)})
			return r, err, true
		},
		rowToJSON: RowToJSON,
	}
	return l
}

func rowIDs(t *testing.T, rows []any) []int64 {
	t.Helper()
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, rowID(t, row))
	}
	return ids
}

func rowID(t *testing.T, v any) int64 {
	t.Helper()
	row, ok := v.([]any)
	if !ok || len(row) != 1 {
		t.Fatalf("row %T %#v", v, v)
	}
	switch x := row[0].(type) {
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			t.Fatal(err)
		}
		return n
	case int64:
		return x
	default:
		t.Fatalf("id type %T %#v", x, x)
		return 0
	}
}
