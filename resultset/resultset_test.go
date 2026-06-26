package resultset

import (
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spaniter"
)

func TestMaterializeNilRowIterator(t *testing.T) {
	t.Parallel()

	_, err := Materialize(nil, false, false)
	if err == nil {
		t.Fatal("error = nil, want nil row iterator error")
	}
}

func TestCollectListValuesNilRowIterator(t *testing.T) {
	t.Parallel()

	rows, err := CollectListValues(nil)
	if err == nil {
		t.Fatal("error = nil, want error from nil row iterator")
	}
	if rows != nil {
		t.Fatalf("rows = %v, want nil", rows)
	}
}

func TestFromIteratorResultEmptyStats(t *testing.T) {
	t.Parallel()

	got, err := FromIteratorResult(nil, spaniter.RowIteratorResult{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Stats != nil {
		t.Fatalf("Stats = %v, want nil", got.Stats)
	}
}

func TestFromIteratorResultPreservesMetadata(t *testing.T) {
	t.Parallel()

	md := &sppb.ResultSetMetadata{}
	got, err := FromIteratorResult(nil, spaniter.RowIteratorResult{Metadata: md}, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata != md {
		t.Fatal("metadata was not preserved")
	}
}

func TestFromIteratorResultDMLZeroRowCount(t *testing.T) {
	t.Parallel()

	got, err := FromIteratorResult(nil, spaniter.RowIteratorResult{
		Stats: spaniter.Stats{RowCount: 0},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if got.Stats == nil {
		t.Fatal("Stats = nil, want DML zero row count stats")
	}
	exact, ok := got.Stats.GetRowCount().(*sppb.ResultSetStats_RowCountExact)
	if !ok {
		t.Fatalf("RowCount type = %T, want RowCountExact", got.Stats.GetRowCount())
	}
	if exact.RowCountExact != 0 {
		t.Fatalf("RowCountExact = %d, want 0", exact.RowCountExact)
	}
}

func TestFromIteratorResultQueryZeroRowCountOmitsStats(t *testing.T) {
	t.Parallel()

	got, err := FromIteratorResult(nil, spaniter.RowIteratorResult{
		Stats: spaniter.Stats{RowCount: 0},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Stats != nil {
		t.Fatalf("Stats = %v, want nil for query zero row count", got.Stats)
	}
}
