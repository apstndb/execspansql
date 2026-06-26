package resultset

import (
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spaniter"
)

func TestMaterializeNilRowIterator(t *testing.T) {
	t.Parallel()

	_, err := Materialize(nil, false)
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

	got, err := FromIteratorResult(nil, spaniter.RowIteratorResult{})
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
	got, err := FromIteratorResult(nil, spaniter.RowIteratorResult{Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata != md {
		t.Fatal("metadata was not preserved")
	}
}
