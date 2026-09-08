package jqresult

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/execspansql/resultset"
	"reflect"
	"testing"

	"github.com/apstndb/spaniter"
)

func TestStatsMapFromResultEmpty(t *testing.T) {
	t.Parallel()

	got, err := StatsMapFromResult(spaniter.RowIteratorResult{})
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("stats map = %v, want nil", got)
	}
}

func TestStatsMapFromResultQueryStats(t *testing.T) {
	t.Parallel()

	got, err := StatsMapFromResult(spaniter.RowIteratorResult{
		Stats: spaniter.Stats{QueryStats: map[string]any{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("stats map = nil, want empty object")
	}
}

func TestMetadataMapFromMetadataNil(t *testing.T) {
	t.Parallel()

	got, err := MetadataMapFromMetadata(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("metadata map = %v, want nil", got)
	}
}

func TestResultSetMapFromRowIteratorNil(t *testing.T) {
	t.Parallel()

	_, err := ResultSetMapFromRowIterator(nil, false)
	if err == nil {
		t.Fatal("error = nil, want nil row iterator error")
	}
}

// Both eager result materialization and lazy stats conversion must preserve
// arbitrary plan/stat contents, independently of RPC query mode support.
func TestPlanAndStatsAcrossResultPaths(t *testing.T) {
	t.Parallel()
	for _, withPlan := range []bool{false, true} {
		for _, withStats := range []bool{false, true} {
			stats := spaniter.Stats{}
			want := map[string]any{}
			if withPlan {
				stats.QueryPlan = &sppb.QueryPlan{PlanNodes: []*sppb.PlanNode{{DisplayName: "Test Scan"}}}
				want["queryPlan"] = map[string]any{"planNodes": []any{map[string]any{"displayName": "Test Scan"}}}
			}
			if withStats {
				stats.QueryStats = map[string]any{"elapsed_time": "1 msecs", "nested": map[string]any{"complete": true}}
				want["queryStats"] = stats.QueryStats
			}
			input := spaniter.RowIteratorResult{Stats: stats}
			lazy, err := StatsMapFromResult(input)
			if err != nil {
				t.Fatal(err)
			}
			rs, err := resultset.FromIteratorResult(nil, input)
			if err != nil {
				t.Fatal(err)
			}
			eager, err := ResultSetMap(rs)
			if err != nil {
				t.Fatal(err)
			}
			if len(want) == 0 {
				if lazy != nil || eager["stats"] != nil {
					t.Fatalf("empty stats: lazy=%v eager=%v", lazy, eager)
				}
			} else if !reflect.DeepEqual(lazy, want) || !reflect.DeepEqual(eager["stats"], want) {
				t.Fatalf("plan=%v stats=%v: lazy=%v eager=%v want=%v", withPlan, withStats, lazy, eager["stats"], want)
			}
		}
	}
}
