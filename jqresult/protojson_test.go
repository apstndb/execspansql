package jqresult

import (
	"testing"

	"github.com/apstndb/spaniter"
)

func TestStatsMapFromStatsEmpty(t *testing.T) {
	t.Parallel()

	got, err := StatsMapFromStats(spaniter.Stats{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("stats map = %v, want nil", got)
	}
}

func TestStatsMapFromStatsQueryStats(t *testing.T) {
	t.Parallel()

	got, err := StatsMapFromStats(spaniter.Stats{QueryStats: map[string]any{}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("stats map = nil, want empty object")
	}
}

func TestStatsMapFromStatsDMLZeroRowCount(t *testing.T) {
	t.Parallel()

	got, err := StatsMapFromStats(spaniter.Stats{RowCount: 0}, true)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("stats map = nil, want rowCountExact")
	}
	if got["rowCountExact"] != "0" {
		t.Fatalf("rowCountExact = %v, want \"0\"", got["rowCountExact"])
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

	_, err := ResultSetMapFromRowIterator(nil, false, false)
	if err == nil {
		t.Fatal("error = nil, want nil row iterator error")
	}
}
