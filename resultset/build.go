package resultset

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spaniter"
	"google.golang.org/protobuf/types/known/structpb"
)

func statsEncoding(dmlRowCount bool) spaniter.StatsEncoding {
	if dmlRowCount {
		return spaniter.StatsEncodingDMLExact
	}
	return spaniter.StatsEncodingDefault
}

// FromIteratorResult constructs a ResultSet from materialized rows and drained
// iterator state. rows may be nil when row values are intentionally redacted.
// When dmlRowCount is true, stats use DML exact row-count encoding. PLAN mode
// callers must pass false.
func FromIteratorResult(rows []*structpb.ListValue, result spaniter.RowIteratorResult, dmlRowCount bool) (*sppb.ResultSet, error) {
	return result.ResultSet(rows, statsEncoding(dmlRowCount))
}
