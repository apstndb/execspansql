package resultset

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spaniter"
	"google.golang.org/protobuf/types/known/structpb"
)

// FromIteratorResult constructs a ResultSet from materialized rows and drained
// iterator state. rows may be nil when row values are intentionally redacted.
// When dmlRowCount is true, stats are encoded with ResultSetStatsForDML so standard DML
// row_count_exact:0 is preserved. PLAN mode callers must pass false.
func FromIteratorResult(rows []*structpb.ListValue, result spaniter.RowIteratorResult, dmlRowCount bool) (*sppb.ResultSet, error) {
	out := &sppb.ResultSet{
		Rows:     rows,
		Metadata: result.Metadata,
	}

	var resultStats *sppb.ResultSetStats
	var err error
	if dmlRowCount {
		resultStats, err = result.Stats.ResultSetStatsForDML()
	} else {
		resultStats, err = result.Stats.ResultSetStats()
	}
	if err != nil {
		return nil, err
	}
	out.Stats = resultStats
	return out, nil
}
