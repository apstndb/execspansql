package resultset

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spaniter"
	"google.golang.org/protobuf/types/known/structpb"
)

// FromIteratorResult constructs a ResultSet from materialized rows and drained
// iterator state. rows may be nil when row values are intentionally redacted.
func FromIteratorResult(rows []*structpb.ListValue, result spaniter.RowIteratorResult) (*sppb.ResultSet, error) {
	out := &sppb.ResultSet{
		Rows:     rows,
		Metadata: result.Metadata,
	}

	resultStats, err := result.Stats.ResultSetStats()
	if err != nil {
		return nil, err
	}
	out.Stats = resultStats
	return out, nil
}
