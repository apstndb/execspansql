package resultset

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spaniter"
	"google.golang.org/protobuf/types/known/structpb"
)

// FromIteratorResult constructs a ResultSet from materialized rows and drained
// iterator state. rows may be nil when row values are intentionally redacted.
// Stats encoding comes from spaniter drain options such as WithStatsEncoding.
func FromIteratorResult(rows []*structpb.ListValue, result spaniter.RowIteratorResult) (*sppb.ResultSet, error) {
	return result.ResultSet(rows)
}
