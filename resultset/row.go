package resultset

import (
	"cloud.google.com/go/spanner"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"
)

// RowToListValue converts a Spanner row to structpb.ListValue.
func RowToListValue(r *spanner.Row) *structpb.ListValue {
	return &structpb.ListValue{
		Values: lo.Map(lo.Range(r.Size()), func(i int, _ int) *structpb.Value {
			return r.ColumnValue(i)
		}),
	}
}
