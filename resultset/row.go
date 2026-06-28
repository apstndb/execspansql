package resultset

import (
	"cloud.google.com/go/spanner"
	"google.golang.org/protobuf/types/known/structpb"
)

// RowToListValue converts a Spanner row to structpb.ListValue.
func RowToListValue(r *spanner.Row) *structpb.ListValue {
	values := make([]*structpb.Value, r.Size())
	for i := range values {
		values[i] = r.ColumnValue(i)
	}
	return &structpb.ListValue{Values: values}
}
