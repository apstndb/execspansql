package jqresult

import (
	"slices"
	"spheric.cloud/xiter"

	"cloud.google.com/go/spanner"
	"google.golang.org/protobuf/types/known/structpb"
)

// RowToListValue converts a Spanner row to structpb.ListValue.
func RowToListValue(r *spanner.Row) *structpb.ListValue {
	return &structpb.ListValue{Values: slices.Collect(xiter.Map(xiter.Range(0, r.Size()), r.ColumnValue))}
}

// RowToJSON encodes one row the same way as protojson on a single-row ResultSet (array-shaped row).
func RowToJSON(r *spanner.Row) (any, error) {
	return RowToListValue(r).AsSlice(), nil
}
