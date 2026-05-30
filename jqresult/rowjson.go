package jqresult

import (
	"bytes"
	"encoding/json"
	"slices"
	"spheric.cloud/xiter"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// RowToListValue converts a Spanner row to structpb.ListValue.
func RowToListValue(r *spanner.Row) *structpb.ListValue {
	return &structpb.ListValue{Values: slices.Collect(xiter.Map(xiter.Range(0, r.Size()), r.ColumnValue))}
}

// RowToJSON encodes one row the same way as protojson on a single-row ResultSet (array-shaped row).
func RowToJSON(r *spanner.Row) (any, error) {
	rs := &sppb.ResultSet{Rows: []*structpb.ListValue{RowToListValue(r)}}
	b, err := protojson.Marshal(rs)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return nil, err
	}
	rows, ok := obj["rows"].([]any)
	if !ok || len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}
