package main

import (
	"cloud.google.com/go/spanner"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func extractRowType(r *spanner.Row) ([]*spannerpb.StructType_Field, error) {
	var rowType []*spannerpb.StructType_Field
	for i, name := range r.ColumnNames() {
		var v spanner.GenericColumnValue
		if err := r.Column(i, &v); err != nil {
			return nil, err
		}

		rowType = append(rowType, &spannerpb.StructType_Field{
			Name: name,
			Type: v.Type,
		})
	}
	return rowType, nil
}

func rowValues(r *spanner.Row) ([]*structpb.Value, error) {
	var vs []*structpb.Value
	for i := 0; i < r.Size(); i++ {
		var gcv spanner.GenericColumnValue
		err := r.Column(i, &gcv)
		if err != nil {
			return nil, err
		}
		vs = append(vs, gcv.Value)
	}
	return vs, nil
}
