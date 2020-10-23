package main

import (
	"cloud.google.com/go/spanner"
	"google.golang.org/protobuf/types/known/structpb"
)

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
