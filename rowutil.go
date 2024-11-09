package main

import (
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue"
	"google.golang.org/protobuf/types/known/structpb"
)

func typeValueToStringExperimental(typ *sppb.Type, value *structpb.Value) (string, error) {
	return spanvalue.SimpleFormatConfig.FormatToplevelColumn(spanner.GenericColumnValue{
		Type:  typ,
		Value: value,
	})
}
