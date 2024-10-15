package main

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
	"slices"
	"spheric.cloud/xiter"
)

// valueFromSpannerpbType generate a minimum valid value for a type.
func valueFromSpannerpbType(typ *sppb.Type) *structpb.Value {
	switch typ.GetCode() {
	// Only STRUCT needs a non-null value.
	case sppb.TypeCode_STRUCT:
		return structpb.NewListValue(&structpb.ListValue{Values: slices.Collect(xiter.Repeat(structpb.NewNullValue(), len(typ.StructType.GetFields())))})
	default:
		return structpb.NewNullValue()
	}
}
