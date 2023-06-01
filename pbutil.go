package main

import (
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// valueFromSpannerpbType generate a minimum valid value for a type.
func valueFromSpannerpbType(typ *spannerpb.Type) (*structpb.Value, error) {
	switch typ.GetCode() {
	// Only STRUCT needs a non-null value.
	case spannerpb.TypeCode_STRUCT:
		var values []*structpb.Value
		for range typ.StructType.GetFields() {
			values = append(values, structpb.NewNullValue())
		}
		return structpb.NewListValue(&structpb.ListValue{Values: values}), nil
	default:
		return structpb.NewNullValue(), nil
	}
}
