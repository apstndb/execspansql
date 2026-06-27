package params

import (
	"testing"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGenerateParams(t *testing.T) {
	t.Parallel()

	got, err := GenerateParams(map[string]string{"int_val": "42"}, false)
	if err != nil {
		t.Fatalf("GenerateParams failed: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 param, got %d", len(got))
	}
	v, ok := got["int_val"].(spanner.GenericColumnValue)
	if !ok {
		t.Fatalf("expected spanner.GenericColumnValue, got %T", got["int_val"])
	}
	if v.Type.GetCode() != sppb.TypeCode_INT64 {
		t.Fatalf("expected INT64, got %v", v.Type.GetCode())
	}
}

func TestGenerateParamsPermitType(t *testing.T) {
	t.Parallel()

	got, err := GenerateParams(map[string]string{"typed_null": "INT64"}, true)
	if err != nil {
		t.Fatalf("GenerateParams failed: %v", err)
	}
	v, ok := got["typed_null"].(spanner.GenericColumnValue)
	if !ok {
		t.Fatalf("expected spanner.GenericColumnValue, got %T", got["typed_null"])
	}
	if v.Type.GetCode() != sppb.TypeCode_INT64 {
		t.Fatalf("expected INT64, got %v", v.Type.GetCode())
	}
	if v.Value == nil || v.Value.GetNullValue() != structpb.NullValue_NULL_VALUE {
		t.Fatalf("expected typed null value, got %v", v.Value)
	}
}
