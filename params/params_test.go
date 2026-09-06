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

func TestReadmeExampleProfileParams(t *testing.T) {
	t.Parallel()

	file, err := LoadParamFile("testdata/readme_example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// README documents --query-mode=PROFILE, which calls GenerateParams(..., false).
	got, err := GenerateParams(file, false)
	if err != nil {
		t.Fatalf("GenerateParams(PROFILE) for README example: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("README PROFILE example params = %v, want exactly arr", got)
	}

	arr, ok := got["arr"].(spanner.GenericColumnValue)
	if !ok {
		t.Fatalf("arr: expected GenericColumnValue, got %T", got["arr"])
	}
	if arr.Type.GetCode() != sppb.TypeCode_ARRAY || arr.Type.GetArrayElementType().GetCode() != sppb.TypeCode_STRING {
		t.Fatalf("arr type = %v, want ARRAY<STRING>", arr.Type)
	}
	vals := arr.Value.GetListValue().GetValues()
	if len(vals) != 2 || vals[0].GetStringValue() != "foo" || vals[1].GetStringValue() != "bar" {
		t.Fatalf("arr values = %v, want [foo bar]", vals)
	}
}
