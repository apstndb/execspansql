package main

import (
	"encoding/hex"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
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

const nullString = "<null>"

type spannerNullableValue interface {
	spanner.NullableValue
	fmt.Stringer
}

type nullBytes []byte

func (n nullBytes) IsNull() bool {
	return n == nil
}

func (n nullBytes) String() string {
	return fmt.Sprintf("0x%s", hex.EncodeToString(n))
}

// gcvToStringExperimental is simple implementation of Cloud Spanner value formatter.
// It fully utilizes String() method in Cloud Spanner client library if possible.
func gcvToStringExperimental(value *spanner.GenericColumnValue) (string, error) {
	switch value.Type.GetCode() {
	case spannerpb.TypeCode_BOOL:
		return gcvElemToStringExperimental[spanner.NullBool](value)
	case spannerpb.TypeCode_INT64:
		return gcvElemToStringExperimental[spanner.NullInt64](value)
	case spannerpb.TypeCode_FLOAT64:
		return gcvElemToStringExperimental[spanner.NullFloat64](value)
	case spannerpb.TypeCode_TIMESTAMP:
		return gcvElemToStringExperimental[spanner.NullTime](value)
	case spannerpb.TypeCode_DATE:
		return gcvElemToStringExperimental[spanner.NullDate](value)
	case spannerpb.TypeCode_STRING:
		return gcvElemToStringExperimental[spanner.NullString](value)
	case spannerpb.TypeCode_BYTES:
		return gcvElemToStringExperimental[nullBytes](value)
	case spannerpb.TypeCode_ARRAY:
		// Note: This format is not intended to be parseable.

		if _, isNull := value.Value.Kind.(*structpb.Value_NullValue); isNull {
			return nullString, nil
		}

		var fieldStrings []string
		for _, v := range value.Value.GetListValue().GetValues() {
			if s, err := typeValueToStringExperimental(value.Type.GetArrayElementType(), v); err != nil {
				return "", err
			} else {
				fieldStrings = append(fieldStrings, s)
			}
		}
		return fmt.Sprintf("[%v]", strings.Join(fieldStrings, ", ")), nil
	case spannerpb.TypeCode_STRUCT:
		// Note: This format is not intended to be parseable.

		var fieldStrings []string
		for i, field := range value.Type.GetStructType().GetFields() {
			if s, err := typeValueToStringExperimental(field.GetType(), value.Value.GetListValue().GetValues()[i]); err != nil {
				return "", err
			} else {
				fieldStrings = append(fieldStrings, fmt.Sprintf("%v AS %v", s, field.GetName()))
			}
		}

		return fmt.Sprintf("(%v)", strings.Join(fieldStrings, ", ")), nil
	case spannerpb.TypeCode_NUMERIC:
		return gcvElemToStringExperimental[spanner.NullNumeric](value)
	case spannerpb.TypeCode_JSON:
		return gcvElemToStringExperimental[spanner.NullJSON](value)
	case spannerpb.TypeCode_TYPE_CODE_UNSPECIFIED:
		fallthrough
	default:
		return "", fmt.Errorf("unknown type: %v", value.Type.String())
	}
}

func gcvElemToStringExperimental[T spannerNullableValue](value *spanner.GenericColumnValue) (string, error) {
	var v T
	if err := value.Decode(&v); err != nil {
		return "", err
	}

	if v.IsNull() {
		return nullString, nil
	}

	return v.String(), nil
}

func typeValueToStringExperimental(typ *spannerpb.Type, value *structpb.Value) (string, error) {
	return gcvToStringExperimental(&spanner.GenericColumnValue{
		Type:  typ,
		Value: value,
	})
}
