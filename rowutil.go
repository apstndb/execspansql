package main

import (
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"
	"slices"
	"spheric.cloud/xiter"
)

func rowValues(r *spanner.Row) []*structpb.Value {
	return slices.Collect(xiter.Map(xiter.Range(0, r.Size()), r.ColumnValue))
}

const nullString = "<null>"

type spannerNullableValue interface {
	spanner.NullableValue
	fmt.Stringer
}

type nullBytes []byte

func (nb nullBytes) IsNull() bool {
	return nb == nil
}

func (nb nullBytes) String() string {
	return fmt.Sprintf("0x%s", hex.EncodeToString(nb))
}

// DecodeSpanner convert a string to a nullBytes value
func (nb *nullBytes) DecodeSpanner(val interface{}) (err error) {
	strVal, ok := val.(string)
	if !ok {
		return fmt.Errorf("failed to decode nullBytes: %v", val)
	}
	b, err := base64.StdEncoding.DecodeString(strVal)
	if err != nil {
		return err
	}
	*nb = b
	return nil
}

func structFieldPairToString(field *spannerpb.StructType_Field, value *structpb.Value) (string, error) {
	s, err := typeValueToStringExperimental(field.GetType(), value)
	return s + lo.If(field.GetName() != "", " AS "+field.GetName()).Else(""), err
}

// gcvToStringExperimental is simple implementation of Cloud Spanner value formatter.
// It fully utilizes String() method in Cloud Spanner client library if possible.
func gcvToStringExperimental(value *spanner.GenericColumnValue) (string, error) {
	switch value.Type.GetCode() {
	case spannerpb.TypeCode_BOOL:
		return gcvElemToStringExperimental[spanner.NullBool](value)
	case spannerpb.TypeCode_INT64, spannerpb.TypeCode_ENUM:
		return gcvElemToStringExperimental[spanner.NullInt64](value)
	case spannerpb.TypeCode_FLOAT32:
		return gcvElemToStringExperimental[spanner.NullFloat32](value)
	case spannerpb.TypeCode_FLOAT64:
		return gcvElemToStringExperimental[spanner.NullFloat64](value)
	case spannerpb.TypeCode_TIMESTAMP:
		return gcvElemToStringExperimental[spanner.NullTime](value)
	case spannerpb.TypeCode_DATE:
		return gcvElemToStringExperimental[spanner.NullDate](value)
	case spannerpb.TypeCode_STRING:
		return gcvElemToStringExperimental[spanner.NullString](value)
	case spannerpb.TypeCode_BYTES, spannerpb.TypeCode_PROTO:
		return gcvElemToStringExperimental[nullBytes](value)
	case spannerpb.TypeCode_ARRAY:
		// Note: This format is not intended to be parseable.

		if _, isNull := value.Value.Kind.(*structpb.Value_NullValue); isNull {
			return nullString, nil
		}

		fieldsStr, err := tryJoin(xiter.MapErr(
			slices.Values(value.Value.GetListValue().GetValues()),
			func(elemValue *structpb.Value) (string, error) {
				return typeValueToStringExperimental(value.Type.GetArrayElementType(), elemValue)
			}), ", ")
		if err != nil {
			return "", err
		}
		return "[" + fieldsStr + "]", nil
	case spannerpb.TypeCode_STRUCT:
		// Note: This format is not intended to be parseable.

		fieldsStr, err := tryJoin(xiter.MapErr(xiter.Zip(
			slices.Values(value.Type.GetStructType().GetFields()),
			slices.Values(value.Value.GetListValue().GetValues()),
		), UncurryErr(structFieldPairToString)), ", ")
		if err != nil {
			return "", err
		}

		return "(" + fieldsStr + ")", nil
	case spannerpb.TypeCode_NUMERIC:
		return gcvElemToStringExperimental[spanner.NullNumeric](value)
	case spannerpb.TypeCode_JSON:
		return gcvElemToStringExperimental[spanner.NullJSON](value)
	case spannerpb.TypeCode_TYPE_CODE_UNSPECIFIED:
		fallthrough
	default:
		return "", fmt.Errorf("unknown type: %v(code:%v)", value.Type.String(), int32(value.Type.GetCode()))
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
