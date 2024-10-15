package main

import (
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
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

func structFieldPairToString(field *sppb.StructType_Field, value *structpb.Value) (string, error) {
	s, err := typeValueToStringExperimental(field.GetType(), value)
	return s + lo.If(field.GetName() != "", " AS "+field.GetName()).Else(""), err
}

// gcvToStringExperimental is simple implementation of Cloud Spanner value formatter.
// It fully utilizes String() method in Cloud Spanner client library if possible.
func gcvToStringExperimental(value *spanner.GenericColumnValue) (string, error) {
	switch value.Type.GetCode() {
	case sppb.TypeCode_BOOL:
		return gcvElemToStringExperimental[spanner.NullBool](value)
	case sppb.TypeCode_INT64, sppb.TypeCode_ENUM:
		return gcvElemToStringExperimental[spanner.NullInt64](value)
	case sppb.TypeCode_FLOAT32:
		return gcvElemToStringExperimental[spanner.NullFloat32](value)
	case sppb.TypeCode_FLOAT64:
		return gcvElemToStringExperimental[spanner.NullFloat64](value)
	case sppb.TypeCode_TIMESTAMP:
		return gcvElemToStringExperimental[spanner.NullTime](value)
	case sppb.TypeCode_DATE:
		return gcvElemToStringExperimental[spanner.NullDate](value)
	case sppb.TypeCode_STRING:
		return gcvElemToStringExperimental[spanner.NullString](value)
	case sppb.TypeCode_BYTES, sppb.TypeCode_PROTO:
		return gcvElemToStringExperimental[nullBytes](value)
	case sppb.TypeCode_ARRAY:
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
	case sppb.TypeCode_STRUCT:
		// Note: This format is not intended to be parseable.

		fieldsStr, err := tryJoin(xiter.MapErr(xiter.Zip(
			slices.Values(value.Type.GetStructType().GetFields()),
			slices.Values(value.Value.GetListValue().GetValues()),
		), UncurryErr(structFieldPairToString)), ", ")
		if err != nil {
			return "", err
		}

		return "(" + fieldsStr + ")", nil
	case sppb.TypeCode_NUMERIC:
		return gcvElemToStringExperimental[spanner.NullNumeric](value)
	case sppb.TypeCode_JSON:
		return gcvElemToStringExperimental[spanner.NullJSON](value)
	case sppb.TypeCode_TYPE_CODE_UNSPECIFIED:
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

func typeValueToStringExperimental(typ *sppb.Type, value *structpb.Value) (string, error) {
	return gcvToStringExperimental(&spanner.GenericColumnValue{
		Type:  typ,
		Value: value,
	})
}
