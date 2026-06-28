package main

import (
	"bytes"
	"encoding/json"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/execspansql/jqresult"
	"github.com/goccy/go-yaml"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func encodeResultSetFormat(filter string, rs *sppb.ResultSet, format string) ([]byte, error) {
	code, err := jqresult.Compile(filter, jqresult.InputEager)
	if err != nil {
		return nil, err
	}
	iter, cleanup, err := jqresult.Execute(code, jqresult.InputEager, nil, rs, false)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	var buf bytes.Buffer
	enc, err := newEncoder(&buf, format, false, false)
	if err != nil {
		return nil, err
	}
	if err := jqresult.Print(enc, iter); err != nil {
		return nil, err
	}
	if err := closeEncoder(enc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func jqFilterToMap(filter string, rs *sppb.ResultSet) (map[string]any, error) {
	code, err := jqresult.Compile(filter, jqresult.InputEager)
	if err != nil {
		return nil, err
	}
	iter, cleanup, err := jqresult.Execute(code, jqresult.InputEager, nil, rs, false)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	v, ok := iter.Next()
	if !ok {
		return nil, nil
	}
	if err, isErr := v.(error); isErr {
		return nil, err
	}
	n, err := jqresult.NormalizeForEncode(v)
	if err != nil {
		return nil, err
	}
	m, ok := n.(map[string]any)
	if !ok {
		return map[string]any{"value": n}, nil
	}
	return m, nil
}

func mapToProtoMessage(m map[string]any, msg proto.Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return protojson.Unmarshal(b, msg)
}

func decodeFormatToMap(b []byte, format string) (map[string]any, error) {
	switch format {
	case "yaml":
		var m map[string]any
		if err := yaml.Unmarshal(b, &m); err != nil {
			return nil, err
		}
		return m, nil
	case "json":
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			return nil, err
		}
		return m, nil
	default:
		return nil, nil
	}
}

func assertProtoFormatRoundtrip(
	t *testing.T,
	rs *sppb.ResultSet,
	filter string,
	want proto.Message,
	format string,
) {
	t.Helper()

	encoded, err := encodeResultSetFormat(filter, rs, format)
	if err != nil {
		t.Fatalf("encodeResultSetFormat(%q) error = %v", format, err)
	}

	decoded, err := decodeFormatToMap(encoded, format)
	if err != nil {
		t.Fatalf("decode %q output error = %v", format, err)
	}

	got := proto.Clone(want)
	proto.Reset(got)
	if err := mapToProtoMessage(decoded, got); err != nil {
		t.Fatalf("mapToProtoMessage after %q decode error = %v", format, err)
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("%s roundtrip mismatch (-want +got):\n%s", format, diff)
	}
}

type protoRoundtripCase struct {
	name   string
	rs     *sppb.ResultSet
	filter string
	want   func(*sppb.ResultSet) proto.Message
}

func protoRoundtripCases() []protoRoundtripCase {
	cases := []protoRoundtripCase{
		{
			name:   "all_types_dot",
			rs:     csvFixtures()["bytes_date_timestamp_numeric"],
			filter: ".",
			want:   func(rs *sppb.ResultSet) proto.Message { return rs },
		},
		{
			name:   "dca_albums",
			rs:     yamlGoldenCases()["dca_albums_rowtype_rows"].rs,
			filter: ".",
			want:   func(rs *sppb.ResultSet) proto.Message { return rs },
		},
	}
	for _, tc := range profileYAMLGoldenCases() {
		if tc.filter != "." {
			continue
		}
		rs, err := loadProfileJSONFixture(tc.jsonFile)
		if err != nil {
			panic(err)
		}
		cases = append(cases, protoRoundtripCase{
			name:   tc.golden,
			rs:     rs,
			filter: ".",
			want:   func(rs *sppb.ResultSet) proto.Message { return rs },
		})
	}
	return cases
}

func TestResultSetProtoJSONRoundtrip(t *testing.T) {
	t.Parallel()

	for _, tc := range protoRoundtripCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, err := jqFilterToMap(tc.filter, tc.rs)
			if err != nil {
				t.Fatal(err)
			}
			got := &sppb.ResultSet{}
			if err := mapToProtoMessage(m, got); err != nil {
				t.Fatal(err)
			}
			want := tc.want(tc.rs)
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Fatalf("protojson roundtrip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResultSetYAMLProtoRoundtrip(t *testing.T) {
	for _, tc := range protoRoundtripCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assertProtoFormatRoundtrip(t, tc.rs, tc.filter, tc.want(tc.rs), "yaml")
		})
	}
}

func TestResultSetJSONProtoRoundtrip(t *testing.T) {
	for _, tc := range protoRoundtripCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assertProtoFormatRoundtrip(t, tc.rs, tc.filter, tc.want(tc.rs), "json")
		})
	}
}

func TestResultSetSubmessageYAMLProtoRoundtrip(t *testing.T) {
	rs, err := loadProfileJSONFixture("testdata/profile/singers_limit3.json")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		filter string
		want   proto.Message
	}{
		{"metadata", ".metadata", rs.GetMetadata()},
		{"stats", ".stats", rs.GetStats()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertProtoFormatRoundtrip(t, rs, tc.filter, tc.want, "yaml")
		})
	}
}

func TestResultSetSubmessageJSONProtoRoundtrip(t *testing.T) {
	rs, err := loadProfileJSONFixture("testdata/profile/singers_limit3.json")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		filter string
		want   proto.Message
	}{
		{"metadata", ".metadata", rs.GetMetadata()},
		{"stats", ".stats", rs.GetStats()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertProtoFormatRoundtrip(t, rs, tc.filter, tc.want, "json")
		})
	}
}
