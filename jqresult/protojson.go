package jqresult

import (
	"bytes"
	"encoding/json"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/execspansql/resultset"
	"github.com/apstndb/spaniter"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ProtoToMap marshals a protobuf message with protojson and decodes to map[string]any.
func ProtoToMap(m proto.Message) (map[string]any, error) {
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var object map[string]any
	if err := dec.Decode(&object); err != nil {
		return nil, err
	}
	return object, nil
}

// MetadataMapFromMetadata returns the metadata object for jq input.
func MetadataMapFromMetadata(metadata *sppb.ResultSetMetadata) (map[string]any, error) {
	if metadata == nil {
		return nil, nil
	}
	return ProtoToMap(metadata)
}

// StatsMapFromResult returns the stats object for jq input from captured iterator state.
func StatsMapFromResult(result spaniter.RowIteratorResult) (map[string]any, error) {
	resultStats, err := result.StatsProto()
	if err != nil {
		return nil, err
	}
	if resultStats == nil {
		return nil, nil
	}
	return ProtoToMap(resultStats)
}

// ResultSetMap materializes a full ResultSet as a jq input map (eager mode).
func ResultSetMap(rs *sppb.ResultSet) (map[string]any, error) {
	return ProtoToMap(rs)
}

// ResultSetMapFromRowIterator materializes rowIter and returns the jq input map.
func ResultSetMapFromRowIterator(rowIter *spanner.RowIterator, redact bool, opts ...spaniter.Option) (map[string]any, error) {
	rs, err := resultset.Materialize(rowIter, redact, opts...)
	if err != nil {
		return nil, err
	}
	return ResultSetMap(rs)
}
