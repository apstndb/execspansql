package jqresult

import (
	"bytes"
	"encoding/json"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
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

// MetadataMap returns the metadata object for a row iterator (before rows are consumed).
func MetadataMap(rowIter *spanner.RowIterator) (map[string]any, error) {
	obj, err := ProtoToMap(&sppb.ResultSet{Metadata: rowIter.Metadata})
	if err != nil {
		return nil, err
	}
	if m, ok := obj["metadata"].(map[string]any); ok {
		return m, nil
	}
	return nil, nil
}

// StatsMap builds the stats object after rowIter has been fully consumed.
func StatsMap(rowIter *spanner.RowIterator, rows []*structpb.ListValue) (map[string]any, error) {
	rs, err := BuildResultSet(rows, rowIter)
	if err != nil {
		return nil, err
	}
	obj, err := ProtoToMap(rs)
	if err != nil {
		return nil, err
	}
	if s, ok := obj["stats"].(map[string]any); ok {
		return s, nil
	}
	return nil, nil
}

// ResultSetMap materializes a full ResultSet as a jq input map (eager mode).
func ResultSetMap(rs *sppb.ResultSet) (map[string]any, error) {
	return ProtoToMap(rs)
}

// BuildResultSet constructs ResultSet stats from a consumed row iterator.
func BuildResultSet(rows []*structpb.ListValue, rowIter *spanner.RowIterator) (*sppb.ResultSet, error) {
	out := &sppb.ResultSet{
		Rows:     rows,
		Metadata: rowIter.Metadata,
	}

	var queryStats *structpb.Struct
	if rowIter.QueryStats != nil {
		qs, err := structpb.NewStruct(rowIter.QueryStats)
		if err != nil {
			return nil, err
		}
		queryStats = qs
	}

	if rowIter.QueryPlan == nil && queryStats == nil && rowIter.RowCount == 0 {
		return out, nil
	}

	out.Stats = &sppb.ResultSetStats{
		QueryPlan:  rowIter.QueryPlan,
		QueryStats: queryStats,
	}
	if rowIter.RowCount != 0 {
		out.Stats.RowCount = &sppb.ResultSetStats_RowCountExact{RowCountExact: rowIter.RowCount}
	}
	return out, nil
}
