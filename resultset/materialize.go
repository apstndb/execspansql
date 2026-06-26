package resultset

import (
	"errors"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spaniter"
	"google.golang.org/protobuf/types/known/structpb"
)

// Materialize drains rowIter and returns a protobuf ResultSet.
// rowIter must not have been read yet; Materialize owns and stops it.
// When redact is true, row values are omitted but metadata and stats are preserved.
// When dml is true, stats use ResultSetStatsForDML for standard DML row counts.
func Materialize(rowIter *spanner.RowIterator, redact bool, dml bool) (*sppb.ResultSet, error) {
	if rowIter == nil {
		return nil, errors.New("nil row iterator")
	}
	if redact {
		result, err := spaniter.DrainRowIterator(rowIter)
		if err != nil {
			return nil, err
		}
		return FromIteratorResult(nil, *result, dml)
	}

	var result spaniter.RowIteratorResult
	rows, err := CollectListValues(rowIter, spaniter.WithResult(&result))
	if err != nil {
		return nil, err
	}
	return FromIteratorResult(rows, result, dml)
}

// CollectListValues drains rowIter into protobuf row values.
// rowIter must not have been read yet unless opts configure partial consumption.
func CollectListValues(rowIter *spanner.RowIterator, opts ...spaniter.Option) ([]*structpb.ListValue, error) {
	var rows []*structpb.ListValue
	for row, err := range spaniter.RowIteratorSeq(rowIter, opts...) {
		if err != nil {
			return nil, err
		}
		rows = append(rows, RowToListValue(row))
	}
	return rows, nil
}
