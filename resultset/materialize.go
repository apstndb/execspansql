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
// Stats encoding is configured via spaniter options such as WithStatsEncoding.
func Materialize(rowIter *spanner.RowIterator, redact bool, opts ...spaniter.Option) (*sppb.ResultSet, error) {
	if rowIter == nil {
		return nil, errors.New("nil row iterator")
	}
	if redact {
		result, err := spaniter.DrainRowIterator(rowIter, opts...)
		if err != nil {
			return nil, err
		}
		return FromIteratorResult(nil, *result)
	}

	var result spaniter.RowIteratorResult
	allOpts := append(append([]spaniter.Option(nil), opts...), spaniter.WithResult(&result))
	rows, err := CollectListValues(rowIter, allOpts...)
	if err != nil {
		return nil, err
	}
	return FromIteratorResult(rows, result)
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
