package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/execspansql/resultset"
)

// queryResult owns either a live read-only iterator or a completed ResultSet.
// Write results are exposed only after commit, so output code never participates
// in transaction retries and cannot change the outcome of a successful write.
type queryResult struct {
	rowIter   *spanner.RowIterator
	resultSet *sppb.ResultSet
	committed bool
}

func executeQuery(ctx context.Context, client *spanner.Client, command *preparedCommand) (*queryResult, error) {
	result := &queryResult{}
	switch mode := command.mode.(type) {
	case single:
		result.rowIter = client.Single().WithTimestampBound(mode.TimestampBound).QueryWithOptions(ctx, command.statement, command.queryOptions)
	case readWrite:
		_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) (err error) {
			// Each attempt replaces the previous result. In particular, no rows from
			// an aborted attempt can reach stdout or a file destination.
			result.resultSet, err = resultset.Materialize(tx.QueryWithOptions(ctx, command.statement, command.queryOptions),
				materializeWithoutRows(command.opts), spaniterStatsOpts(mode, command.queryOptions)...)
			return err
		})
		if err != nil {
			return nil, err
		}
		result.committed = true
	case partitionedDML:
		count, err := client.PartitionedUpdateWithOptions(ctx, command.statement, command.queryOptions)
		if err != nil {
			return nil, err
		}
		result.resultSet = &sppb.ResultSet{
			Metadata: &sppb.ResultSetMetadata{RowType: &sppb.StructType{}},
			Stats:    &sppb.ResultSetStats{RowCount: &sppb.ResultSetStats_RowCountLowerBound{RowCountLowerBound: count}},
		}
		result.committed = true
	default:
		return nil, fmt.Errorf("unknown query mode: %T", mode)
	}
	return result, nil
}

func (r *queryResult) materialize(redact bool) (*sppb.ResultSet, error) {
	if r.resultSet != nil {
		return r.resultSet, nil
	}
	var err error
	r.resultSet, err = resultset.Materialize(r.rowIter, redact)
	r.rowIter = nil // Materialize owns and stops the iterator, including on error.
	return r.resultSet, err
}

func (r *queryResult) Close() {
	if r.rowIter != nil {
		r.rowIter.Stop()
	}
}
