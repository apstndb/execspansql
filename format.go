package main

import (
	"context"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/execspansql/jqresult"
	"github.com/wader/gojq"
)

// writeResult chooses only an output strategy. Transaction selection, retries,
// and commit handling belong to executeQuery and runCLI.
func (c *preparedCommand) writeResult(ctx context.Context, result *queryResult, sinks *outputSinks) error {
	// Discarded results need metadata/stats only. DML is already materialized;
	// read-only CSV and lazy jq retain their streaming behavior.
	if c.DiscardResults || result.resultSet != nil || (c.Format != "experimental_csv" && c.jqMode == jqresult.InputEager) {
		rs, err := result.materialize(materializeWithoutRows(c.opts))
		if err != nil {
			return err
		}
		return c.writeResultSet(ctx, rs, sinks)
	}

	if c.Format == "experimental_csv" {
		result, err := writeCsvFromRowIter(sinks.primary, result.rowIter, c.RedactRows)
		if err != nil {
			return err
		}
		sinks.MarkPrimaryComplete()
		if !sinks.hasPlan {
			return nil
		}
		stats, err := statsFromWriterResult(result)
		if err != nil {
			return err
		}
		return c.writePlan(ctx, sinks, result.Metadata, stats)
	}

	enc, err := newEncoder(sinks.primary, c.Format, c.CompactOutput, c.JqRawOutput)
	if err != nil {
		return err
	}
	var lazyOpts []jqresult.LazyOption
	if sinks.hasPlan {
		lazyOpts = append(lazyOpts, jqresult.WithOmitQueryPlan())
	}
	lazy := jqresult.NewLazy(result.rowIter, c.RedactRows, lazyOpts...)
	defer lazy.Stop()
	if err := printJQ(ctx, c.jqCode, lazy, enc); err != nil {
		return err
	}
	sinks.MarkPrimaryComplete()
	if !sinks.hasPlan {
		return nil
	}
	if err := lazy.Drain(); err != nil {
		return err
	}
	drained := lazy.Result()
	stats, err := drained.StatsProto()
	if err != nil {
		return err
	}
	return c.writePlan(ctx, sinks, drained.Metadata, stats)
}

func (c *preparedCommand) writeResultSet(ctx context.Context, rs *sppb.ResultSet, sinks *outputSinks) error {
	stats, metadata := rs.Stats, rs.Metadata
	if sinks.hasPlan {
		stats, metadata = stripQueryPlanForPrimary(rs)
	}
	if sinks.primary != nil {
		if c.Format == "experimental_csv" {
			if err := writeCsvFromResultSet(sinks.primary, rs); err != nil {
				return err
			}
		} else {
			input, err := jqresult.ResultSetMap(rs)
			if err != nil {
				return err
			}
			enc, err := newEncoder(sinks.primary, c.Format, c.CompactOutput, c.JqRawOutput)
			if err != nil {
				return err
			}
			if err := printJQ(ctx, c.jqCode, input, enc); err != nil {
				return err
			}
		}
	}
	sinks.MarkPrimaryComplete()
	return c.writePlan(ctx, sinks, metadata, stats)
}

// printJQ owns encoder completion on both success and failure. Use the caller's
// cancellation context even after SQL has completed and no RPC remains active.
func printJQ(ctx context.Context, code *gojq.Code, input any, enc encoder) (err error) {
	defer func() {
		if closeErr := closeEncoder(enc); err == nil {
			err = closeErr
		}
	}()
	return jqresult.Print(enc, code.RunWithContext(ctx, input))
}

// Use the prepared statement for query text so rendering never reloads SQL files.
func (c *preparedCommand) writePlan(ctx context.Context, sinks *outputSinks, metadata *sppb.ResultSetMetadata, stats *sppb.ResultSetStats) error {
	o := c.opts
	o.Sql = c.statement.SQL
	return writePlan(ctx, sinks.plan, effectivePlanFormat(o), metadata, stats, o)
}
