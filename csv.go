package main

import (
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue"
	svwriter "github.com/apstndb/spanvalue/writer"
)

func validateCSVOutputOptions(o opts) error {
	switch o.CSVFormat {
	case "", "simple", "spanner-cli":
	default:
		return fmt.Errorf("invalid --csv-format %q: must be simple or spanner-cli", o.CSVFormat)
	}
	if o.CSVFormat == "" && !o.NoCSVHeader {
		return nil
	}
	if o.Format != "experimental_csv" {
		return errors.New("--csv-format and --no-csv-header require --format=experimental_csv")
	}
	if o.TryPartitionQuery || o.DiscardResults {
		return errors.New("--csv-format and --no-csv-header require primary CSV output; cannot be combined with --try-partition-query or --discard-results")
	}
	return nil
}

// csvWriterOptions keeps streaming reads and buffered DML output on the same
// formatting policy. An omitted format preserves the existing simple output.
func csvWriterOptions(o opts) []svwriter.DelimitedOption {
	formatter := spanvalue.SimpleFormatConfig()
	if o.CSVFormat == "spanner-cli" {
		formatter = spanvalue.SpannerCLICompatibleFormatConfig()
	}
	return []svwriter.DelimitedOption{
		svwriter.WithHeader(!o.NoCSVHeader),
		svwriter.WithFormatter(formatter),
	}
}

// csvRedactRowIteratorWriter implements [svwriter.RowIteratorWriter] for --redact-rows CSV:
// it registers schema and flushes the header via the embedded [svwriter.DelimitedWriter] but
// discards row bodies in WriteRow while WriteRowIterator drains the iterator.
type csvRedactRowIteratorWriter struct {
	*svwriter.DelimitedWriter
}

func (csvRedactRowIteratorWriter) WriteRow(*spanner.Row) error { return nil }

// writeCsvFromRowIter streams query rows to CSV without materializing a ResultSet.
// WriteRowIterator stops the iterator; queryResult also closes it on early failures.
func writeCsvFromRowIter(writer io.Writer, rowIter *spanner.RowIterator, redactRows bool, options ...svwriter.DelimitedOption) (*svwriter.RowIteratorResult, error) {
	csvWriter, err := svwriter.NewCSVWriter(writer, options...)
	if err != nil {
		return nil, err
	}
	iterWriter := svwriter.RowIteratorWriter(csvWriter)
	if redactRows {
		iterWriter = csvRedactRowIteratorWriter{csvWriter}
	}
	return svwriter.WriteRowIterator(rowIter, iterWriter)
}

func prepareCsvRowType(csvWriter *svwriter.DelimitedWriter, metadata *sppb.ResultSetMetadata) error {
	if metadata == nil || metadata.GetRowType() == nil {
		return errors.New("result set metadata is missing or invalid")
	}
	return csvWriter.PrepareRowType(metadata.GetRowType())
}

// writeCsvFromResultSet writes completed DML results without a live iterator.
func writeCsvFromResultSet(writer io.Writer, rs *sppb.ResultSet, options ...svwriter.DelimitedOption) error {
	if rs == nil || rs.GetMetadata() == nil || rs.GetMetadata().GetRowType() == nil {
		return errors.New("result set metadata is missing or invalid")
	}

	csvWriter, err := svwriter.NewCSVWriter(writer, append(options, svwriter.WithMetadata(rs.GetMetadata()))...)
	if err != nil {
		return err
	}
	for _, row := range rs.GetRows() {
		if row == nil {
			return fmt.Errorf("nil row in result set")
		}
		if err := csvWriter.WriteStructValues(row.GetValues()); err != nil {
			return err
		}
	}
	return csvWriter.Flush()
}
