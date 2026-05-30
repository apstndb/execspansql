package jqresult

import (
	"errors"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// RowIter streams query rows as jq-compatible values ([]any per row, matching protojson ResultSet rows).
type RowIter struct {
	iter      *spanner.RowIterator
	redact    bool
	rowToJSON func(*spanner.Row) (any, error)
	stopped   bool

	primedRow *spanner.Row
	primed    bool
}

func NewRowIter(rowIter *spanner.RowIterator, redact bool, rowToJSON func(*spanner.Row) (any, error)) *RowIter {
	if rowToJSON == nil {
		rowToJSON = RowToJSON
	}
	return &RowIter{iter: rowIter, redact: redact, rowToJSON: rowToJSON}
}

// Prime reads the first row (or iterator.Done) so rowIter.Metadata is populated.
// The first row is buffered for the next Next call when present.
func (r *RowIter) Prime() error {
	if r.primed || r.stopped {
		return nil
	}
	r.primed = true
	row, err := r.iter.Next()
	if errors.Is(err, iterator.Done) {
		return nil
	}
	if err != nil {
		return err
	}
	r.primedRow = row
	return nil
}

func (r *RowIter) Next() (any, bool) {
	if r.redact || r.stopped {
		return nil, false
	}
	if r.primedRow != nil {
		row := r.primedRow
		r.primedRow = nil
		v, err := r.rowToJSON(row)
		if err != nil {
			return err, true
		}
		return v, true
	}
	row, err := r.iter.Next()
	if errors.Is(err, iterator.Done) {
		return nil, false
	}
	if err != nil {
		return err, true
	}
	v, err := r.rowToJSON(row)
	if err != nil {
		return err, true
	}
	return v, true
}

// Drain exhausts the Spanner row iterator. When redact is false, drained rows are returned.
func (r *RowIter) Drain() ([]any, error) {
	if r.stopped {
		return nil, nil
	}
	var rows []any
	for {
		v, ok := r.Next()
		if !ok {
			return rows, nil
		}
		if err, isErr := v.(error); isErr {
			return rows, err
		}
		if r.redact {
			continue
		}
		rows = append(rows, v)
	}
}

func (r *RowIter) Stop() {
	if r.stopped {
		return
	}
	r.stopped = true
	r.iter.Stop()
}
