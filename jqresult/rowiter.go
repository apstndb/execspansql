package jqresult

import (
	"iter"
	"sync"

	"cloud.google.com/go/spanner"
	"github.com/apstndb/spaniter"
)

// RowIter streams query rows as jq-compatible values ([]any per row, matching protojson ResultSet rows).
type RowIter struct {
	ioMu      *sync.Mutex
	rowIter   *spanner.RowIterator
	redact    bool
	rowToJSON func(*spanner.Row) (any, error)
	stopped   bool

	result    spaniter.RowIteratorResult
	pull      func() (*spanner.Row, error, bool)
	stopSeq   func()
	seqActive bool

	primedRow *spanner.Row
	primed    bool
}

func NewRowIter(rowIter *spanner.RowIterator, redact bool, rowToJSON func(*spanner.Row) (any, error)) *RowIter {
	if rowToJSON == nil {
		rowToJSON = RowToJSON
	}
	return &RowIter{rowIter: rowIter, redact: redact, rowToJSON: rowToJSON}
}

// Result returns lifecycle data captured by spaniter while rows are consumed.
func (r *RowIter) Result() spaniter.RowIteratorResult {
	return r.result
}

func (r *RowIter) lockIO() func() {
	if r.ioMu == nil {
		return func() {}
	}
	r.ioMu.Lock()
	return r.ioMu.Unlock
}

func (r *RowIter) ensureSeq() {
	if r.seqActive || r.stopped || r.rowIter == nil {
		return
	}
	r.seqActive = true
	seq := spaniter.RowIteratorSeq(r.rowIter, spaniter.WithResult(&r.result))
	r.pull, r.stopSeq = iter.Pull2(seq)
}

// Prime reads the first row (or iterator.Done) so metadata is populated.
// The first row is buffered for the next Next call when present.
func (r *RowIter) Prime() error {
	unlock := r.lockIO()
	defer unlock()
	return r.primeUnlocked()
}

func (r *RowIter) primeUnlocked() error {
	if r.primed || r.stopped {
		return nil
	}
	r.primed = true
	r.ensureSeq()
	if r.pull == nil {
		return nil
	}
	row, err, ok := r.pull()
	if !ok {
		return err
	}
	if err != nil {
		return err
	}
	r.primedRow = row
	return nil
}

func (r *RowIter) nextRow() (*spanner.Row, error) {
	if r.primedRow != nil {
		row := r.primedRow
		r.primedRow = nil
		return row, nil
	}
	r.ensureSeq()
	if r.pull == nil {
		return nil, nil
	}
	row, err, ok := r.pull()
	if !ok {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return row, nil
}

func (r *RowIter) Next() (any, bool) {
	unlock := r.lockIO()
	defer unlock()
	return r.nextUnlocked()
}

func (r *RowIter) nextUnlocked() (any, bool) {
	if r.redact || r.stopped {
		return nil, false
	}
	row, err := r.nextRow()
	if row == nil && err == nil {
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
	unlock := r.lockIO()
	defer unlock()
	return r.drainUnlocked()
}

func (r *RowIter) drainUnlocked() ([]any, error) {
	if r.stopped {
		return nil, nil
	}
	var rows []any
	for {
		row, err := r.nextRow()
		if row == nil && err == nil {
			return rows, nil
		}
		if err != nil {
			return rows, err
		}
		if r.redact {
			continue
		}
		v, err := r.rowToJSON(row)
		if err != nil {
			return rows, err
		}
		rows = append(rows, v)
	}
}

func (r *RowIter) Stop() {
	unlock := r.lockIO()
	defer unlock()

	if r.stopped {
		return
	}
	r.stopped = true
	if r.stopSeq != nil {
		r.stopSeq()
		return
	}
	if r.rowIter != nil {
		r.rowIter.Stop()
	}
}
