package jqresult

import (
	"errors"
	"testing"

	"cloud.google.com/go/spanner"
)

func TestRowIterPrimePropagatesTerminalError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("terminal iteration error")
	r := &RowIter{
		seqActive: true,
		pull: func() (*spanner.Row, error, bool) {
			return nil, sentinel, true
		},
	}

	if err := r.primeUnlocked(); !errors.Is(err, sentinel) {
		t.Fatalf("primeUnlocked() = %v, want %v", err, sentinel)
	}
}

func TestRowIterNextRowPropagatesTerminalError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("terminal iteration error")
	r := &RowIter{
		seqActive: true,
		primed:    true,
		pull: func() (*spanner.Row, error, bool) {
			return nil, sentinel, true
		},
	}

	row, err := r.nextRow()
	if row != nil {
		t.Fatalf("row = %v, want nil", row)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
}

func TestRowIterStopDoesNotDrainRemainingRows(t *testing.T) {
	t.Parallel()

	var pullsAfterPrime int
	first := true
	r := &RowIter{
		seqActive: true,
		stopSeq:   func() {},
		pull: func() (*spanner.Row, error, bool) {
			if first {
				first = false
				return &spanner.Row{}, nil, true
			}
			pullsAfterPrime++
			return nil, nil, false
		},
	}
	if err := r.primeUnlocked(); err != nil {
		t.Fatal(err)
	}
	r.Stop()
	if pullsAfterPrime != 0 {
		t.Fatalf("Stop() drained %d rows, want 0", pullsAfterPrime)
	}
}
