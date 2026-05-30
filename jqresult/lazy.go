package jqresult

import (
	"sync"

	"cloud.google.com/go/spanner"
	"github.com/wader/gojq"
)

// StatsFunc builds the stats object after all rows have been read from rowIter.
type StatsFunc func(rowIter *spanner.RowIterator) (map[string]any, error)

// StatsFuncFromProto is a StatsFunc that uses BuildResultSet with nil rows (iterator already drained).
func StatsFuncFromProto() StatsFunc {
	return func(rowIter *spanner.RowIterator) (map[string]any, error) {
		return StatsMap(rowIter, nil)
	}
}

// Lazy is a JQValue root for a Spanner query result. Accessing stats drains rows; rows streams via gojq.Iter.
type Lazy struct {
	mu   sync.Mutex
	ioMu sync.Mutex

	rowIter *spanner.RowIterator
	redact  bool
	statsFn StatsFunc

	metadata         map[string]any
	metadataReady    bool
	metadataErr      error
	stats            map[string]any
	rows             *RowIter
	materializedRows []any

	drained  bool
	drainErr error
}

// NewLazy builds a lazy jq input. rowIter must not have been read yet; Lazy takes ownership and Stop()s it.
func NewLazy(rowIter *spanner.RowIterator, redact bool, statsFn StatsFunc) *Lazy {
	l := &Lazy{
		rowIter: rowIter,
		redact:  redact,
		statsFn: statsFn,
	}
	l.rows = NewRowIter(rowIter, redact, RowToJSON)
	l.rows.ioMu = &l.ioMu
	return l
}

func (l *Lazy) drain() error {
	l.mu.Lock()
	if l.drained {
		err := l.drainErr
		l.mu.Unlock()
		return err
	}
	l.mu.Unlock()

	l.ioMu.Lock()
	l.mu.Lock()
	if l.drained {
		err := l.drainErr
		l.mu.Unlock()
		l.ioMu.Unlock()
		return err
	}
	l.mu.Unlock()

	materializedRows, drainErr := l.rows.drainUnlocked()
	var stats map[string]any
	if drainErr == nil && l.statsFn != nil {
		stats, drainErr = l.statsFn(l.rowIter)
	}
	l.ioMu.Unlock()

	l.mu.Lock()
	l.materializedRows = materializedRows
	l.stats = stats
	l.drainErr = drainErr
	l.drained = true
	l.mu.Unlock()

	l.rows.Stop()
	return drainErr
}

func (l *Lazy) ensureMetadata() error {
	l.mu.Lock()
	if l.metadataReady {
		err := l.metadataErr
		l.mu.Unlock()
		return err
	}
	l.mu.Unlock()

	l.ioMu.Lock()
	l.mu.Lock()
	if l.metadataReady {
		err := l.metadataErr
		l.mu.Unlock()
		l.ioMu.Unlock()
		return err
	}
	l.mu.Unlock()

	if err := l.rows.primeUnlocked(); err != nil {
		l.mu.Lock()
		l.metadataErr = err
		l.metadataReady = true
		l.mu.Unlock()
		l.ioMu.Unlock()
		return err
	}
	m, err := MetadataMap(l.rowIter)
	l.ioMu.Unlock()

	l.mu.Lock()
	l.metadata = m
	l.metadataErr = err
	l.metadataReady = true
	l.mu.Unlock()
	return err
}

func (l *Lazy) statsMap() (map[string]any, error) {
	if err := l.drain(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stats, nil
}

func (l *Lazy) rowsJQValue() any {
	l.mu.Lock()
	drained := l.drained
	redact := l.redact
	materialized := append([]any(nil), l.materializedRows...)
	l.mu.Unlock()

	if redact {
		return gojq.NewIter[any]()
	}
	if drained {
		return materializedRowsIter(materialized)
	}
	return &lazyRowsField{l: l}
}

func (l *Lazy) JQValueType() string { return gojq.JQTypeObject }

func (l *Lazy) JQValueLength() any { return 3 }

func (l *Lazy) JQValueSliceLen() any { return 0 }

func (l *Lazy) JQValueIndex(int) any { return nil }

func (l *Lazy) JQValueSlice(int, int) any { return nil }

func (l *Lazy) JQValueKeys() any {
	return []any{"metadata", "rows", "stats"}
}

func (l *Lazy) JQValueHas(key any) any {
	k, _ := key.(string)
	switch k {
	case "metadata", "rows", "stats":
		return true
	default:
		return false
	}
}

func (l *Lazy) JQValueToNumber() any { return nil }

func (l *Lazy) JQValueToString() any { return "" }

func (l *Lazy) JQValueToGoJQ() any {
	if err := l.ensureMetadata(); err != nil {
		return err
	}
	if err := l.drain(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return map[string]any{
		"metadata": l.metadata,
		"stats":    l.stats,
		"rows":     l.materializedRows,
	}
}

func (l *Lazy) JQValueKey(name string) any {
	switch name {
	case "metadata":
		if err := l.ensureMetadata(); err != nil {
			return err
		}
		l.mu.Lock()
		m := l.metadata
		l.mu.Unlock()
		return m
	case "rows":
		return l.rowsJQValue()
	case "stats":
		stats, err := l.statsMap()
		if err != nil {
			return err
		}
		return stats
	default:
		return nil
	}
}

func (l *Lazy) JQValueEach() any {
	if err := l.ensureMetadata(); err != nil {
		return err
	}
	l.mu.Lock()
	m := l.metadata
	drained := l.drained
	var statsVal any
	if drained {
		statsVal = l.stats
	} else {
		statsVal = &lazyStatsField{l: l}
	}
	l.mu.Unlock()

	return []gojq.PathValue{
		{Path: "metadata", Value: m},
		{Path: "rows", Value: l.rowsJQValue()},
		{Path: "stats", Value: statsVal},
	}
}

// lazyStatsField defers stats reads until jq accesses the stats field (including via object iteration).
type lazyStatsField struct {
	l *Lazy
}

func (f *lazyStatsField) JQValueType() string { return gojq.JQTypeObject }

func (f *lazyStatsField) JQValueLength() any {
	s, err := f.l.statsMap()
	if err != nil {
		return err
	}
	return len(s)
}

func (f *lazyStatsField) JQValueSliceLen() any {
	return f.JQValueLength()
}

func (f *lazyStatsField) JQValueIndex(int) any { return nil }

func (f *lazyStatsField) JQValueSlice(int, int) any { return nil }

func (f *lazyStatsField) JQValueKeys() any {
	s, err := f.l.statsMap()
	if err != nil {
		return err
	}
	keys := make([]any, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	return keys
}

func (f *lazyStatsField) JQValueHas(key any) any {
	s, err := f.l.statsMap()
	if err != nil {
		return err
	}
	k, _ := key.(string)
	_, ok := s[k]
	return ok
}

func (f *lazyStatsField) JQValueToNumber() any { return nil }

func (f *lazyStatsField) JQValueToString() any { return "" }

func (f *lazyStatsField) JQValueToGoJQ() any {
	s, err := f.l.statsMap()
	if err != nil {
		return err
	}
	return s
}

func (f *lazyStatsField) JQValueKey(name string) any {
	s, err := f.l.statsMap()
	if err != nil {
		return err
	}
	return s[name]
}

func (f *lazyStatsField) JQValueEach() any {
	s, err := f.l.statsMap()
	if err != nil {
		return err
	}
	pvs := make([]gojq.PathValue, 0, len(s))
	for k, v := range s {
		pvs = append(pvs, gojq.PathValue{Path: k, Value: v})
	}
	return pvs
}

// Stop releases the row iterator without draining unconsumed rows.
func (l *Lazy) Stop() {
	l.rows.Stop()
}

// lazyRowsField streams rows until drain, then replays materializedRows.
type lazyRowsField struct {
	l   *Lazy
	mat *materializedIter
}

func (f *lazyRowsField) Next() (any, bool) {
	f.l.mu.Lock()
	drained := f.l.drained
	redact := f.l.redact
	f.l.mu.Unlock()

	if redact {
		return nil, false
	}
	if drained {
		if f.mat == nil {
			f.l.mu.Lock()
			rows := append([]any(nil), f.l.materializedRows...)
			f.l.mu.Unlock()
			f.mat = &materializedIter{rows: rows}
		}
		return f.mat.Next()
	}
	return f.l.rows.Next()
}

type materializedIter struct {
	rows []any
	i    int
}

func (m *materializedIter) Next() (any, bool) {
	if m.i >= len(m.rows) {
		return nil, false
	}
	v := m.rows[m.i]
	m.i++
	return v, true
}

func materializedRowsIter(rows []any) gojq.Iter {
	if len(rows) == 0 {
		return gojq.NewIter[any]()
	}
	return &materializedIter{rows: rows}
}
