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
	rowsStreamDone bool

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
	alreadyStreamed := l.rowsStreamDone
	cachedRows := append([]any(nil), l.materializedRows...)
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

	var materializedRows []any
	var drainErr error
	if alreadyStreamed {
		materializedRows = cachedRows
	} else {
		var remaining []any
		remaining, drainErr = l.rows.drainUnlocked()
		materializedRows = append(cachedRows, remaining...)
	}
	var stats map[string]any
	if drainErr == nil && l.statsFn != nil {
		stats, drainErr = l.statsFn(l.rowIter)
	}
	l.ioMu.Unlock()

	l.mu.Lock()
	if materializedRows == nil {
		materializedRows = []any{}
	}
	l.materializedRows = materializedRows
	l.stats = stats
	l.drainErr = drainErr
	l.drained = true
	l.rowsStreamDone = true
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
	streamDone := l.rowsStreamDone
	materialized := append([]any(nil), l.materializedRows...)
	l.mu.Unlock()

	if redact {
		return &lazyRowsField{l: l, mat: materializedRowsIter([]any{})}
	}
	if drained || streamDone {
		return &lazyRowsField{l: l, mat: materializedRowsIter(materialized)}
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

// lazyRowsField streams rows and caches them so multiple captured .rows values can replay.
type lazyRowsField struct {
	l   *Lazy
	mat gojq.Iter
}

func (f *lazyRowsField) cachedRows() ([]any, error) {
	f.l.mu.Lock()
	defer f.l.mu.Unlock()
	if f.l.redact {
		return []any{}, nil
	}
	rows := append([]any(nil), f.l.materializedRows...)
	if rows == nil {
		rows = []any{}
	}
	return rows, nil
}

func (f *lazyRowsField) materializeSlice() (any, error) {
	f.l.mu.Lock()
	streamDone := f.l.rowsStreamDone
	drained := f.l.drained
	f.l.mu.Unlock()

	if streamDone || drained {
		return f.cachedRows()
	}

	var out []any
	for {
		v, ok := f.Next()
		if !ok {
			if err, isErr := v.(error); isErr {
				return nil, err
			}
			return out, nil
		}
		if err, isErr := v.(error); isErr {
			return nil, err
		}
		out = append(out, v)
	}
}

func (f *lazyRowsField) JQValueType() string { return gojq.JQTypeArray }

func (f *lazyRowsField) JQValueLength() any {
	s, err := f.materializeSlice()
	if err != nil {
		return err
	}
	return len(s.([]any))
}

func (f *lazyRowsField) JQValueSliceLen() any { return f.JQValueLength() }

func (f *lazyRowsField) JQValueIndex(i int) any {
	s, err := f.materializeSlice()
	if err != nil {
		return err
	}
	rows := s.([]any)
	if i < 0 || i >= len(rows) {
		return nil
	}
	return rows[i]
}

func (f *lazyRowsField) JQValueSlice(start, end int) any {
	s, err := f.materializeSlice()
	if err != nil {
		return err
	}
	rows := s.([]any)
	if start < 0 {
		start = 0
	}
	if end > len(rows) {
		end = len(rows)
	}
	if start > end {
		return []any{}
	}
	return rows[start:end]
}

func (f *lazyRowsField) JQValueKeys() any {
	s, err := f.materializeSlice()
	if err != nil {
		return err
	}
	rows := s.([]any)
	keys := make([]any, len(rows))
	for i := range rows {
		keys[i] = i
	}
	return keys
}

func (f *lazyRowsField) JQValueHas(key any) any {
	s, err := f.materializeSlice()
	if err != nil {
		return err
	}
	rows := s.([]any)
	i, ok := key.(int)
	if !ok {
		return false
	}
	return i >= 0 && i < len(rows)
}

func (f *lazyRowsField) JQValueToNumber() any { return nil }

func (f *lazyRowsField) JQValueToString() any { return "" }

func (f *lazyRowsField) JQValueToGoJQ() any {
	s, err := f.materializeSlice()
	if err != nil {
		return err
	}
	return s
}

func (f *lazyRowsField) JQValueKey(string) any { return nil }

func (f *lazyRowsField) JQValueEach() any {
	f.l.mu.Lock()
	streamDone := f.l.rowsStreamDone
	drained := f.l.drained
	redact := f.l.redact
	f.l.mu.Unlock()

	if redact {
		return []gojq.PathValue{}
	}
	// While rows are still streaming, let jq iterate via Next() instead of materializing here.
	if !streamDone && !drained {
		return nil
	}
	rows, err := f.cachedRows()
	if err != nil {
		return err
	}
	pvs := make([]gojq.PathValue, len(rows))
	for i, v := range rows {
		pvs[i] = gojq.PathValue{Path: i, Value: v}
	}
	return pvs
}

func (f *lazyRowsField) Next() (any, bool) {
	f.l.mu.Lock()
	drained := f.l.drained
	redact := f.l.redact
	if redact || drained || f.l.rowsStreamDone {
		if f.mat == nil {
			rows := append([]any(nil), f.l.materializedRows...)
			f.l.mu.Unlock()
			f.mat = materializedRowsIter(rows)
			return f.mat.Next()
		}
		f.l.mu.Unlock()
		return f.mat.Next()
	}
	f.l.mu.Unlock()

	f.l.ioMu.Lock()
	v, ok := f.l.rows.nextUnlocked()
	f.l.ioMu.Unlock()
	if !ok {
		if err, isErr := v.(error); isErr {
			return err, true
		}
		f.l.mu.Lock()
		f.l.rowsStreamDone = true
		f.l.mu.Unlock()
		return nil, false
	}
	if err, isErr := v.(error); isErr {
		return err, true
	}
	f.l.mu.Lock()
	f.l.materializedRows = append(f.l.materializedRows, v)
	f.l.mu.Unlock()
	return v, true
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
