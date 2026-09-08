package jqresult

import (
	"fmt"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/wader/gojq"
)

// Execute runs jq. For eager mode, rs must be set and rowIter is ignored.
// For lazy mode, rowIter must be unread; cleanup releases the iterator state.
// Lazy mode is intended for read-only queries; read-write callers should
// materialize first and use eager mode.
// opts apply only to lazy mode (for example WithOmitQueryPlan).
func Execute(code *gojq.Code, mode InputMode, rowIter *spanner.RowIterator, rs *sppb.ResultSet, redactRows bool, opts ...LazyOption) (gojq.Iter, func(), error) {
	switch mode {
	case InputEager:
		if rs == nil {
			return nil, func() {}, fmt.Errorf("eager mode requires a materialized ResultSet")
		}
		m, err := ResultSetMap(rs)
		if err != nil {
			return nil, func() {}, err
		}
		return code.Run(m), func() {}, nil
	case InputLazy:
		if rowIter == nil {
			return nil, func() {}, fmt.Errorf("lazy mode requires an unread RowIterator")
		}
		lazy := NewLazy(rowIter, redactRows, opts...)
		return code.Run(lazy), lazy.Stop, nil
	default:
		return nil, func() {}, fmt.Errorf("unknown jq input mode: %s", mode)
	}
}
