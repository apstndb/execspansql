package params

import (
	"github.com/apstndb/memebridge/cliparams"
)

// GenerateParams returns map for spanner.Statement.Params.
// All values are spanner.GenericColumnValue.
func GenerateParams(ss map[string]string, permitType bool) (map[string]any, error) {
	var opts []cliparams.Option
	if permitType {
		opts = append(opts, cliparams.WithBareTypeAsNull())
	}
	parsed, err := cliparams.ParseMap(ss, opts...)
	if err != nil {
		return nil, err
	}
	return cliparams.StatementParams(parsed), nil
}
