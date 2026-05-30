package jqresult

import "fmt"

// InputMode selects how Spanner query results are passed to gojq.
type InputMode string

const (
	InputEager InputMode = "eager"
	InputLazy  InputMode = "lazy"
)

func ParseInputMode(s string) (InputMode, error) {
	switch InputMode(s) {
	case InputEager, InputLazy:
		return InputMode(s), nil
	default:
		return "", fmt.Errorf("jq-input-mode must be eager or lazy")
	}
}

// DefaultFilter returns the filter used when the user passes an empty --filter.
func DefaultFilter(InputMode) string {
	return "."
}

// ValidateFormat returns an error when mode is incompatible with the output format.
func (m InputMode) ValidateFormat(format string) error {
	if format == "experimental_csv" && m != InputEager {
		return fmt.Errorf("--jq-input-mode=%s is only supported with --format=json or yaml", m)
	}
	return nil
}
