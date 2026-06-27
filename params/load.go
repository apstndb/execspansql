package params

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apstndb/memebridge/cliparams"
	"gopkg.in/yaml.v3"
)

// ParseParamFlags parses repeated --param name:value flags.
func ParseParamFlags(ss []string) (map[string]string, error) {
	if len(ss) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(ss))
	for _, s := range ss {
		name, value, err := cliparams.SplitAssignment(s)
		if err != nil {
			return nil, fmt.Errorf("invalid --param %q: %w", s, err)
		}
		if name == "" {
			return nil, fmt.Errorf("invalid --param %q: empty parameter name", s)
		}
		out[name] = value
	}
	return out, nil
}

// LoadParamFile loads param name→literal/type strings from a YAML or JSON file.
func LoadParamFile(path string) (map[string]string, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(b)) == 0 {
		return nil, nil
	}
	var raw map[string]any
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		if err := dec.Decode(&raw); err != nil {
			return nil, fmt.Errorf("parse param file as JSON: %w", err)
		}
	default:
		if err := yaml.Unmarshal(b, &raw); err != nil {
			return nil, fmt.Errorf("parse param file as YAML: %w", err)
		}
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		s, err := paramFileValueToString(v)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", k, err)
		}
		out[k] = s
	}
	return out, nil
}

func paramFileValueToString(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "", fmt.Errorf("untyped null values are not supported; use a typed null expression like 'CAST(NULL AS TYPE)' or a type name (PLAN mode only)")
	case string:
		return x, nil
	case json.Number:
		return x.String(), nil
	case bool:
		if x {
			return "TRUE", nil
		}
		return "FALSE", nil
	case float64:
		return formatParamFloat(x), nil
	case float32:
		return formatParamFloat(float64(x)), nil
	case time.Time:
		return fmt.Sprintf("TIMESTAMP %q", x.Format(time.RFC3339Nano)), nil
	case []any, map[string]any, map[any]any:
		return "", fmt.Errorf("arrays and maps must be specified as string literals (e.g., '[1, 2, 3]'), got %T", v)
	default:
		return fmt.Sprintf("%v", x), nil
	}
}

func formatParamFloat(x float64) string {
	s := fmt.Sprintf("%g", x)
	if s == "NaN" || s == "+Inf" || s == "-Inf" {
		return s
	}
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return s
}

// MergeParams returns file params with cli params overriding on name conflict.
func MergeParams(file, cli map[string]string) map[string]string {
	if len(file) == 0 {
		return cli
	}
	if len(cli) == 0 {
		return file
	}
	out := make(map[string]string, len(file)+len(cli))
	maps.Copy(out, file)
	maps.Copy(out, cli)
	return out
}
