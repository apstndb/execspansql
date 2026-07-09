package params

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apstndb/memebridge/cliparams"
	"github.com/goccy/go-yaml"
)

const (
	paramFlagSeparator       = "="
	legacyParamFlagSeparator = ":"
)

// ParseParamFlags parses repeated --param flags as name=value (preferred) or
// legacy name:value assignments.
func ParseParamFlags(ss []string) (map[string]string, error) {
	if len(ss) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(ss))
	for _, s := range ss {
		name, value, err := splitParamFlag(s)
		if err != nil {
			return nil, fmt.Errorf("invalid --param %q: %w", s, err)
		}
		out[name] = value
	}
	return out, nil
}

func splitParamFlag(s string) (name, value string, err error) {
	eqIdx := strings.Index(s, paramFlagSeparator)
	colonIdx := strings.Index(s, legacyParamFlagSeparator)

	switch {
	case eqIdx >= 0 && (colonIdx < 0 || eqIdx < colonIdx):
		return cliparams.SplitAssignment(s, cliparams.WithSeparator(paramFlagSeparator))
	case colonIdx >= 0:
		return cliparams.SplitAssignment(s, cliparams.WithSeparator(legacyParamFlagSeparator))
	default:
		return "", "", fmt.Errorf("expected name=value or name:value")
	}
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
		if raw == nil {
			return nil, fmt.Errorf("parse param file as JSON: top-level value must be a mapping")
		}
		if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
			if err != nil {
				return nil, fmt.Errorf("parse param file as JSON: %w", err)
			}
			return nil, fmt.Errorf("parse param file as JSON: multiple top-level values")
		}
	default:
		return loadParamYAMLFile(b)
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

func loadParamYAMLFile(b []byte) (map[string]string, error) {
	var raw map[string]yaml.RawMessage
	dec := yaml.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("parse param file as YAML: %w", err)
	}
	if raw == nil {
		return nil, fmt.Errorf("parse param file as YAML: top-level value must be a mapping")
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, fmt.Errorf("parse param file as YAML: %w", err)
		}
		return nil, fmt.Errorf("parse param file as YAML: multiple documents are not supported")
	}
	out := make(map[string]string, len(raw))
	for k, msg := range raw {
		s, err := paramFileYAMLValueToString(msg)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", k, err)
		}
		out[k] = s
	}
	return out, nil
}

func paramFileYAMLValueToString(raw yaml.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return "", fmt.Errorf("empty value")
	}
	if trimmed[0] == '"' || trimmed[0] == '\'' {
		var s string
		if err := yaml.Unmarshal(raw, &s); err != nil {
			return "", err
		}
		return s, nil
	}
	var t time.Time
	if err := yaml.Unmarshal(raw, &t); err == nil && !t.IsZero() {
		return fmt.Sprintf("TIMESTAMP %q", t.Format(time.RFC3339Nano)), nil
	}
	var v any
	if err := yaml.Unmarshal(raw, &v); err != nil {
		return "", err
	}
	return paramFileValueToString(v)
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
		return formatParamFloat(x)
	case float32:
		return formatParamFloat(float64(x))
	case time.Time:
		return fmt.Sprintf("TIMESTAMP %q", x.Format(time.RFC3339Nano)), nil
	case []any, map[string]any, map[any]any:
		return "", fmt.Errorf("arrays and maps must be specified as string literals (e.g., '[1, 2, 3]'), got %T", v)
	default:
		return fmt.Sprintf("%v", x), nil
	}
}

func formatParamFloat(x float64) (string, error) {
	if math.IsNaN(x) {
		return "", fmt.Errorf("NaN is not a valid parameter value")
	}
	if math.IsInf(x, 0) {
		return "", fmt.Errorf("infinity is not a valid parameter value")
	}
	s := fmt.Sprintf("%g", x)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return s, nil
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
