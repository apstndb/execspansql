package params

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ParseParamFlags parses repeated --param name:value flags.
func ParseParamFlags(ss []string) (map[string]string, error) {
	if len(ss) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(ss))
	for _, s := range ss {
		name, value, ok := strings.Cut(s, ":")
		if !ok || name == "" {
			return nil, fmt.Errorf("invalid --param %q: expected name:value", s)
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
	var raw map[string]any
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		dec := json.NewDecoder(strings.NewReader(string(b)))
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
		if v == nil {
			continue
		}
		s, err := paramFileValueToString(v)
		if err != nil {
			return nil, err
		}
		out[k] = s
	}
	return out, nil
}

func paramFileValueToString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case json.Number:
		return x.String(), nil
	case bool:
		if x {
			return "TRUE", nil
		}
		return "FALSE", nil
	default:
		return fmt.Sprintf("%v", x), nil
	}
}

// MergeParams returns file params with cli params overriding on name conflict.
func MergeParams(file, cli map[string]string) map[string]string {
	out := make(map[string]string)
	maps.Copy(out, file)
	maps.Copy(out, cli)
	return out
}
