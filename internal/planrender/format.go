package planrender

import (
	"fmt"
	"strings"
)

// Format is a plan-rendering output format.
type Format string

const (
	FormatText    Format = "text"
	FormatDOT     Format = "dot"
	FormatMermaid Format = "mermaid"
	FormatD2      Format = "d2"
	FormatSVG     Format = "svg"
	FormatPNG     Format = "png"
)

// ParseFormat parses a plan-rendering format name.
// Valid values are text, dot, mermaid, d2, svg, and png (case-insensitive).
func ParseFormat(s string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case string(FormatText):
		return FormatText, nil
	case string(FormatDOT):
		return FormatDOT, nil
	case string(FormatMermaid):
		return FormatMermaid, nil
	case string(FormatD2):
		return FormatD2, nil
	case string(FormatSVG):
		return FormatSVG, nil
	case string(FormatPNG):
		return FormatPNG, nil
	default:
		return "", fmt.Errorf("unknown plan format %q; want text, dot, mermaid, d2, svg, or png", s)
	}
}

// IsBinary reports whether format is a binary image (PNG). SVG is text XML.
func (f Format) IsBinary() bool {
	return f == FormatPNG
}

// NeedsGraphviz reports whether format requires the embedded Graphviz/WASM runtime.
func (f Format) NeedsGraphviz() bool {
	return f == FormatSVG || f == FormatPNG
}

func (f Format) isGraph() bool {
	switch f {
	case FormatDOT, FormatMermaid, FormatD2, FormatSVG, FormatPNG:
		return true
	default:
		return false
	}
}
