// Package planrender renders Cloud Spanner query plans through a thin adapter
// over github.com/apstndb/spannerplan and github.com/apstndb/spannerplanviz.
//
// The exported API accepts only this package's Format and Options types plus
// spannerpb messages (metadata.rowType and ResultSetStats). It does not leak
// those libraries' types. Both libraries are v0 and documented as experimental;
// this adapter is the CLI boundary while those APIs evolve.
//
// Graphviz SVG/PNG rendering uses github.com/goccy/go-graphviz's WASM runtime
// (wazero). The v0.11.0 graphviz.Renderer constructs that runtime per Render call
// and closes it before returning; this package does not retain it.
package planrender
