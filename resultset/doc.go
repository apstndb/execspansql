// Package resultset drains Cloud Spanner row iterators into protobuf ResultSets.
//
// It composes [github.com/apstndb/spaniter] for iterator lifecycle and stats
// conversion. Callers that need jq-shaped maps should use
// [github.com/apstndb/execspansql/jqresult] on top of the returned ResultSet.
package resultset
