# AGENTS.md

## Project overview

`execspansql` is a Go CLI for Spanner query execution. It supports SQL parameter loading, multiple output formats (JSON/YAML/CSV), optional JQ filtering, and tracing options.

Primary package layout:
- `main.go` / `trace.go` - CLI entrypoint, command wiring, execution flow.
- `params/` - parameter file parsing and typed conversion helpers.
- `resultset/` - Spanner result set materialization and formatting helpers.
- `jqresult/` - JQ compile/execution pipeline and JSON conversion helpers.
- `docs/`, `examples/`, `testdata/` - reference assets, examples, and golden fixtures.

## Key commands

- Build: `go build ./...`
- Unit/package tests: `go test ./params/... ./jqresult/... ./resultset/...`
- Full test suite: `go test ./...` (requires emulator/container environment for integration tests)
- Lint: `golangci-lint run`
- Golden files:
  - Update CSV goldens: `go test -update-golden -run TestExperimentalCsvGolden .`
  - Update YAML/profile goldens: `go test -update-golden -run TestYamlOutputGolden .`
  - Update profile YAML goldens: `go test -update-golden -run TestProfileJSONToYamlGolden .`

## Integration-test flow

`go test ./...` runs tests that exercise Spanner via testcontainers/spanner-emulator.
Run these in a Docker-capable environment only, and expect additional startup time.
For fast local checks without Docker dependency, limit to package-level tests as above.

## CI / release notes

- CI workflows include lint and Go build/test under `.github/workflows/`.
- Lint workflow uses `golangci-lint`.
- `ko` publish workflow builds on tag pushes for `v*`.
- `.goreleaser.yaml` exists and should be treated as release automation metadata.
- Release/packaging behavior follows `go.mod` Go version pin (`go 1.25.0`).

## Repository hygiene

- Keep local editor/build noise out of git:
  - `.tmp/` for scratch notes and generated review artifacts.
  - `.idea/` local IDE metadata.
  - `dist/` local distribution outputs.
- Local scratch artifacts from this project's workflow (root-level files) should remain ignored:
  - SQL/JSON examples: `enhanced_query*.sql`, `fulltext*.sql`, `singerinfo.sql`, `test.sql`, `types.sql`, `test.json`
  - Match3 experiment outputs: `match3.gql`, `match3.txt`, `match3*.png`
  - Built binary: `/execspansql` and `/execspansql.exe` on Windows
