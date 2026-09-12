package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/execspansql/jqresult"
	svwriter "github.com/apstndb/spanvalue/writer"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	destStdoutDash = "-"
	destDevStdout  = "/dev/stdout"
	destDevStderr  = "/dev/stderr"

	planModesHelp = "PLAN, PROFILE, or WITH_PLAN_AND_STATS"
)

var errNoQueryPlan = errors.New("query returned no query plan (nil or empty planNodes); not retrying the statement")

type destKind int

const (
	destKindStdout destKind = iota
	destKindStderr
	destKindFile
)

type resolvedDest struct {
	kind destKind
	raw  string
	abs  string
}

func defaultPrimaryOutput(raw string) string {
	if raw == "" {
		return destStdoutDash
	}
	return raw
}

func resolveDestination(raw string) resolvedDest {
	switch raw {
	case destStdoutDash, destDevStdout:
		return resolvedDest{kind: destKindStdout, raw: raw}
	case destDevStderr:
		return resolvedDest{kind: destKindStderr, raw: raw}
	default:
		abs, err := filepath.Abs(filepath.Clean(raw))
		if err != nil {
			abs = filepath.Clean(raw)
		}
		return resolvedDest{kind: destKindFile, raw: raw, abs: abs}
	}
}

func destLabel(d resolvedDest) string {
	switch d.kind {
	case destKindStdout:
		return "stdout"
	case destKindStderr:
		return "stderr"
	default:
		if d.abs != "" {
			return d.abs
		}
		return d.raw
	}
}

func isPlanProducingQueryMode(mode string) bool {
	switch mode {
	case "PLAN", "PROFILE", "WITH_PLAN_AND_STATS":
		return true
	default:
		return false
	}
}

func effectivePlanFormat(o opts) string {
	if o.PlanFormat != "" {
		return o.PlanFormat
	}
	switch o.Format {
	case "json", "yaml":
		return o.Format
	default:
		return "json"
	}
}

func validatePlanOutputOptions(o opts) error {
	hasPlan := o.PlanOutput != ""
	if o.PlanFormat != "" && !hasPlan {
		return fmt.Errorf("--plan-format requires --plan-output")
	}
	if o.DiscardResults && !hasPlan {
		return fmt.Errorf("--discard-results requires --plan-output")
	}
	if o.PlanFormat != "" && o.PlanFormat != "json" && o.PlanFormat != "yaml" {
		return fmt.Errorf("--plan-format must be json or yaml")
	}
	if !hasPlan {
		return nil
	}
	mode := o.QueryMode
	if mode == "" {
		mode = "NORMAL"
	}
	if !isPlanProducingQueryMode(mode) {
		return fmt.Errorf("--plan-output requires --query-mode=%s", planModesHelp)
	}
	if o.TryPartitionQuery {
		return fmt.Errorf("--plan-output cannot be combined with --try-partition-query")
	}
	if o.EnablePartitionedDML {
		return fmt.Errorf("--plan-output cannot be combined with --enable-partitioned-dml")
	}
	return nil
}

func validateDestinations(o opts) error {
	primaryRaw := defaultPrimaryOutput(o.Output)
	primary := resolveDestination(primaryRaw)
	var plan *resolvedDest
	if o.PlanOutput != "" {
		d := resolveDestination(o.PlanOutput)
		plan = &d
	}

	if plan != nil && !o.DiscardResults {
		if primary.kind == destKindStdout && plan.kind == destKindStdout {
			return fmt.Errorf("in split mode --output and --plan-output cannot both write to stdout")
		}
		if primary.kind == destKindStderr && plan.kind == destKindStderr {
			return fmt.Errorf("in split mode --output and --plan-output cannot both write to stderr")
		}
		if primary.kind == destKindFile && plan.kind == destKindFile {
			same, err := sameOutputFile(primary.raw, plan.raw)
			if err != nil {
				return err
			}
			if same {
				return fmt.Errorf("--output and --plan-output cannot target the same file")
			}
		}
	}

	inputs := []struct {
		flag, path string
	}{
		{"--sql-file", o.SqlFile},
		{"--param-file", o.ParamFile},
		{"--filter-file", o.JqFromFile},
	}

	checkAlias := func(flagName, destRaw string, d resolvedDest) error {
		if d.kind != destKindFile {
			return nil
		}
		for _, in := range inputs {
			if in.path == "" {
				continue
			}
			same, err := sameOutputFile(destRaw, in.path)
			if err != nil {
				return err
			}
			if same {
				return fmt.Errorf("%s cannot alias %s", flagName, in.flag)
			}
		}
		return nil
	}

	if !o.DiscardResults {
		if err := checkAlias("--output", primaryRaw, primary); err != nil {
			return err
		}
	}
	if plan != nil {
		if err := checkAlias("--plan-output", o.PlanOutput, *plan); err != nil {
			return err
		}
	}
	return nil
}

func fileIdentity(path string) (abs string, info os.FileInfo, exists bool, err error) {
	abs, err = filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", nil, false, err
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		if errors.Is(statErr, os.ErrNotExist) {
			return abs, nil, false, nil
		}
		return abs, nil, false, statErr
	}
	if eval, evalErr := filepath.EvalSymlinks(path); evalErr == nil {
		if evalAbs, absErr := filepath.Abs(eval); absErr == nil {
			abs = evalAbs
		}
	}
	return abs, info, true, nil
}

func sameOutputFile(a, b string) (bool, error) {
	absA, infoA, existsA, err := fileIdentity(a)
	if err != nil {
		return false, err
	}
	absB, infoB, existsB, err := fileIdentity(b)
	if err != nil {
		return false, err
	}
	if absA == absB {
		return true, nil
	}
	if existsA && existsB {
		return os.SameFile(infoA, infoB), nil
	}
	return false, nil
}

// fileSink is a regular-file destination written via a sibling temp file.
type fileSink struct {
	file  *os.File
	final string
}

type outputSinks struct {
	primary io.Writer
	plan    io.Writer

	primaryDest resolvedDest
	planDest    resolvedDest
	hasPlan     bool
	discard     bool

	primaryFile *fileSink
	planFile    *fileSink

	primaryReady bool
	done         bool
}

func newOutputSinks(o opts) (*outputSinks, error) {
	if err := validateDestinations(o); err != nil {
		return nil, err
	}
	s := &outputSinks{discard: o.DiscardResults}
	if o.DiscardResults {
		s.primaryReady = true
	} else {
		d := resolveDestination(defaultPrimaryOutput(o.Output))
		w, file, err := openDestination(d)
		if err != nil {
			return nil, fmt.Errorf("--output: %w", err)
		}
		s.primary = w
		s.primaryDest = d
		s.primaryFile = file
	}
	if o.PlanOutput != "" {
		d := resolveDestination(o.PlanOutput)
		w, file, err := openDestination(d)
		if err != nil {
			s.Abort()
			return nil, fmt.Errorf("--plan-output: %w", err)
		}
		s.plan = w
		s.planDest = d
		s.planFile = file
		s.hasPlan = true
	}
	return s, nil
}

func openDestination(d resolvedDest) (io.Writer, *fileSink, error) {
	switch d.kind {
	case destKindStdout:
		return os.Stdout, nil, nil
	case destKindStderr:
		return os.Stderr, nil, nil
	case destKindFile:
		dir := filepath.Dir(d.abs)
		tmp, err := os.CreateTemp(dir, ".execspansql-*.tmp")
		if err != nil {
			return nil, nil, err
		}
		if err := tmp.Chmod(0o600); err != nil {
			name := tmp.Name()
			_ = tmp.Close()
			_ = os.Remove(name)
			return nil, nil, err
		}
		return tmp, &fileSink{file: tmp, final: d.abs}, nil
	default:
		return nil, nil, fmt.Errorf("unknown destination kind")
	}
}

func (s *outputSinks) MarkPrimaryComplete() {
	if s == nil {
		return
	}
	s.primaryReady = true
}

func (s *outputSinks) primaryLabel() string {
	if s == nil || s.discard {
		return ""
	}
	return destLabel(s.primaryDest)
}

func abortFileSink(fs *fileSink) {
	if fs == nil || fs.file == nil {
		return
	}
	name := fs.file.Name()
	_ = fs.file.Close()
	_ = os.Remove(name)
	fs.file = nil
}

func publishFileSink(fs *fileSink) error {
	if fs == nil || fs.file == nil {
		return nil
	}
	name := fs.file.Name()
	if err := fs.file.Close(); err != nil {
		_ = os.Remove(name)
		fs.file = nil
		return err
	}
	fs.file = nil
	if err := os.Rename(name, fs.final); err != nil {
		_ = os.Remove(name)
		return err
	}
	return nil
}

func (s *outputSinks) Abort() {
	if s == nil || s.done {
		return
	}
	s.done = true
	abortFileSink(s.primaryFile)
	abortFileSink(s.planFile)
}

func (s *outputSinks) Finish(workErr error) error {
	if s == nil {
		return workErr
	}
	if workErr != nil && !s.primaryReady {
		s.Abort()
		return workErr
	}
	if workErr != nil {
		pubErr := publishFileSink(s.primaryFile)
		abortFileSink(s.planFile)
		s.done = true
		if pubErr != nil {
			return fmt.Errorf("%v; also failed to publish primary output: %w", workErr, pubErr)
		}
		if label := s.primaryLabel(); label != "" {
			return fmt.Errorf("primary output written to %s; %w", label, workErr)
		}
		return workErr
	}
	if err := publishFileSink(s.primaryFile); err != nil {
		abortFileSink(s.planFile)
		s.done = true
		return err
	}
	if err := publishFileSink(s.planFile); err != nil {
		s.done = true
		if label := s.primaryLabel(); label != "" {
			return fmt.Errorf("primary output written to %s; failed to publish plan output: %w", label, err)
		}
		return fmt.Errorf("failed to publish plan output: %w", err)
	}
	s.done = true
	return nil
}

func wrapCommittedOutputError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("output failed after the statement was committed; this is not a rollback and the SQL is not replayed: %w", err)
}

func hasUsableQueryPlan(stats *sppb.ResultSetStats) bool {
	if stats == nil || stats.GetQueryPlan() == nil {
		return false
	}
	return len(stats.GetQueryPlan().GetPlanNodes()) > 0
}

// stripQueryPlanForPrimary clones stats for the plan artifact and clears QueryPlan
// on a separate clone assigned to rs. The original stats message is not mutated.
func stripQueryPlanForPrimary(rs *sppb.ResultSet) (planStats *sppb.ResultSetStats, metadata *sppb.ResultSetMetadata) {
	if rs == nil {
		return nil, nil
	}
	metadata = rs.Metadata
	if rs.Stats == nil {
		return nil, metadata
	}
	planStats, ok := proto.Clone(rs.Stats).(*sppb.ResultSetStats)
	if !ok || planStats == nil {
		return nil, metadata
	}
	primaryStats, ok := proto.Clone(rs.Stats).(*sppb.ResultSetStats)
	if !ok || primaryStats == nil {
		return planStats, metadata
	}
	primaryStats.QueryPlan = nil
	rs.Stats = primaryStats
	return planStats, metadata
}

func writePlan(w io.Writer, format string, metadata *sppb.ResultSetMetadata, stats *sppb.ResultSetStats) error {
	if w == nil {
		return nil
	}
	if !hasUsableQueryPlan(stats) {
		return errNoQueryPlan
	}
	envelope := &sppb.ResultSet{
		Metadata: metadata,
		Stats:    stats,
	}
	m, err := jqresult.ProtoToMap(envelope)
	if err != nil {
		return err
	}
	enc, err := newEncoder(w, format, false, false)
	if err != nil {
		return err
	}
	if err := enc.Encode(m); err != nil {
		_ = closeEncoder(enc)
		return err
	}
	return closeEncoder(enc)
}

func statsFromWriterResult(r *svwriter.RowIteratorResult) (*sppb.ResultSetStats, error) {
	if r == nil {
		return nil, nil
	}
	stats := &sppb.ResultSetStats{
		QueryPlan: r.Stats.QueryPlan,
	}
	if r.Stats.QueryStats != nil {
		qs, err := structpb.NewStruct(r.Stats.QueryStats)
		if err != nil {
			return nil, fmt.Errorf("encode query stats: %w", err)
		}
		stats.QueryStats = qs
	}
	if stats.QueryPlan == nil && stats.QueryStats == nil && stats.RowCount == nil {
		return nil, nil
	}
	return stats, nil
}
