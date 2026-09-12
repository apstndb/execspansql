package planrender

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spannerplan/plantree/reference"
	"github.com/apstndb/spannerplanviz/d2"
	"github.com/apstndb/spannerplanviz/dot"
	"github.com/apstndb/spannerplanviz/graphviz"
	"github.com/apstndb/spannerplanviz/mermaid"
	"github.com/apstndb/spannerplanviz/visualize"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// ErrNoQueryPlan is returned when stats does not contain a usable query plan.
var ErrNoQueryPlan = errors.New("no query plan in result; requires --query-mode=PLAN, PROFILE, or WITH_PLAN_AND_STATS")

// Options configures plan rendering. Zero values select defaults.
//
// TextStyle, WrapWidth, and PrintSections apply only to FormatText.
// Full, ShowQuery, ShowQueryStats, and Query apply only to graph formats.
// Render rejects combinations that do not apply to the chosen format
// rather than silently ignoring them.
type Options struct {
	// TextStyle is current, traditional, or compact. Empty means current.
	TextStyle string
	// WrapWidth is passed to the text renderer. 0 disables wrapping.
	WrapWidth int
	// PrintSections is basic, enhanced, full, none, or an explicit
	// comma-separated section list. Empty means basic.
	PrintSections string
	// Full selects visualize.FullBuildOptions instead of StructureBuildOptions.
	Full bool
	// ShowQuery adds a query-text node to graph output.
	ShowQuery bool
	// ShowQueryStats adds query statistics to that node.
	ShowQueryStats bool
	// Query is injected as query_stats.query_text when ShowQuery is true.
	// It is ignored when empty; a non-empty value without ShowQuery is an error.
	Query string
}

// Render writes a query plan in format to w.
//
// rowType may be nil. stats must contain a non-empty query plan.
// Traversal-budget and cycle-detection errors from spannerplan are returned
// as-is (wrapped). The text renderer has no context parameter; graph renderers
// receive ctx and honor cancellation where the libraries check it.
func Render(ctx context.Context, w io.Writer, format Format, rowType *sppb.StructType, stats *sppb.ResultSetStats, opts Options) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w == nil {
		return errors.New("writer is nil")
	}
	if !format.isGraph() && format != FormatText {
		return fmt.Errorf("unknown plan format %q", format)
	}
	if err := opts.validate(format); err != nil {
		return err
	}
	if stats == nil || stats.GetQueryPlan() == nil || len(stats.GetQueryPlan().GetPlanNodes()) == 0 {
		return ErrNoQueryPlan
	}

	if format == FormatText {
		return renderText(w, stats, opts)
	}
	return renderGraph(ctx, w, format, rowType, stats, opts)
}

// Validate reports whether opts apply to format.
func (o Options) Validate(format Format) error {
	return o.validate(format)
}

func (o Options) validate(format Format) error {
	if o.WrapWidth < 0 {
		return fmt.Errorf("WrapWidth cannot be negative: %d", o.WrapWidth)
	}
	if o.TextStyle != "" {
		if _, err := reference.ParseFormat(o.TextStyle); err != nil {
			return fmt.Errorf("invalid TextStyle %q: %w", o.TextStyle, err)
		}
	}
	if o.PrintSections != "" {
		if _, err := reference.ParsePrintSections(o.PrintSections); err != nil {
			return fmt.Errorf("invalid PrintSections %q: %w", o.PrintSections, err)
		}
	}
	if o.Query != "" && !o.ShowQuery {
		return errors.New("option Query is only used when ShowQuery is true")
	}

	var names []string
	if format == FormatText {
		if o.Full {
			names = append(names, "Full")
		}
		if o.ShowQuery {
			names = append(names, "ShowQuery")
		}
		if o.ShowQueryStats {
			names = append(names, "ShowQueryStats")
		}
		if o.Query != "" {
			names = append(names, "Query")
		}
	} else {
		if o.TextStyle != "" {
			names = append(names, "TextStyle")
		}
		if o.WrapWidth != 0 {
			names = append(names, "WrapWidth")
		}
		if o.PrintSections != "" {
			names = append(names, "PrintSections")
		}
	}
	if len(names) == 0 {
		return nil
	}
	return fmt.Errorf("options %s do not apply to plan format %q", strings.Join(names, ", "), format)
}

func renderText(w io.Writer, stats *sppb.ResultSetStats, opts Options) error {
	styleName := opts.TextStyle
	if styleName == "" {
		styleName = "current"
	}
	style, err := reference.ParseFormat(styleName)
	if err != nil {
		return fmt.Errorf("invalid TextStyle %q: %w", opts.TextStyle, err)
	}

	printSections := opts.PrintSections
	if printSections == "" {
		printSections = "basic"
	}
	sections, err := reference.ParsePrintSections(printSections)
	if err != nil {
		return fmt.Errorf("invalid PrintSections %q: %w", opts.PrintSections, err)
	}

	text, err := reference.RenderTreeTableWithOptions(
		stats.GetQueryPlan().GetPlanNodes(),
		reference.RenderModeAuto,
		style,
		reference.WithWrapWidth(opts.WrapWidth),
		reference.WithPrintSections(sections...),
	)
	if err != nil {
		return fmt.Errorf("render text plan: %w", err)
	}
	if _, err := io.WriteString(w, text); err != nil {
		return fmt.Errorf("write plan: %w", err)
	}
	return nil
}

func renderGraph(ctx context.Context, w io.Writer, format Format, rowType *sppb.StructType, stats *sppb.ResultSetStats, opts Options) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	stats = statsWithQuery(stats, opts)
	buildOpts := visualize.StructureBuildOptions()
	if opts.Full {
		buildOpts = visualize.FullBuildOptions()
	}

	plan, err := visualize.BuildPlan(rowType, stats, buildOpts)
	if err != nil {
		return fmt.Errorf("build plan graph: %w", err)
	}

	switch format {
	case FormatDOT:
		return dot.NewRenderer(dot.Options{
			ShowQuery:      opts.ShowQuery,
			ShowQueryStats: opts.ShowQueryStats,
		}).Render(ctx, w, plan)
	case FormatMermaid:
		return mermaid.NewRenderer(mermaid.Options{
			BuildOptions:   buildOpts,
			ShowQuery:      opts.ShowQuery,
			ShowQueryStats: opts.ShowQueryStats,
		}).Render(ctx, w, plan)
	case FormatD2:
		return d2.NewRenderer(d2.Options{
			BuildOptions:   buildOpts,
			ShowQuery:      opts.ShowQuery,
			ShowQueryStats: opts.ShowQueryStats,
		}).Render(ctx, w, plan)
	case FormatSVG, FormatPNG:
		gvFormat := graphviz.SVG
		if format == FormatPNG {
			gvFormat = graphviz.PNG
		}
		// graphviz.Renderer.Render (v0.11.0) calls graphviz.New(ctx) and
		// defers Close on both the runtime and the parsed graph.
		return graphviz.NewRenderer(graphviz.Options{
			Format:         gvFormat,
			ShowQuery:      opts.ShowQuery,
			ShowQueryStats: opts.ShowQueryStats,
		}).Render(ctx, w, plan)
	default:
		return fmt.Errorf("unknown plan format %q", format)
	}
}

func statsWithQuery(stats *sppb.ResultSetStats, opts Options) *sppb.ResultSetStats {
	if !opts.ShowQuery || opts.Query == "" {
		return stats
	}
	cloned := proto.CloneOf(stats)
	if cloned.QueryStats == nil {
		cloned.QueryStats = &structpb.Struct{}
	}
	if cloned.QueryStats.Fields == nil {
		cloned.QueryStats.Fields = make(map[string]*structpb.Value)
	}
	cloned.QueryStats.Fields["query_text"] = structpb.NewStringValue(opts.Query)
	return cloned
}
