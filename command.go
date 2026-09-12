package main

import (
	"fmt"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/execspansql/jqresult"
	"github.com/apstndb/execspansql/params"
	"github.com/wader/gojq"
)

// preparedCommand holds validated options and resolved inputs. Preparation does
// not authenticate, create a client, or open output files.
type preparedCommand struct {
	opts
	statement    spanner.Statement
	queryOptions spanner.QueryOptions
	mode         queryMode
	jqMode       jqresult.InputMode
	jqCode       *gojq.Code
}

func prepareCommand(o opts) (*preparedCommand, error) {
	jqMode, err := jqresult.ParseInputMode(o.JqInputMode)
	if err != nil {
		return nil, err
	}
	if err := jqMode.ValidateFormat(o.Format); err != nil {
		return nil, err
	}
	if err := validateJqOutputOptions(o, jqMode); err != nil {
		return nil, err
	}

	var jqCode *gojq.Code
	if !o.TryPartitionQuery && o.Format != "experimental_csv" {
		jqFilter, err := readFileOrDefault(o.JqFromFile, o.JqFilter)
		if err != nil {
			return nil, err
		}
		if jqFilter == "" {
			jqFilter = jqresult.DefaultFilter(jqMode)
		}

		jqCode, err = jqresult.Compile(jqFilter, jqMode)
		if err != nil {
			return nil, err
		}
	}

	mode := sppb.ExecuteSqlRequest_QueryMode(sppb.ExecuteSqlRequest_QueryMode_value[o.QueryMode])
	queryOpts := queryOptionsFor(mode, o.Priority)

	query, err := readFileOrDefault(o.SqlFile, o.Sql)
	if err != nil {
		return nil, err
	}

	tb, err := parseTimestampBound(o.TimestampBound.ReadTimestamp)
	if err != nil {
		return nil, fmt.Errorf("--read-timestamp is supplied but wrong: %w", err)
	}

	m := queryModeForQuery(query, o.EnablePartitionedDML, tb)
	if err := validateExecutionOptions(o, m); err != nil {
		return nil, err
	}

	// Freeze the statement (SQL and parameters) before any interactive step so
	// a parameter file edited during a browser login cannot change what runs.
	paramStrMap, err := o.mergedParams()
	if err != nil {
		return nil, err
	}
	paramMap, err := params.GenerateParams(paramStrMap, mode == sppb.ExecuteSqlRequest_PLAN)
	if err != nil {
		return nil, err
	}
	return &preparedCommand{
		opts:         o,
		statement:    spanner.Statement{SQL: query, Params: paramMap},
		queryOptions: queryOpts,
		mode:         m,
		jqMode:       jqMode,
		jqCode:       jqCode,
	}, nil
}
