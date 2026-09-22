package main

import (
	"strings"
	"testing"
)

func TestParamFlagKeepsCommasThroughCLI(t *testing.T) {
	values := []string{
		`arr=[1, 2]`,
		`s='a,b'`,
		`x=STRUCT<a INT64,b STRING>(1,'two')`,
	}
	args := []string{"db", "--project=p", "--instance=i", "--sql=SELECT @arr"}
	for _, value := range values {
		args = append(args, "--param="+value)
	}
	o, err := processFlags(args)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(o.ParamFlags, "\n") != strings.Join(values, "\n") {
		t.Fatalf("ParamFlags = %#v, want one entry per flag", o.ParamFlags)
	}
	if _, err := prepareCommand(o); err != nil {
		t.Fatalf("prepareCommand: %v", err)
	}
}

func TestParamFlagDoesNotSplitCommaSeparatedAssignments(t *testing.T) {
	o, err := processFlags([]string{
		"db", "--project=p", "--instance=i", "--sql=SELECT @n, @m",
		"--param=n=1,m=2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(o.ParamFlags) != 1 || o.ParamFlags[0] != "n=1,m=2" {
		t.Fatalf("ParamFlags = %#v, want a single unsplit assignment", o.ParamFlags)
	}
}
