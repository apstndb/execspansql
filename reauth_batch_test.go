package main

import (
	"strings"
	"testing"
)

func TestWindowsBatchDetectionAndQuoting(t *testing.T) {
	if !isWindowsBatch(`C:\Program Files\Google\Cloud SDK\bin\gcloud.cmd`) || !isWindowsBatch(`gcloud.bat`) {
		t.Fatal("cmd and bat files must use cmd.exe")
	}
	if isWindowsBatch(`C:\Program Files\gcloud.exe`) || isWindowsBatch(`gcloud`) {
		t.Fatal("executables must not be wrapped in cmd.exe")
	}
	bin := `C:\Program Files\Google\Cloud SDK\bin\gcloud.cmd`
	line := windowsCmdCommandLine(bin, []string{"auth", "application-default", "login", "--no-launch-browser"})
	want := `/d /s /c ""C:\Program Files\Google\Cloud SDK\bin\gcloud.cmd" auth application-default login --no-launch-browser"`
	if line != want {
		t.Fatalf("line = %q, want %q", line, want)
	}
	meta := windowsCmdCommandLine(`C:\tools\gcloud.cmd`, []string{`a&b`, `x(y)`})
	if !strings.Contains(meta, `"a&b"`) || !strings.Contains(meta, `"x(y)"`) {
		t.Fatalf("metacharacters were not quoted: %s", meta)
	}
}
