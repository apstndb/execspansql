package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRunGcloudADCLoginWindowsFakeCmd(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("requires a Windows command interpreter to launch a .cmd file")
	}
	dir := filepath.Join(t.TempDir(), "Cloud SDK")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	argvPath := filepath.Join(dir, "argv.txt")
	script := "@echo off\r\n" +
		"echo %1>>" + windowsQuote(argvPath) + "\r\n" +
		"echo %2>>" + windowsQuote(argvPath) + "\r\n" +
		"echo %3>>" + windowsQuote(argvPath) + "\r\n" +
		"echo %4>>" + windowsQuote(argvPath) + "\r\n" +
		"echo gcloud-login-stdout\r\n"
	gcloudPath := filepath.Join(dir, "gcloud.cmd")
	if err := os.WriteFile(gcloudPath, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("PATHEXT", ".CMD;.EXE")
	t.Setenv(envSSHConnection, "203.0.113.1 60000 203.0.113.2 22")

	err := runGcloudADCLogin(context.Background(), os.Getenv, func(string) (string, error) {
		return gcloudPath, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(argvPath)
	if err != nil {
		t.Fatal(err)
	}
	want := "auth\r\napplication-default\r\nlogin\r\n--no-launch-browser\r\n"
	if strings.ReplaceAll(string(got), "\r\n", "\n") != strings.ReplaceAll(want, "\r\n", "\n") {
		t.Fatalf("argv = %q, want %q", got, want)
	}
}

func windowsQuote(path string) string {
	return `"` + path + `"`
}
