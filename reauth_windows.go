//go:build windows

package main

import (
	"context"
	"os"
	"os/exec"
	"syscall"
)

func windowsBatchCommand(ctx context.Context, bin string, args []string) *exec.Cmd {
	comspec := os.Getenv("ComSpec")
	if comspec == "" {
		comspec = `C:\Windows\System32\cmd.exe`
	}
	cmd := exec.CommandContext(ctx, comspec)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CmdLine: comspec + " " + windowsCmdCommandLine(bin, args),
	}
	return cmd
}
