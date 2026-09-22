//go:build !windows

package main

import (
	"context"
	"os/exec"
)

func windowsBatchCommand(ctx context.Context, bin string, args []string) *exec.Cmd {
	return exec.CommandContext(ctx, bin, args...)
}
