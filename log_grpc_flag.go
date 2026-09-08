package main

import (
	"strings"

	"github.com/alecthomas/kong"
)

// logGrpcFlag retains the old boolean flag syntax while accepting explicit
// logging modes. As with boolean flags, explicit values use "=" so a bare
// flag never consumes the following database argument.
type logGrpcFlag string

func (*logGrpcFlag) IsBool() bool { return true }

func (m *logGrpcFlag) Decode(ctx *kong.DecodeContext) error {
	if ctx.Scan.Peek().Type != kong.FlagValueToken {
		*m = logGrpcModePayload
		return nil
	}
	var value string
	if err := ctx.Scan.PopValueInto("logging mode", &value); err != nil {
		return err
	}
	// Preserve Kong's previous boolean spellings as well as the bare flag.
	// The enum tag validates explicit mode names after this normalization.
	switch strings.ToLower(value) {
	case "true", "1", "yes":
		value = logGrpcModePayload
	case "false", "0", "no":
		value = logGrpcModeOff
	}
	*m = logGrpcFlag(value)
	return nil
}
