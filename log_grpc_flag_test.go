package main

import (
	"strings"
	"testing"

	"github.com/alecthomas/kong"
)

func TestLogGrpcFlag(t *testing.T) {
	for _, tt := range []struct {
		name    string
		args    []string
		want    string
		wantDB  string
		wantErr bool
	}{
		{name: "omitted", args: []string{"db"}, want: "off", wantDB: "db"},
		{name: "bare_before_database", args: []string{"--log-grpc", "db"}, want: "payload", wantDB: "db"},
		{name: "bare_after_database", args: []string{"db", "--log-grpc"}, want: "payload", wantDB: "db"},
		{name: "mode_named_database", args: []string{"--log-grpc", "metadata"}, want: "payload", wantDB: "metadata"},
		{name: "off", args: []string{"db", "--log-grpc=off"}, want: "off", wantDB: "db"},
		{name: "metadata", args: []string{"db", "--log-grpc=metadata"}, want: "metadata", wantDB: "db"},
		{name: "payload", args: []string{"db", "--log-grpc=payload"}, want: "payload", wantDB: "db"},
		{name: "legacy_true", args: []string{"db", "--log-grpc=true"}, want: "payload", wantDB: "db"},
		{name: "legacy_yes", args: []string{"db", "--log-grpc=YES"}, want: "payload", wantDB: "db"},
		{name: "legacy_one", args: []string{"db", "--log-grpc=1"}, want: "payload", wantDB: "db"},
		{name: "legacy_false", args: []string{"db", "--log-grpc=false"}, want: "off", wantDB: "db"},
		{name: "legacy_no", args: []string{"db", "--log-grpc=NO"}, want: "off", wantDB: "db"},
		{name: "legacy_zero", args: []string{"db", "--log-grpc=0"}, want: "off", wantDB: "db"},
		{name: "invalid", args: []string{"db", "--log-grpc=invalid"}, wantErr: true},
		{name: "empty", args: []string{"db", "--log-grpc="}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var o opts
			parser, err := kong.New(&o)
			if err != nil {
				t.Fatal(err)
			}
			args := append(append([]string{}, tt.args...), "--project=p", "--instance=i", "--sql=SELECT 1")
			_, err = parser.Parse(args)
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "--log-grpc") {
					t.Fatalf("error = %v, want invalid log mode", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(o.LogGrpc) != tt.want || o.Database != tt.wantDB {
				t.Fatalf("mode=%q database=%q, want %q %q", o.LogGrpc, o.Database, tt.want, tt.wantDB)
			}
		})
	}
}
