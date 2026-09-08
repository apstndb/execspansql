package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"github.com/alecthomas/kong"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	raptJSON         = []byte(`{"error":"invalid_grant","error_description":"reauth related error (invalid_rapt)","error_uri":"https://support.google.com/a/answer/9368756","error_subtype":"invalid_rapt"}`)
	raptRequiredJSON = []byte(`{"error":"invalid_grant","error_subtype":"rapt_required"}`)
	revokedJSON      = []byte(`{"error":"invalid_grant","error_description":"Token has been expired or revoked."}`)
)

func newAuthErr(body []byte) *auth.Error {
	// auth.Error.Error() dereferences Response when the unexported code is empty.
	return &auth.Error{
		Response: &http.Response{StatusCode: http.StatusBadRequest},
		Body:     body,
	}
}

func TestClassifyReauthError(t *testing.T) {
	t.Parallel()

	grpcRAPT := status.Error(codes.Unauthenticated, `transport: per-RPC creds failed due to error: auth: "invalid_grant" "reauth related error (invalid_rapt)"`)

	tests := []struct {
		name string
		err  error
		want reauthClass
	}{
		{name: "nil", want: reauthClassNone},
		{name: "auth_invalid_rapt", err: newAuthErr(raptJSON), want: reauthClassTyped},
		{name: "auth_rapt_required", err: newAuthErr(raptRequiredJSON), want: reauthClassTyped},
		{name: "auth_revoked_invalid_grant", err: newAuthErr(revokedJSON), want: reauthClassNone},
		{name: "auth_non_json_body", err: newAuthErr([]byte("not json")), want: reauthClassNone},
		{name: "auth_wrapped_invalid_rapt", err: fmtWrap(newAuthErr(raptJSON)), want: reauthClassTyped},
		{
			name: "oauth2_invalid_rapt",
			err:  &oauth2.RetrieveError{ErrorCode: "invalid_grant", Body: raptJSON},
			want: reauthClassTyped,
		},
		{
			name: "oauth2_rapt_required",
			err:  &oauth2.RetrieveError{ErrorCode: "invalid_grant", Body: raptRequiredJSON},
			want: reauthClassTyped,
		},
		{
			name: "oauth2_revoked_invalid_grant",
			err: &oauth2.RetrieveError{
				ErrorCode:        "invalid_grant",
				ErrorDescription: "Token has been expired or revoked.",
				Body:             revokedJSON,
			},
			want: reauthClassNone,
		},
		{
			name: "oauth2_non_json_body",
			err:  &oauth2.RetrieveError{ErrorCode: "invalid_grant", Body: []byte("not json")},
			want: reauthClassNone,
		},
		{
			name: "oauth2_body_only_invalid_rapt",
			err:  &oauth2.RetrieveError{Body: raptJSON},
			want: reauthClassTyped,
		},
		{name: "grpc_unauthenticated_rapt_hint_only", err: grpcRAPT, want: reauthClassHint},
		{name: "unrelated", err: errors.New("connection refused"), want: reauthClassNone},
		{name: "permission_denied", err: status.Error(codes.PermissionDenied, "denied"), want: reauthClassNone},
		{name: "unauthenticated_without_rapt", err: status.Error(codes.Unauthenticated, "missing credentials"), want: reauthClassNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyReauthError(tt.err)
			if got != tt.want {
				t.Fatalf("classifyReauthError() = %v, want %v (err=%v)", got, tt.want, tt.err)
			}
			if tt.want == reauthClassTyped && !isReauthError(tt.err) {
				t.Fatal("isReauthError() = false, want true for typed reauth")
			}
			if tt.want != reauthClassTyped && isReauthError(tt.err) {
				t.Fatal("isReauthError() = true, want false")
			}
		})
	}
}

func fmtWrap(err error) error {
	return errors.Join(err)
}

func TestWrapWithHint(t *testing.T) {
	t.Setenv(envAppCredentials, "")
	t.Setenv(envCloudSDKConfig, "")

	typed := newAuthErr(raptJSON)
	hinted := wrapWithHint(typed)
	if hinted == nil || !strings.Contains(hinted.Error(), reauthHintText) {
		t.Fatalf("typed wrap = %v, want hint", hinted)
	}
	if !errors.Is(hinted, typed) {
		t.Fatal("wrapWithHint should wrap the original typed error")
	}

	grpcErr := status.Error(codes.Unauthenticated, `transport: per-RPC creds failed due to error: auth: "invalid_grant" "reauth related error (invalid_rapt)"`)
	got := wrapWithHint(grpcErr)
	if got == nil || !strings.Contains(got.Error(), reauthHintText) {
		t.Fatalf("unauthenticated wrap = %v, want hint", got)
	}

	unrelated := errors.New("boom")
	if wrapWithHint(unrelated) != unrelated {
		t.Fatalf("unrelated error was rewritten: %v", wrapWithHint(unrelated))
	}

	already := wrapWithHint(typed)
	if wrapWithHint(already) != already && strings.Count(wrapWithHint(already).Error(), reauthHintText) != 1 {
		t.Fatal("hint was applied more than once")
	}
}

func TestWrapWithHintMentionsEnv(t *testing.T) {
	t.Setenv(envCloudSDKConfig, "")
	t.Setenv(envAppCredentials, "/tmp/creds.json")
	got := wrapWithHint(newAuthErr(raptJSON)).Error()
	if !strings.Contains(got, "GOOGLE_APPLICATION_CREDENTIALS") {
		t.Fatalf("hint = %q, want GOOGLE_APPLICATION_CREDENTIALS", got)
	}

	t.Setenv(envAppCredentials, "")
	t.Setenv(envCloudSDKConfig, "/tmp/gcloud-config")
	got = wrapWithHint(newAuthErr(raptJSON)).Error()
	if !strings.Contains(got, "CLOUDSDK_CONFIG") {
		t.Fatalf("hint = %q, want CLOUDSDK_CONFIG", got)
	}
}

func TestProcessFlagsReauth(t *testing.T) {
	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })

	base := []string{"execspansql", "database", "--project", "p", "--instance", "i", "--sql", "SELECT 1"}
	tests := []struct {
		name    string
		env     string
		args    []string
		want    string
		wantErr string
	}{
		{name: "default_off", args: base, want: reauthModeOff},
		{name: "flag_auto", args: append(append([]string{}, base...), "--reauth", "auto"), want: reauthModeAuto},
		{name: "flag_off", args: append(append([]string{}, base...), "--reauth", "off"), want: reauthModeOff},
		{name: "env_auto", env: reauthModeAuto, args: base, want: reauthModeAuto},
		{name: "invalid", args: append(append([]string{}, base...), "--reauth", "prompt"), wantErr: "--reauth must be one of"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.env != "" {
				t.Setenv("EXECSPANSQL_REAUTH", tt.env)
			} else {
				t.Setenv("EXECSPANSQL_REAUTH", "")
				_ = os.Unsetenv("EXECSPANSQL_REAUTH")
			}
			os.Args = tt.args
			got, err := processFlags()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("processFlags() error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Reauth != tt.want {
				t.Fatalf("Reauth = %q, want %q", got.Reauth, tt.want)
			}
		})
	}
}

func TestGcloudADCLoginArgs(t *testing.T) {
	t.Parallel()

	plain := gcloudADCLoginArgs(func(k string) string {
		if k == envDisplay {
			return ":0"
		}
		return ""
	})
	if want := []string{"auth", "application-default", "login"}; !equalStrings(plain, want) {
		t.Fatalf("args = %v, want %v", plain, want)
	}
	ssh := gcloudADCLoginArgs(func(k string) string {
		if k == envSSHConnection {
			return "1 2 3 4"
		}
		return ""
	})
	if want := []string{"auth", "application-default", "login", "--no-launch-browser"}; !equalStrings(ssh, want) {
		t.Fatalf("ssh args = %v, want %v", ssh, want)
	}
}

func TestReauthApplicability(t *testing.T) {
	authorized := adcSnapshot{
		Type:     string(credentials.AuthorizedUser),
		Writable: true,
		Contents: []byte(`{"type":"authorized_user"}`),
	}
	base := func() *reauthHooks {
		return &reauthHooks{
			getenv:        func(string) string { return "" },
			lookPath:      func(string) (string, error) { return "/usr/bin/gcloud", nil },
			isTerminal:    func(int) bool { return true },
			stdinFD:       1,
			stderrFD:      2,
			wellKnownPath: func() string { return "/tmp/adc.json" },
			inspectADC:    func(string) (adcSnapshot, error) { return authorized, nil },
		}
	}

	tests := []struct {
		name     string
		o        opts
		injected []option.ClientOption
		hooks    func() *reauthHooks
		want     bool
	}{
		{name: "all_hold", o: opts{Reauth: reauthModeAuto}, hooks: base, want: true},
		{name: "reauth_off", o: opts{Reauth: reauthModeOff}, hooks: base},
		{
			name:     "injected_client_options",
			o:        opts{Reauth: reauthModeAuto},
			injected: []option.ClientOption{option.WithoutAuthentication()},
			hooks:    base,
		},
		{
			name: "emulator_host",
			o:    opts{Reauth: reauthModeAuto},
			hooks: func() *reauthHooks {
				h := base()
				h.getenv = getenvMap(map[string]string{envSpannerEmulatorHost: "localhost:9010"})
				return h
			},
		},
		{
			name: "application_credentials",
			o:    opts{Reauth: reauthModeAuto},
			hooks: func() *reauthHooks {
				h := base()
				h.getenv = getenvMap(map[string]string{envAppCredentials: "/tmp/sa.json"})
				return h
			},
		},
		{
			name: "cloudsdk_config",
			o:    opts{Reauth: reauthModeAuto},
			hooks: func() *reauthHooks {
				h := base()
				h.getenv = getenvMap(map[string]string{envCloudSDKConfig: "/tmp/gcloud"})
				return h
			},
		},
		{
			name: "missing_file",
			o:    opts{Reauth: reauthModeAuto},
			hooks: func() *reauthHooks {
				h := base()
				h.inspectADC = func(string) (adcSnapshot, error) { return adcSnapshot{}, os.ErrNotExist }
				return h
			},
		},
		{
			name: "unwritable_file",
			o:    opts{Reauth: reauthModeAuto},
			hooks: func() *reauthHooks {
				h := base()
				h.inspectADC = func(string) (adcSnapshot, error) {
					s := authorized
					s.Writable = false
					return s, nil
				}
				return h
			},
		},
		{
			name: "service_account_type",
			o:    opts{Reauth: reauthModeAuto},
			hooks: func() *reauthHooks {
				h := base()
				h.inspectADC = func(string) (adcSnapshot, error) {
					return adcSnapshot{Type: string(credentials.ServiceAccount), Writable: true, Contents: []byte(`{"type":"service_account"}`)}, nil
				}
				return h
			},
		},
		{
			name: "non_tty_stdin",
			o:    opts{Reauth: reauthModeAuto},
			hooks: func() *reauthHooks {
				h := base()
				h.isTerminal = func(fd int) bool { return fd != h.stdinFD }
				return h
			},
		},
		{
			name: "non_tty_stderr",
			o:    opts{Reauth: reauthModeAuto},
			hooks: func() *reauthHooks {
				h := base()
				h.isTerminal = func(fd int) bool { return fd != h.stderrFD }
				return h
			},
		},
		{
			name: "gcloud_missing",
			o:    opts{Reauth: reauthModeAuto},
			hooks: func() *reauthHooks {
				h := base()
				h.lookPath = func(string) (string, error) { return "", os.ErrNotExist }
				return h
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := reauthApplicable(tt.o, tt.injected, tt.hooks())
			if got != tt.want {
				t.Fatalf("reauthApplicable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReauthApplicabilityEnvAndHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv(envSpannerEmulatorHost, "")
	t.Setenv(envAppCredentials, "")
	t.Setenv(envCloudSDKConfig, "")

	adcDir := filepath.Join(home, ".config", "gcloud")
	if err := os.MkdirAll(adcDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(adcDir, adcFileName)
	if err := os.WriteFile(path, []byte(`{"type":"authorized_user"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	hooks := func() *reauthHooks {
		h := productionReauthHooks()
		h.getenv = os.Getenv
		h.isTerminal = func(int) bool { return true }
		h.lookPath = func(string) (string, error) { return "/usr/bin/gcloud", nil }
		return h
	}
	o := opts{Reauth: reauthModeAuto}
	if !reauthApplicable(o, nil, hooks()) {
		t.Fatal("want applicable with temp HOME ADC")
	}
	if wellKnownADCPath() != path {
		t.Fatalf("wellKnownADCPath() = %q, want %q", wellKnownADCPath(), path)
	}

	t.Run("emulator", func(t *testing.T) {
		t.Setenv(envSpannerEmulatorHost, "localhost:9010")
		if reauthApplicable(o, nil, hooks()) {
			t.Fatal("SPANNER_EMULATOR_HOST should disable auto reauth")
		}
	})
	t.Run("gac", func(t *testing.T) {
		t.Setenv(envAppCredentials, filepath.Join(home, "sa.json"))
		if reauthApplicable(o, nil, hooks()) {
			t.Fatal("GOOGLE_APPLICATION_CREDENTIALS should disable auto reauth")
		}
	})
	t.Run("cloudsdk_config", func(t *testing.T) {
		t.Setenv(envCloudSDKConfig, filepath.Join(home, "gcloud-config"))
		if reauthApplicable(o, nil, hooks()) {
			t.Fatal("CLOUDSDK_CONFIG should disable auto reauth")
		}
	})
	t.Run("missing_file", func(t *testing.T) {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if reauthApplicable(o, nil, hooks()) {
			t.Fatal("missing ADC file should disable auto reauth")
		}
	})
	t.Run("wrong_type", func(t *testing.T) {
		if err := os.WriteFile(path, []byte(`{"type":"service_account"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if reauthApplicable(o, nil, hooks()) {
			t.Fatal("non-authorized_user ADC should disable auto reauth")
		}
	})
	t.Run("unwritable", func(t *testing.T) {
		if err := os.WriteFile(path, []byte(`{"type":"authorized_user"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o400); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
		if reauthApplicable(o, nil, hooks()) {
			t.Fatal("unwritable ADC file should disable auto reauth")
		}
	})
}

func TestReauthPreflight(t *testing.T) {
	raptErr := newAuthErr(raptJSON)
	auto := opts{Reauth: reauthModeAuto}
	off := opts{Reauth: reauthModeOff}

	t.Run("off_no_detection", func(t *testing.T) {
		h, loginCalls, detectCalls := newPreflightHooks(t, raptErr)
		opts, err := runPreflight(context.Background(), off, nil, h)
		if err != nil {
			t.Fatal(err)
		}
		if opts != nil {
			t.Fatalf("client options = %v, want nil", opts)
		}
		if *detectCalls != 0 || *loginCalls != 0 {
			t.Fatalf("detect=%d login=%d, want 0, 0", *detectCalls, *loginCalls)
		}
	})

	t.Run("reauth_off_hint_only", func(t *testing.T) {
		h, loginCalls, _ := newPreflightHooks(t, raptErr)
		_, err := runPreflight(context.Background(), off, nil, h)
		if err != nil {
			t.Fatal(err)
		}
		if *loginCalls != 0 {
			t.Fatalf("login calls = %d, want 0", *loginCalls)
		}
		hinted := wrapWithHint(raptErr)
		if !strings.Contains(hinted.Error(), reauthHintText) {
			t.Fatalf("hint = %v", hinted)
		}
	})

	t.Run("auto_success_no_login", func(t *testing.T) {
		h, loginCalls, detectCalls := newPreflightHooks(t, nil)
		opts, err := runPreflight(context.Background(), auto, nil, h)
		if err != nil {
			t.Fatal(err)
		}
		if len(opts) != 1 {
			t.Fatalf("client options len = %d, want 1", len(opts))
		}
		if *loginCalls != 0 || *detectCalls != 1 {
			t.Fatalf("detect=%d login=%d, want 1, 0", *detectCalls, *loginCalls)
		}
	})

	t.Run("auto_reauth_one_login", func(t *testing.T) {
		h, loginCalls, detectCalls, stderr := newMutablePreflightHooks(t, raptErr)
		h.login = func(context.Context) error {
			*loginCalls++
			h.inspectADC = func(string) (adcSnapshot, error) {
				return adcSnapshot{
					Type:     string(credentials.AuthorizedUser),
					Writable: true,
					Contents: []byte(`{"type":"authorized_user","refresh_token":"new"}`),
				}, nil
			}
			return nil
		}
		fetch := 0
		h.fetchToken = func(context.Context, *auth.Credentials) error {
			fetch++
			if fetch == 1 {
				return raptErr
			}
			return nil
		}
		opts, err := runPreflight(context.Background(), auto, nil, h)
		if err != nil {
			t.Fatal(err)
		}
		if len(opts) != 1 {
			t.Fatalf("client options len = %d, want 1", len(opts))
		}
		if *loginCalls != 1 {
			t.Fatalf("login calls = %d, want 1", *loginCalls)
		}
		if *detectCalls != 2 {
			t.Fatalf("detect calls = %d, want 2", *detectCalls)
		}
		if fetch != 2 {
			t.Fatalf("fetch calls = %d, want 2", fetch)
		}
		if !strings.Contains(stderr.String(), gcloudADCLoginNotice) {
			t.Fatalf("stderr = %q, want login notice", stderr.String())
		}
	})

	t.Run("login_fails", func(t *testing.T) {
		h, loginCalls, detectCalls := newPreflightHooks(t, raptErr)
		h.login = func(context.Context) error {
			*loginCalls++
			return errors.New("gcloud failed")
		}
		h.fetchToken = func(context.Context, *auth.Credentials) error { return raptErr }
		_, err := runPreflight(context.Background(), auto, nil, h)
		if err == nil || !strings.Contains(err.Error(), reauthHintText) {
			t.Fatalf("error = %v, want original plus hint", err)
		}
		if !errors.Is(err, raptErr) {
			t.Fatalf("error = %v, want original typed error", err)
		}
		if *loginCalls != 1 || *detectCalls != 1 {
			t.Fatalf("detect=%d login=%d, want 1, 1 (no second login)", *detectCalls, *loginCalls)
		}
	})

	t.Run("reloaded_unchanged", func(t *testing.T) {
		h, loginCalls, detectCalls := newPreflightHooks(t, raptErr)
		h.login = func(context.Context) error { *loginCalls++; return nil }
		h.fetchToken = func(context.Context, *auth.Credentials) error { return raptErr }
		_, err := runPreflight(context.Background(), auto, nil, h)
		if err == nil || !errors.Is(err, raptErr) || !strings.Contains(err.Error(), reauthHintText) {
			t.Fatalf("error = %v, want original plus hint", err)
		}
		if *loginCalls != 1 || *detectCalls != 1 {
			t.Fatalf("detect=%d login=%d, want no loop", *detectCalls, *loginCalls)
		}
	})

	t.Run("reloaded_wrong_type", func(t *testing.T) {
		h, loginCalls, detectCalls := newPreflightHooks(t, raptErr)
		h.login = func(context.Context) error {
			*loginCalls++
			h.inspectADC = func(string) (adcSnapshot, error) {
				return adcSnapshot{Type: string(credentials.ServiceAccount), Writable: true, Contents: []byte(`{"type":"service_account"}`)}, nil
			}
			return nil
		}
		h.fetchToken = func(context.Context, *auth.Credentials) error { return raptErr }
		_, err := runPreflight(context.Background(), auto, nil, h)
		if err == nil || !errors.Is(err, raptErr) {
			t.Fatalf("error = %v, want original", err)
		}
		if *loginCalls != 1 || *detectCalls != 1 {
			t.Fatalf("detect=%d login=%d, want no loop", *detectCalls, *loginCalls)
		}
	})

	t.Run("reloaded_unreadable", func(t *testing.T) {
		h, loginCalls, detectCalls := newPreflightHooks(t, raptErr)
		h.login = func(context.Context) error {
			*loginCalls++
			h.inspectADC = func(string) (adcSnapshot, error) { return adcSnapshot{}, errors.New("unreadable") }
			return nil
		}
		h.fetchToken = func(context.Context, *auth.Credentials) error { return raptErr }
		_, err := runPreflight(context.Background(), auto, nil, h)
		if err == nil || !errors.Is(err, raptErr) {
			t.Fatalf("error = %v, want original", err)
		}
		if *loginCalls != 1 || *detectCalls != 1 {
			t.Fatalf("detect=%d login=%d, want no loop", *detectCalls, *loginCalls)
		}
	})

	t.Run("second_fetch_fails", func(t *testing.T) {
		h, loginCalls, detectCalls := newPreflightHooks(t, raptErr)
		h.login = func(context.Context) error {
			*loginCalls++
			h.inspectADC = func(string) (adcSnapshot, error) {
				return adcSnapshot{
					Type:     string(credentials.AuthorizedUser),
					Writable: true,
					Contents: []byte(`{"type":"authorized_user","refresh_token":"new"}`),
				}, nil
			}
			return nil
		}
		h.fetchToken = func(context.Context, *auth.Credentials) error { return raptErr }
		_, err := runPreflight(context.Background(), auto, nil, h)
		if err == nil || !errors.Is(err, raptErr) || !strings.Contains(err.Error(), reauthHintText) {
			t.Fatalf("error = %v, want original plus hint", err)
		}
		if *loginCalls != 1 || *detectCalls != 2 {
			t.Fatalf("detect=%d login=%d, want 2, 1", *detectCalls, *loginCalls)
		}
	})

	t.Run("budget_already_spent", func(t *testing.T) {
		h, loginCalls, _ := newPreflightHooks(t, raptErr)
		h.loginUsed = true
		h.fetchToken = func(context.Context, *auth.Credentials) error { return raptErr }
		_, err := runPreflight(context.Background(), auto, nil, h)
		if err == nil || !errors.Is(err, raptErr) || !strings.Contains(err.Error(), reauthHintText) {
			t.Fatalf("error = %v, want original plus hint", err)
		}
		if *loginCalls != 0 {
			t.Fatalf("login calls = %d, want 0", *loginCalls)
		}
	})

	t.Run("quota_project_changed", func(t *testing.T) {
		h, _, _, stderr := newMutablePreflightHooks(t, raptErr)
		firstJSON := []byte(`{"type":"authorized_user","quota_project_id":"old"}`)
		secondJSON := []byte(`{"type":"authorized_user","quota_project_id":"new","refresh_token":"new"}`)
		detect := 0
		h.detect = func(context.Context) (*auth.Credentials, error) {
			detect++
			if detect == 1 {
				return credsWithQuota("old", firstJSON), nil
			}
			return credsWithQuota("new", secondJSON), nil
		}
		h.login = func(context.Context) error {
			h.inspectADC = func(string) (adcSnapshot, error) {
				return adcSnapshot{Type: string(credentials.AuthorizedUser), Writable: true, Contents: secondJSON}, nil
			}
			return nil
		}
		fetch := 0
		h.fetchToken = func(context.Context, *auth.Credentials) error {
			fetch++
			if fetch == 1 {
				return raptErr
			}
			return nil
		}
		_, err := runPreflight(context.Background(), auto, nil, h)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(stderr.String(), "old") || !strings.Contains(stderr.String(), "new") {
			t.Fatalf("stderr = %q, want quota project difference", stderr.String())
		}
	})

	t.Run("injected_options_skip_detect", func(t *testing.T) {
		h, loginCalls, detectCalls := newPreflightHooks(t, raptErr)
		_, err := runPreflight(context.Background(), auto, []option.ClientOption{option.WithoutAuthentication()}, h)
		if err != nil {
			t.Fatal(err)
		}
		if *detectCalls != 0 || *loginCalls != 0 {
			t.Fatalf("detect=%d login=%d, want skipped", *detectCalls, *loginCalls)
		}
	})
}

func runPreflight(ctx context.Context, o opts, injected []option.ClientOption, h *reauthHooks) ([]option.ClientOption, error) {
	opts, err := maybeAuthPreflight(ctx, o, injected, h)
	return opts, wrapWithHint(err)
}

func newPreflightHooks(t *testing.T, tokenErr error) (*reauthHooks, *int, *int) {
	t.Helper()
	h, loginCalls, detectCalls, _ := newMutablePreflightHooks(t, tokenErr)
	return h, loginCalls, detectCalls
}

func newMutablePreflightHooks(t *testing.T, tokenErr error) (*reauthHooks, *int, *int, *bytes.Buffer) {
	t.Helper()
	var stderr bytes.Buffer
	var loginCalls, detectCalls int
	snap := adcSnapshot{
		Type:     string(credentials.AuthorizedUser),
		Writable: true,
		Contents: []byte(`{"type":"authorized_user","refresh_token":"old"}`),
	}
	h := &reauthHooks{
		getenv:        func(string) string { return "" },
		lookPath:      func(string) (string, error) { return "/usr/bin/gcloud", nil },
		isTerminal:    func(int) bool { return true },
		wellKnownPath: func() string { return "/tmp/adc.json" },
		inspectADC:    func(string) (adcSnapshot, error) { return snap, nil },
		stderr:        &stderr,
		detect: func(context.Context) (*auth.Credentials, error) {
			detectCalls++
			return credsWithQuota("proj", snap.Contents), nil
		},
		fetchToken: func(context.Context, *auth.Credentials) error { return tokenErr },
		login: func(context.Context) error {
			loginCalls++
			t.Fatal("unexpected login")
			return nil
		},
	}
	return h, &loginCalls, &detectCalls, &stderr
}

func credsWithQuota(quota string, raw []byte) *auth.Credentials {
	return auth.NewCredentials(&auth.CredentialsOptions{
		TokenProvider: tokenStub{tok: &auth.Token{Value: "ya29.fake"}},
		JSON:          raw,
		QuotaProjectIDProvider: auth.CredentialsPropertyFunc(func(context.Context) (string, error) {
			return quota, nil
		}),
	})
}

type tokenStub struct {
	tok *auth.Token
	err error
}

func (s tokenStub) Token(context.Context) (*auth.Token, error) {
	return s.tok, s.err
}

func getenvMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestReauthKongEnum(t *testing.T) {
	t.Parallel()
	var o opts
	parser, err := kong.New(&o, kong.Name("execspansql"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = parser.Parse([]string{"db", "--project", "p", "--instance", "i", "--sql", "SELECT 1", "--reauth", "auto"})
	if err != nil {
		t.Fatal(err)
	}
	if o.Reauth != reauthModeAuto {
		t.Fatalf("Reauth = %q, want %q", o.Reauth, reauthModeAuto)
	}
}

func TestInspectADCFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, adcFileName)
	if _, err := inspectADCFile(path); err == nil {
		t.Fatal("missing file should error")
	}
	if err := os.WriteFile(path, []byte(`{"type":"authorized_user"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	snap, err := inspectADCFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Type != string(credentials.AuthorizedUser) || !snap.Writable {
		t.Fatalf("snapshot = %+v", snap)
	}
	if err := os.Chmod(path, 0o400); err != nil {
		t.Fatal(err)
	}
	snap, err = inspectADCFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Writable {
		t.Fatal("want unwritable")
	}
}

func TestRunGcloudADCLogin(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake gcloud script is a POSIX shell script")
	}
	dir := t.TempDir()
	argvPath := filepath.Join(dir, "argv")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$@\" > " + shellQuote(argvPath) + "\n" +
		"echo gcloud-login-stdout\n"
	gcloudPath := filepath.Join(dir, "gcloud")
	if err := os.WriteFile(gcloudPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv(envSSHConnection, "203.0.113.1 60000 203.0.113.2 22")
	t.Setenv(envSSHTTY, "")

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldStdout, oldStderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = stdoutW, stderrW
	defer func() {
		os.Stdout, os.Stderr = oldStdout, oldStderr
	}()

	var stdoutBuf, stderrBuf bytes.Buffer
	stdoutDone := make(chan struct{})
	stderrDone := make(chan struct{})
	go func() { _, _ = io.Copy(&stdoutBuf, stdoutR); close(stdoutDone) }()
	go func() { _, _ = io.Copy(&stderrBuf, stderrR); close(stderrDone) }()

	runErr := runGcloudADCLogin(context.Background(), os.Getenv, exec.LookPath)
	if err := stdoutW.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stderrW.Close(); err != nil {
		t.Fatal(err)
	}
	<-stdoutDone
	<-stderrDone
	os.Stdout, os.Stderr = oldStdout, oldStderr

	if runErr != nil {
		t.Fatalf("runGcloudADCLogin() = %v", runErr)
	}
	gotArgv, err := os.ReadFile(argvPath)
	if err != nil {
		t.Fatal(err)
	}
	wantArgv := "auth\napplication-default\nlogin\n--no-launch-browser\n"
	if string(gotArgv) != wantArgv {
		t.Fatalf("argv = %q, want %q", gotArgv, wantArgv)
	}
	if strings.Contains(stdoutBuf.String(), "gcloud-login-stdout") {
		t.Fatalf("gcloud stdout leaked to process stdout: %q", stdoutBuf.String())
	}
	if !strings.Contains(stderrBuf.String(), "gcloud-login-stdout") {
		t.Fatalf("stderr = %q, want gcloud-login-stdout", stderrBuf.String())
	}
}

func shellQuote(path string) string {
	return "'" + strings.ReplaceAll(path, "'", `'"'"'`) + "'"
}
