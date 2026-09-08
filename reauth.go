package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	apiv1 "cloud.google.com/go/spanner/apiv1"
	"golang.org/x/oauth2"
	"golang.org/x/term"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	reauthModeOff  = "off"
	reauthModeAuto = "auto"

	adcFileName = "application_default_credentials.json"

	reauthHintText = "Reauthentication is needed. Please run 'gcloud auth application-default login' to reauthenticate."

	gcloudADCLoginNotice = "Reauthentication is needed; updating Application Default Credentials via gcloud auth application-default login."
)

const (
	envSpannerEmulatorHost = "SPANNER_EMULATOR_HOST"
	envAppCredentials      = "GOOGLE_APPLICATION_CREDENTIALS"
	envCloudSDKConfig      = "CLOUDSDK_CONFIG"
	envSSHConnection       = "SSH_CONNECTION"
	envSSHTTY              = "SSH_TTY"
	envDisplay             = "DISPLAY"
)

// reauthClass distinguishes a typed token error (login-eligible) from a
// message-only match that may only receive a hint.
type reauthClass int

const (
	reauthClassNone reauthClass = iota
	reauthClassTyped
	reauthClassHint
)

var errLoginBudgetSpent = errors.New("reauth login budget already spent")

type adcSnapshot struct {
	Type     string
	Writable bool
	Contents []byte
}

// reauthHooks are the injectable seams for the ADC preflight. Production
// wiring uses productionReauthHooks. Tests replace individual fields.
type reauthHooks struct {
	detect        func(ctx context.Context) (*auth.Credentials, error)
	fetchToken    func(ctx context.Context, creds *auth.Credentials) error
	login         func(ctx context.Context) error
	getenv        func(key string) string
	lookPath      func(file string) (string, error)
	isTerminal    func(fd int) bool
	stdinFD       int
	stderrFD      int
	wellKnownPath func() string
	inspectADC    func(path string) (adcSnapshot, error)
	stderr        io.Writer

	mu        sync.Mutex
	loginUsed bool
}

func productionReauthHooks() *reauthHooks {
	return &reauthHooks{
		detect: detectDefaultCredentials,
		fetchToken: func(ctx context.Context, creds *auth.Credentials) error {
			if creds == nil {
				return errors.New("no credentials")
			}
			_, err := creds.Token(ctx)
			return err
		},
		login: func(ctx context.Context) error {
			return runGcloudADCLogin(ctx, os.Getenv, exec.LookPath)
		},
		getenv:        os.Getenv,
		lookPath:      exec.LookPath,
		isTerminal:    term.IsTerminal,
		stdinFD:       int(os.Stdin.Fd()),
		stderrFD:      int(os.Stderr.Fd()),
		wellKnownPath: wellKnownADCPath,
		inspectADC:    inspectADCFile,
		stderr:        os.Stderr,
	}
}

var newReauthHooks = productionReauthHooks

func detectDefaultCredentials(context.Context) (*auth.Credentials, error) {
	return credentials.DetectDefault(&credentials.DetectOptions{
		Scopes: apiv1.DefaultAuthScopes(),
	})
}

// wellKnownADCPath is the path the Go auth library reads. It ignores
// CLOUDSDK_CONFIG; gcloud honors that variable, which is why a set
// CLOUDSDK_CONFIG makes automatic login inapplicable.
func wellKnownADCPath() string {
	if runtime.GOOS == "windows" {
		return filepath.Join(os.Getenv("APPDATA"), "gcloud", adcFileName)
	}
	return filepath.Join(os.Getenv("HOME"), ".config", "gcloud", adcFileName)
}

func inspectADCFile(path string) (adcSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return adcSnapshot{}, err
	}
	snap := adcSnapshot{
		Type:     credentialJSONType(data),
		Contents: data,
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return snap, nil
	}
	snap.Writable = true
	_ = f.Close()
	return snap, nil
}

func credentialJSONType(raw []byte) string {
	var parsed struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(raw, &parsed) != nil {
		return ""
	}
	return parsed.Type
}

func isAuthorizedUserType(typ string) bool {
	return typ == string(credentials.AuthorizedUser)
}

func classifyReauthError(err error) reauthClass {
	if err == nil {
		return reauthClassNone
	}
	if isTypedReauthError(err) {
		return reauthClassTyped
	}
	if hasReauthSubstring(err.Error()) {
		return reauthClassHint
	}
	return reauthClassNone
}

// isReauthError reports whether err is a typed reauthentication failure
// (invalid_grant with error_subtype invalid_rapt or rapt_required).
// Message-only matches are not login-eligible.
func isReauthError(err error) bool {
	return classifyReauthError(err) == reauthClassTyped
}

func isTypedReauthError(err error) bool {
	var ae *auth.Error
	if errors.As(err, &ae) {
		return bodyIsRAPTGrant(ae.Body)
	}
	var re *oauth2.RetrieveError
	if errors.As(err, &re) {
		return retrieveErrorIsRAPTGrant(re)
	}
	return false
}

func retrieveErrorIsRAPTGrant(re *oauth2.RetrieveError) bool {
	code, subtype, parsed := parseOAuthErrorBody(re.Body)
	if re.ErrorCode != "" {
		code = re.ErrorCode
	}
	if !parsed && re.ErrorCode == "" {
		return false
	}
	return isRAPTGrant(code, subtype)
}

func bodyIsRAPTGrant(body []byte) bool {
	code, subtype, ok := parseOAuthErrorBody(body)
	if !ok {
		return false
	}
	return isRAPTGrant(code, subtype)
}

func parseOAuthErrorBody(body []byte) (code, subtype string, ok bool) {
	var parsed struct {
		Error        string `json:"error"`
		ErrorSubtype string `json:"error_subtype"`
	}
	if json.Unmarshal(body, &parsed) != nil {
		return "", "", false
	}
	return parsed.Error, parsed.ErrorSubtype, true
}

func isRAPTGrant(code, subtype string) bool {
	if code != "invalid_grant" {
		return false
	}
	return subtype == "invalid_rapt" || subtype == "rapt_required"
}

func hasReauthSubstring(s string) bool {
	return strings.Contains(s, "invalid_rapt") || strings.Contains(s, "rapt_required")
}

func needsReauthHint(err error) bool {
	if err == nil {
		return false
	}
	if isReauthError(err) {
		return true
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unauthenticated && hasReauthSubstring(st.Message()) {
		return true
	}
	return hasReauthSubstring(err.Error())
}

func wrapWithHint(err error) error {
	if err == nil {
		return nil
	}
	if !needsReauthHint(err) {
		return err
	}
	if strings.Contains(err.Error(), reauthHintText) {
		return err
	}
	var b strings.Builder
	b.WriteString(reauthHintText)
	if os.Getenv(envAppCredentials) != "" {
		b.WriteString("\nGOOGLE_APPLICATION_CREDENTIALS is set; gcloud writes the well-known ADC file, not that path.")
	}
	if os.Getenv(envCloudSDKConfig) != "" {
		b.WriteString("\nCLOUDSDK_CONFIG is set; the Go auth library does not read that config directory.")
	}
	return fmt.Errorf("%w\n%s", err, b.String())
}

func reauthApplicable(o opts, injectedClientOptions []option.ClientOption, h *reauthHooks) bool {
	if o.Reauth != reauthModeAuto {
		return false
	}
	if len(injectedClientOptions) > 0 {
		return false
	}
	if h.getenv(envSpannerEmulatorHost) != "" {
		return false
	}
	if h.getenv(envAppCredentials) != "" {
		return false
	}
	if h.getenv(envCloudSDKConfig) != "" {
		return false
	}
	if !h.isTerminal(h.stdinFD) || !h.isTerminal(h.stderrFD) {
		return false
	}
	if _, err := h.lookPath("gcloud"); err != nil {
		return false
	}
	snap, err := h.inspectADC(h.wellKnownPath())
	if err != nil {
		return false
	}
	return snap.Writable && isAuthorizedUserType(snap.Type)
}

func useNoLaunchBrowser(getenv func(string) string) bool {
	if getenv(envSSHConnection) != "" || getenv(envSSHTTY) != "" {
		return true
	}
	return runtime.GOOS == "linux" && getenv(envDisplay) == ""
}

func gcloudADCLoginArgs(getenv func(string) string) []string {
	args := []string{"auth", "application-default", "login"}
	if useNoLaunchBrowser(getenv) {
		args = append(args, "--no-launch-browser")
	}
	return args
}

// runGcloudADCLogin runs `gcloud auth application-default login` with a
// fixed argument vector. Stdout is attached to stderr so query output is
// not mixed with gcloud's instructions. Tokens and ADC JSON are not logged.
//
// On Windows the SDK installs gcloud as gcloud.cmd. exec.LookPath resolves
// it through PATHEXT and CreateProcess launches batch files through cmd.exe
// implicitly, so no explicit interpreter is needed (this is the same
// mechanism os/exec documents under its cmd.exe quoting caveat). The cmd.exe
// unquoting differences do not matter here because every argument is a fixed
// literal without spaces or metacharacters; only the resolved path may
// contain spaces, and Go quotes argv[0] like any other argument.
func runGcloudADCLogin(ctx context.Context, getenv func(string) string, lookPath func(string) (string, error)) error {
	bin, err := lookPath("gcloud")
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, bin, gcloudADCLoginArgs(getenv)...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (h *reauthHooks) tryLogin(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.loginUsed {
		return errLoginBudgetSpent
	}
	h.loginUsed = true
	_, _ = fmt.Fprintln(h.stderr, gcloudADCLoginNotice)
	return h.login(ctx)
}

func quotaProjectID(ctx context.Context, creds *auth.Credentials) string {
	if creds == nil {
		return ""
	}
	id, err := creds.QuotaProjectID(ctx)
	if err != nil {
		return ""
	}
	return id
}

func maybeAuthPreflight(ctx context.Context, o opts, injectedClientOptions []option.ClientOption, h *reauthHooks) ([]option.ClientOption, error) {
	if o.Reauth != reauthModeAuto || len(injectedClientOptions) > 0 || !reauthApplicable(o, injectedClientOptions, h) {
		return nil, nil
	}
	return runAuthPreflight(ctx, h)
}

func runAuthPreflight(ctx context.Context, h *reauthHooks) ([]option.ClientOption, error) {
	creds, err := h.detect(ctx)
	if err == nil {
		err = h.fetchToken(ctx, creds)
	}
	if !isReauthError(err) {
		if err != nil {
			return nil, err
		}
		return []option.ClientOption{option.WithAuthCredentials(creds)}, nil
	}
	orig := err
	before, _ := h.inspectADC(h.wellKnownPath())
	quotaBefore := quotaProjectID(ctx, creds)
	if err := h.tryLogin(ctx); err != nil {
		if !errors.Is(err, errLoginBudgetSpent) {
			_, _ = fmt.Fprintln(h.stderr, "gcloud auth application-default login failed")
		}
		return nil, orig
	}
	after, inspErr := h.inspectADC(h.wellKnownPath())
	if inspErr != nil || !isAuthorizedUserType(after.Type) || bytes.Equal(after.Contents, before.Contents) {
		return nil, orig
	}
	creds, err = h.detect(ctx)
	if err != nil {
		return nil, orig
	}
	if !isAuthorizedUserType(credentialJSONType(creds.JSON())) {
		return nil, orig
	}
	if err := h.fetchToken(ctx, creds); err != nil {
		return nil, orig
	}
	quotaAfter := quotaProjectID(ctx, creds)
	if quotaBefore != quotaAfter {
		_, _ = fmt.Fprintf(h.stderr, "warning: quota project ID changed from %q to %q after reauthentication\n", quotaBefore, quotaAfter)
	}
	return []option.ClientOption{option.WithAuthCredentials(creds)}, nil
}
