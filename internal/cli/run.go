package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// App is one invocation after the global flags have been parsed.
type App struct {
	ctx     context.Context
	Stdin   io.Reader
	Stdout  io.Writer
	Stderr  io.Writer
	env     func(string) string
	home    string
	Context string
	opt     options
	JSON    bool
	Verbose bool
	Yes     bool
	tty     bool
	Args    []string
	sec     secrets
}

// Run executes one CLI invocation and returns the process exit code (E11-D007).
// env is consulted for MFTIKO_TOKEN and MFTIKO_CONTEXT. home is the directory
// that contains .config/mftiko/config.yaml.
func Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, env func(string) string, home string) int {
	if ctx == nil {
		ctx = context.Background()
	}
	if stdin == nil {
		stdin = strings.NewReader("")
	}
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if env == nil {
		env = func(string) string { return "" }
	}
	var sec secrets
	sec.add(env("MFTIKO_TOKEN"))
	sec.add(strings.TrimSpace(env("MFTIKO_TOKEN")))
	opt, pos, err := parseArgs(args)
	if err != nil {
		if errors.Is(err, errHelp) {
			fmt.Fprint(stdout, generalHelp)
			return ExitOK
		}
		return writeErr(stderr, sec, err.Error())
	}
	if opt.version {
		fmt.Fprint(stdout, versionText(cliBuild()))
		return ExitOK
	}
	if len(pos) == 0 {
		return writeErr(stderr, sec, usageLine)
	}
	if pos[0] == "help" {
		return help(stdout, stderr, sec, pos[1:])
	}
	cmd, rest, ok := lookup(pos)
	if !ok {
		return unknown(stderr, sec, pos)
	}
	app := &App{
		ctx:     ctx,
		Stdin:   stdin,
		Stdout:  stdout,
		Stderr:  stderr,
		env:     env,
		home:    home,
		opt:     opt,
		JSON:    opt.output == "json",
		Verbose: opt.verbose,
		Yes:     opt.yes,
		tty:     stdinIsTerminal(stdin),
		Args:    rest,
		sec:     sec,
	}
	return cmd.run(app)
}

type command struct {
	action   string
	resource string
	run      func(*App) int
}

// commands is the dispatch table (E11-D002). login and whoami are single words,
// as E11-D006 names them. The read-only queries are E11-D010–E11-D013.
// account is mftiko account <action> (E11-D014).
// session create is E11-D019–E11-D023.
// session and record are E11-D024–E11-D028.
// session watch and --wait are E11-D029–E11-D034.
// version is E11-D036.
var commands = []command{
	{action: "login", run: runLogin},
	{action: "whoami", run: runWhoami},
	{action: "version", run: runVersion},
	{resource: "strategy", action: "list", run: runStrategyList},
	{resource: "strategy", action: "versions", run: runStrategyVersions},
	{resource: "strategy", action: "show", run: runStrategyShow},
	{resource: "instance", action: "list", run: runInstanceList},
	{resource: "instance", action: "show", run: runInstanceShow},
	{resource: "instance", action: "retire", run: runInstanceRetire},
	{resource: "account-kind", action: "list", run: runAccountKindList},
	{resource: "account", action: "list", run: runAccountList},
	{resource: "account", action: "show", run: runAccountShow},
	{resource: "account", action: "create", run: runAccountCreate},
	{resource: "account", action: "update", run: runAccountUpdate},
	{resource: "account", action: "delete", run: runAccountDelete},
	{resource: "session", action: "create", run: runSessionCreate},
	{resource: "session", action: "list", run: runSessionList},
	{resource: "session", action: "show", run: runSessionShow},
	{resource: "session", action: "end", run: runSessionEnd},
	{resource: "session", action: "delete", run: runSessionDelete},
	{resource: "session", action: "watch", run: runSessionWatch},
	{resource: "record", action: "list", run: runRecordList},
	{resource: "record", action: "show", run: runRecordShow},
}

func lookup(pos []string) (command, []string, bool) {
	for _, c := range commands {
		if c.resource == "" && c.action == pos[0] {
			return c, pos[1:], true
		}
	}
	if len(pos) >= 2 {
		for _, c := range commands {
			if c.resource == pos[0] && c.action == pos[1] {
				return c, pos[2:], true
			}
		}
	}
	return command{}, nil, false
}

func (a *App) useContext() int {
	name, err := resolveContext(a.opt, a.env)
	if err != nil {
		return a.exit(err.Error())
	}
	a.Context = name
	return 0
}

// resolveContext implements E11-D004 without choosing a context the user did
// not name. Both selectors must agree when both are set. Neither set is an error.
func resolveContext(opt options, env func(string) string) (string, error) {
	envName := strings.TrimSpace(env("MFTIKO_CONTEXT"))
	envSet := envName != ""
	switch {
	case opt.contextSet && envSet && opt.context != envName:
		return "", errors.New("--context and MFTIKO_CONTEXT differ")
	case opt.contextSet:
		return opt.context, nil
	case envSet:
		return envName, nil
	default:
		return "", errors.New("set --context or MFTIKO_CONTEXT")
	}
}

func unknown(stderr io.Writer, sec secrets, pos []string) int {
	if text, ok := commandHelp(pos[0]); ok && pos[0] != "login" && pos[0] != "whoami" && pos[0] != "version" && pos[0] != "help" {
		if len(pos) == 1 {
			return writeErr(stderr, sec, text)
		}
		return writeErr(stderr, sec, fmt.Sprintf("unknown command %q\n%s", pos[0]+" "+pos[1], text))
	}
	return writeErr(stderr, sec, fmt.Sprintf("unknown command %q\n%s", pos[0], usageLine))
}

func help(stdout, stderr io.Writer, sec secrets, args []string) int {
	switch len(args) {
	case 0:
		fmt.Fprint(stdout, generalHelp)
		return ExitOK
	case 1:
		text, ok := commandHelp(args[0])
		if !ok {
			return writeErr(stderr, sec, fmt.Sprintf("unknown command %q", args[0]))
		}
		fmt.Fprint(stdout, text)
		return ExitOK
	default:
		return writeErr(stderr, sec, "help takes at most one command")
	}
}

func commandHelp(name string) (string, bool) {
	switch name {
	case "login":
		return loginHelp, true
	case "whoami":
		return whoamiHelp, true
	case "version":
		return versionHelp, true
	case "strategy":
		return strategyHelp, true
	case "instance":
		return instanceHelp, true
	case "account-kind":
		return accountKindHelp, true
	case "account":
		return accountHelp, true
	case "session":
		return sessionHelp, true
	case "record":
		return recordHelp, true
	case "help":
		return generalHelp, true
	default:
		return "", false
	}
}

func runLogin(a *App) int {
	if len(a.Args) != 0 {
		return a.exit("login takes no arguments")
	}
	if code := a.rejectPageFlags(); code != 0 {
		return code
	}
	if code := a.onlyFlags(); code != 0 {
		return code
	}
	if code := a.useContext(); code != 0 {
		return code
	}
	path, f, c, code := a.load()
	if code != 0 {
		return code
	}
	if code := a.rejectEndpoint(c); code != 0 {
		return code
	}
	if a.tty {
		fmt.Fprint(a.Stderr, "Token: ")
	}
	token, err := readToken(a.Stdin)
	if err != nil {
		a.sec.add(token)
		return a.exit(err.Error())
	}
	a.sec.add(token)
	api, code := a.dial(c, token)
	if code != 0 {
		return code
	}
	res, err := api.GetMe(a.ctx)
	if code := a.checked(res, err); code != 0 {
		return code
	}
	c.Token = token
	f.Contexts[a.Context] = c
	if err := Save(path, f); err != nil {
		return a.exit(err.Error())
	}
	return a.writeMe(res)
}

func runWhoami(a *App) int {
	if len(a.Args) != 0 {
		return a.exit("whoami takes no arguments")
	}
	if code := a.rejectPageFlags(); code != 0 {
		return code
	}
	if code := a.onlyFlags(); code != 0 {
		return code
	}
	api, code := a.api()
	if code != 0 {
		return code
	}
	res, err := api.GetMe(a.ctx)
	if code := a.checked(res, err); code != 0 {
		return code
	}
	return a.writeMe(res)
}

// api loads the selected context and dials with MFTIKO_TOKEN or the stored token.
func (a *App) api() (*API, int) {
	if code := a.useContext(); code != 0 {
		return nil, code
	}
	_, _, c, code := a.load()
	if code != 0 {
		return nil, code
	}
	token := strings.TrimSpace(a.env("MFTIKO_TOKEN"))
	if token == "" {
		token = c.Token
	}
	a.sec.add(c.Token)
	a.sec.add(token)
	if token == "" {
		return nil, a.exit("no token; set MFTIKO_TOKEN or run mftiko login")
	}
	return a.dial(c, token)
}

func (a *App) load() (string, *File, Context, int) {
	if a.home == "" {
		return "", nil, Context{}, a.exit("no home directory")
	}
	path := ConfigPath(a.home)
	f, err := Load(path)
	if err != nil {
		return "", nil, Context{}, a.exit(err.Error())
	}
	for _, c := range f.Contexts {
		a.sec.add(c.Token)
	}
	c, err := f.Context(a.Context)
	if err != nil {
		return "", nil, Context{}, a.exit(err.Error())
	}
	return path, f, c, 0
}

// rejectEndpoint checks the context URL, TLS file, and timeout before login
// reads stdin, so a usage error does not consume the token.
func (a *App) rejectEndpoint(c Context) int {
	_, _, err := prepare(Endpoint{
		URL:          c.URL,
		CAFile:       c.CAFile,
		InsecureHTTP: c.InsecureHTTP,
		Timeout:      a.opt.timeout,
	})
	if err == nil {
		return 0
	}
	return a.failDial(err)
}

func (a *App) dial(c Context, token string) (*API, int) {
	api, err := Dial(Endpoint{
		URL:          c.URL,
		Token:        token,
		CAFile:       c.CAFile,
		InsecureHTTP: c.InsecureHTTP,
		Timeout:      a.opt.timeout,
	})
	if err != nil {
		return nil, a.failDial(err)
	}
	return api, 0
}

func (a *App) failDial(err error) int {
	code := ExitUsage
	if !isUsage(err) {
		code = ExitNetwork
	}
	a.exitCode(code, err.Error())
	return code
}

// checked reports a transport failure as exit 2 and a problem body as its
// E11-D007 code. A 2xx response returns 0 and is not written.
func (a *App) checked(res Result, err error) int {
	if err != nil {
		return a.exitCode(ExitNetwork, "network (2): "+err.Error())
	}
	if res.Status < 200 || res.Status >= 300 {
		return reportProblem(a.Stderr, res.Body, res.RequestID, a.sec)
	}
	return ExitOK
}

func (a *App) writeMe(res Result) int {
	if a.JSON {
		if _, err := a.Stdout.Write(a.sec.redactBytes(res.Body)); err != nil {
			return a.exit(err.Error())
		}
	} else {
		var me apiv1.Me
		if err := json.Unmarshal(res.Body, &me); err != nil {
			return a.exit("response is not a user")
		}
		var buf strings.Builder
		header, rows := meTable(me)
		if err := WriteTable(&buf, header, rows); err != nil {
			return a.exit(err.Error())
		}
		if _, err := io.WriteString(a.Stdout, a.sec.redact(buf.String())); err != nil {
			return a.exit(err.Error())
		}
	}
	if a.Verbose && res.RequestID != "" {
		fmt.Fprintf(a.Stderr, "X-Request-Id: %s\n", a.sec.redact(res.RequestID))
	}
	return ExitOK
}

// confirm is the App wrapper around Confirm for later commands in this package.
func (a *App) confirm(prompt string) error {
	return Confirm(a.Stdin, a.Stderr, a.tty, a.Yes, prompt)
}

func (a *App) exit(msg string) int {
	return a.exitCode(ExitUsage, msg)
}

func (a *App) exitCode(code int, msg string) int {
	writeErr(a.Stderr, a.sec, msg)
	return code
}

func writeErr(w io.Writer, sec secrets, msg string) int {
	msg = strings.TrimRight(msg, "\n")
	fmt.Fprintf(w, "mftiko: %s\n", sec.redact(msg))
	return ExitUsage
}

func readToken(r io.Reader) (string, error) {
	b, err := io.ReadAll(io.LimitReader(r, 64<<10+1))
	if err != nil {
		return "", err
	}
	if len(b) > 64<<10 {
		return "", errors.New("token is too large")
	}
	s := strings.TrimSpace(string(b))
	if tokenInvalid(s) {
		return "", errors.New("login reads a token on stdin")
	}
	return s, nil
}
