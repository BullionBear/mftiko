package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
)

var errHelp = errors.New("help")

// flagValue matches the flag package's `invalid value %q for flag` wrapper so a
// token passed as a flag value is not echoed (E11-D003).
var flagValue = regexp.MustCompile(`invalid value "(?:[^"\\]|\\.)*" for flag`)

const usageLine = "usage: mftiko [flags] <command> [arguments]\n       mftiko [flags] <resource> <action> [arguments]"

const generalHelp = usageLine + `

commands:
  login         read a token from stdin, verify it with GET /auth/me, and store it
  whoami        call GET /auth/me and print the user
  strategy      list strategies, their versions, and one manifest
  instance      list instances and show one
  account-kind  list account kinds
  account       list, show, create, update, and delete trading accounts
  session       list, show, end, and delete sessions
  record        list session records and show one
  help          show this help

flags:
  -context name   context in the config file, or set MFTIKO_CONTEXT (they must match if both are set)
  -timeout 30s    HTTP client timeout, a Go duration greater than 0
  -o json         write the API body unchanged; the default is a table
  -limit N        cap the total number of rows for strategy list, strategy versions, account list, session list, and record list
  -cursor value   fetch one page for strategy list, strategy versions, account list, session list, and record list
  -v              print X-Request-Id on success
  -yes            allow a destructive command when stdin is not a terminal
  -version        print the release and exit

The config file is ~/.config/mftiko/config.yaml and its mode must not be wider
than 0600. It holds named contexts:

  contexts:
    dev:
      url: http://127.0.0.1:8080/api/v1
      insecure_http: true

url is the API client's server and includes /api/v1. The token comes from
MFTIKO_TOKEN or the context, and the environment variable wins. login is the
exception: its token is only the line on stdin. There is no token flag.
Plain HTTP is refused unless the context sets insecure_http: true.

strategy list and strategy versions follow next_cursor until the list ends.
Each request uses limit=1000. -limit N caps the total number of rows.
-cursor fetches only that page, and -o json writes that page unchanged.
-limit and -cursor cannot both be set. account list, session list, and record list use the same two flags.
`

const strategyHelp = `usage: mftiko strategy list
       mftiko strategy versions <id>
       mftiko strategy show <id> <ref>

list calls GET /strategies. versions calls GET /strategies/{id}/versions.
show calls GET /strategies/{id}/versions/{ref}. ref is a tag or a digest.

list shows the latest tag and the first 7 characters of its digest,
with a leading sha256: removed.
versions marks each row whose digest equals the latest tag in the rows
that were fetched. show prints the full digest.

list and versions read every page. Each request uses limit=1000.
-limit N caps the total number of rows. -cursor fetches one page, and
-o json writes that page unchanged. The two flags cannot both be set.
`

const instanceHelp = `usage: mftiko instance list
       mftiko instance show <name>

list calls GET /instances. show calls GET /instances/{name}.
The table includes the plane, whether the agent is present, and last_known.
A missing agent, or last_known true, is not present.
If the response has unavailable_regions or controller_absent, one warning
is printed to stderr and the exit code stays 0. A 503 for one instance is
region_unavailable.
`

const accountKindHelp = `usage: mftiko account-kind list

Call GET /account-kinds. The table shows trades, key_types, passphrase, and supported.
`

const loginHelp = `usage: mftiko login

Read one token from stdin, call GET /auth/me with Authorization: Bearer, and
write the token into the selected context only when that call succeeds.
The token is not a flag. The context's url must already be in the config file.
`

const whoamiHelp = `usage: mftiko whoami

Call GET /auth/me and print the user. The default is a table with local times.
-o json writes the API body unchanged.
`

const accountHelp = `usage: mftiko account list|show|create|update|delete

list
  mftiko account list [-td-instance name] [-limit N]
  mftiko account list [-td-instance name] [-cursor TOKEN]

-limit and -cursor cannot both be set.

show
  mftiko account show <api_id>

create
  mftiko account create -name NAME -kind KIND -td-instance NAME
      [-key-type hmac|ed25519]
      [-key-file PATH] [-secret-file PATH] [-passphrase-file PATH]

update
  mftiko account update <api_id>
      [-key-type hmac|ed25519]
      [-key-file PATH] [-secret-file PATH] [-passphrase-file PATH]

delete
  mftiko account delete <api_id>

Credential values are read from files or, on a terminal, without echo. They are
never flag values. "-" reads that one file from stdin, and at most one file may
be "-". An ed25519 secret is sent unchanged, including newlines; on a terminal
end it with EOF. HMAC key, secret, and passphrase values lose one trailing
newline. A kind with one key type uses it. A kind with several requires
-key-type. Giving any credential file on update replaces the whole set. With
none, update keeps the stored ciphertext. delete asks for confirmation.
`

const sessionHelp = `usage: mftiko session list|show|end|delete

list
  mftiko session list [-phase PHASE] [-active true|false] [-sts-instance NAME]
  mftiko session list [-limit N]
  mftiko session list [-cursor TOKEN]

-phase may be repeated. -active is true or false. -limit and -cursor cannot
both be set. Each request uses limit=1000 and follows next_cursor. -limit N
caps the total number of rows. -cursor fetches one page.

show
  mftiko session show <id>

end
  mftiko session end <id>

delete
  mftiko session delete <id>

show prints the spec summary, status, each demand ack, exit information, and
the log location. The strategy field shows id, name, and the first 7 characters
of the digest with a leading sha256: removed. -o json keeps the full digest.
A log path is <sts_instance> <host>:<path>. When the STS agent
record is the last known value, the log section says 最後已知，agent 目前不在線.
The command does not read the log file. end asks for confirmation; a session
that has already ended still succeeds. delete asks for confirmation. A session
that is not terminal exits 14 and says to run session end first.
`

const recordHelp = `usage: mftiko record list|show

list
  mftiko record list [-session-id ID] [-strategy-id ID] [-created-by NAME]
      [-created-after TIME] [-created-before TIME]
  mftiko record list [-limit N]
  mftiko record list [-cursor TOKEN]

TIME is RFC 3339, or YYYY-MM-DD at local midnight. Relative times are
rejected. -limit and -cursor cannot both be set. Each request uses
limit=1000 and follows next_cursor. -limit N caps the total number of rows.
-cursor fetches one page.

show
  mftiko record show <record_id>

A record stays readable after the session spec is deleted.
`

type options struct {
	context       string
	contextSet    bool
	timeout       time.Duration
	output        string
	limit         int
	limitSet      bool
	cursor        string
	cursorSet     bool
	verbose       bool
	yes           bool
	version       bool
	name          stringOpt
	kind          stringOpt
	tdInstance    stringOpt
	keyFile       stringOpt
	secretFile    stringOpt
	passFile      stringOpt
	keyType       stringOpt
	phase         stringList
	active        boolOpt
	stsInstance   stringOpt
	sessionID     stringOpt
	strategyID    stringOpt
	createdBy     stringOpt
	createdAfter  stringOpt
	createdBefore stringOpt
}

type stringOpt struct {
	v   string
	set bool
}

func (s *stringOpt) String() string { return s.v }

func (s *stringOpt) Set(v string) error {
	s.v = v
	s.set = true
	return nil
}

type durationOpt struct {
	v   time.Duration
	set bool
}

func (d *durationOpt) String() string {
	if !d.set {
		return "30s"
	}
	return d.v.String()
}

type intOpt struct {
	v   int
	set bool
}

func (n *intOpt) String() string {
	if !n.set {
		return ""
	}
	return strconv.Itoa(n.v)
}

func (n *intOpt) Set(s string) error {
	v, err := strconv.Atoi(s)
	if err != nil || v < 1 {
		return errors.New("must be a positive integer")
	}
	n.v = v
	n.set = true
	return nil
}

type boolOpt struct {
	v   bool
	set bool
}

func (b *boolOpt) String() string {
	if !b.set {
		return ""
	}
	return strconv.FormatBool(b.v)
}

func (b *boolOpt) Set(s string) error {
	switch s {
	case "true":
		b.v = true
	case "false":
		b.v = false
	default:
		return errors.New("must be true or false")
	}
	b.set = true
	return nil
}

// stringList collects repeated values of one flag, such as -phase.
type stringList struct {
	v   []string
	set bool
}

func (s *stringList) String() string { return strings.Join(s.v, ",") }

func (s *stringList) Set(v string) error {
	s.v = append(s.v, v)
	s.set = true
	return nil
}

func (d *durationOpt) Set(s string) error {
	v, err := time.ParseDuration(s)
	if err != nil {
		return errors.New("invalid duration")
	}
	if v <= 0 {
		return errors.New("must be positive")
	}
	d.v = v
	d.set = true
	return nil
}

func parseArgs(args []string) (options, []string, error) {
	flags, pos, err := splitArgs(args)
	if err != nil {
		return options{}, nil, err
	}
	var context stringOpt
	var timeout durationOpt
	var output stringOpt
	var outputLong stringOpt
	var name, kind, tdInstance stringOpt
	var keyFile, secretFile, passFile, keyType, cursor stringOpt
	var stsInstance, sessionID, strategyID, createdBy, createdAfter, createdBefore stringOpt
	var phase stringList
	var active boolOpt
	var limit intOpt
	fs := flag.NewFlagSet("mftiko", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	fs.Var(&context, "context", "config context")
	fs.Var(&timeout, "timeout", "HTTP timeout")
	fs.Var(&output, "o", "json or the default table")
	fs.Var(&outputLong, "output", "json or the default table")
	fs.Var(&limit, "limit", "cap the total number of rows")
	fs.Var(&cursor, "cursor", "fetch one page")
	verbose := fs.Bool("v", false, "print X-Request-Id on success")
	verboseLong := fs.Bool("verbose", false, "print X-Request-Id on success")
	yes := fs.Bool("yes", false, "confirm a destructive command off a terminal")
	version := fs.Bool("version", false, "print the release and exit")
	fs.Var(&name, "name", "account name")
	fs.Var(&kind, "kind", "account kind")
	fs.Var(&tdInstance, "td-instance", "TD instance")
	fs.Var(&keyFile, "key-file", "file to read the credential key from")
	fs.Var(&secretFile, "secret-file", "file to read the credential secret from")
	fs.Var(&passFile, "passphrase-file", "file to read the credential passphrase from")
	fs.Var(&keyType, "key-type", "hmac or ed25519")
	fs.Var(&phase, "phase", "session phase; may be repeated")
	fs.Var(&active, "active", "true or false")
	fs.Var(&stsInstance, "sts-instance", "STS instance")
	fs.Var(&sessionID, "session-id", "session id")
	fs.Var(&strategyID, "strategy-id", "strategy id")
	fs.Var(&createdBy, "created-by", "user who created the record")
	fs.Var(&createdAfter, "created-after", "RFC 3339 or YYYY-MM-DD")
	fs.Var(&createdBefore, "created-before", "RFC 3339 or YYYY-MM-DD")
	if err := fs.Parse(append(flags, pos...)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return options{}, nil, errHelp
		}
		return options{}, nil, errors.New(flagValue.ReplaceAllString(err.Error(), "invalid value for flag"))
	}
	opt := options{
		context:       context.v,
		contextSet:    context.set,
		timeout:       30 * time.Second,
		limit:         limit.v,
		limitSet:      limit.set,
		cursor:        cursor.v,
		cursorSet:     cursor.set,
		verbose:       *verbose || *verboseLong,
		yes:           *yes,
		version:       *version,
		name:          name,
		kind:          kind,
		tdInstance:    tdInstance,
		keyFile:       keyFile,
		secretFile:    secretFile,
		passFile:      passFile,
		keyType:       keyType,
		phase:         phase,
		active:        active,
		stsInstance:   stsInstance,
		sessionID:     sessionID,
		strategyID:    strategyID,
		createdBy:     createdBy,
		createdAfter:  createdAfter,
		createdBefore: createdBefore,
	}
	if timeout.set {
		opt.timeout = timeout.v
	}
	switch {
	case output.set && outputLong.set && output.v != outputLong.v:
		return options{}, nil, errors.New("-o and -output differ")
	case output.set:
		opt.output = output.v
	case outputLong.set:
		opt.output = outputLong.v
	}
	if opt.output != "" && opt.output != "json" {
		return options{}, nil, errors.New("-o must be json")
	}
	if opt.contextSet && strings.TrimSpace(opt.context) == "" {
		return options{}, nil, errors.New("-context needs a name")
	}
	opt.context = strings.TrimSpace(opt.context)
	return opt, fs.Args(), nil
}

var (
	valueFlags = map[string]bool{
		"context": true, "timeout": true, "o": true, "output": true,
		"name": true, "kind": true, "td-instance": true,
		"key-file": true, "secret-file": true, "passphrase-file": true, "key-type": true,
		"limit": true, "cursor": true,
		"phase": true, "active": true, "sts-instance": true,
		"session-id": true, "strategy-id": true, "created-by": true,
		"created-after": true, "created-before": true,
	}
	boolFlags = map[string]bool{"v": true, "verbose": true, "yes": true, "version": true, "h": true, "help": true}
)

// splitArgs pulls flags in front of positionals so flag.FlagSet sees every
// flag, including ones written after the command. Unknown flags fail here,
// and the error names the flag without its value.
func splitArgs(args []string) (flags, pos []string, err error) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			pos = append(pos, args[i+1:]...)
			break
		}
		if a == "-" || !strings.HasPrefix(a, "-") {
			pos = append(pos, a)
			continue
		}
		body := strings.TrimLeft(a, "-")
		name, _, _ := strings.Cut(body, "=")
		if name == "" {
			return nil, nil, fmt.Errorf("flag provided but not defined: %s", a)
		}
		switch {
		case boolFlags[name]:
			flags = append(flags, a)
		case valueFlags[name]:
			if strings.Contains(body, "=") {
				flags = append(flags, a)
				continue
			}
			if i+1 >= len(args) {
				return nil, nil, fmt.Errorf("flag needs an argument: -%s", name)
			}
			flags = append(flags, a, args[i+1])
			i++
		default:
			return nil, nil, fmt.Errorf("flag provided but not defined: -%s", name)
		}
	}
	return flags, pos, nil
}

func versionLine() string { return buildinfo.String() }
