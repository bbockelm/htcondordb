package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/message"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/userfunc"
)

// The `function` subcommand: the administrative path to the user functions stored in the
// database, over DBUserFuncControl.
//
// It is a subcommand rather than a shell meta-command because what it carries is a FILE -- a
// script or a compiled module -- and a shell line is a bad place to put one. The daemon it talks
// to is the one the ordinary connection flags resolve, so `-addr`/`-pool`/`-name` work as they do
// everywhere else.
//
//	htcondordb-cli function list
//	htcondordb-cli function register pctUsed -file pct.cas
//	htcondordb-cli function register mean -kind wasm -file mean.wasm -arity 2
//	htcondordb-cli function replace pctUsed -file pct2.cas
//	htcondordb-cli function remove pctUsed
//
// Registering is authorized at DAEMON or ADMINISTRATOR, not WRITE: a user function runs inside
// every query that names it, the name space is global, and a name that has been claimed cannot be
// given back.

// functionUsage is printed when the arguments do not make a request.
const functionUsage = `htcondordb-cli function <list|register|replace|remove> [name] [flags]

  list                       what this daemon has registered, with each function's state
  register <name>            claim a name for a stored function
  replace <name>             point an existing stored function at new source, in place
  remove <name>              delete a stored function's record

  -file <path>               the source: ClassAd Script text, or a .wasm module
  -kind <script|wasm>        default: wasm for a .wasm file, script otherwise
  -arity <n>                 argument count; required for wasm, taken from the def for a script
  -max-arity <n>             largest accepted count (-1 variadic); default: -arity

A replacement may not change the argument count: a compiled query has already captured it.
Registering requires DAEMON or ADMINISTRATOR at the daemon.
`

// functionArgs is one parsed `function` command line.
type functionArgs struct {
	action string
	name   string
	file   string
	kind   string
	arity  int
	// maxArity is only meaningful when maxAritySet is, because -1 is a VALUE here -- a variadic
	// function -- and cannot double as "not given".
	maxArity    int
	maxAritySet bool
}

// parseFunctionArgs reads the subcommand's own flags out of what the global parser left behind.
func parseFunctionArgs(args []string) (functionArgs, error) {
	fa := functionArgs{arity: -1}
	var positional []string
	for i := 0; i < len(args); i++ {
		next := func() (string, bool) {
			if i+1 < len(args) {
				i++
				return args[i], true
			}
			return "", false
		}
		switch args[i] {
		case "-file", "--file":
			v, ok := next()
			if !ok {
				return fa, fmt.Errorf("-file needs a path")
			}
			fa.file = v
		case "-kind", "--kind":
			v, ok := next()
			if !ok {
				return fa, fmt.Errorf("-kind needs a value")
			}
			fa.kind = strings.ToLower(v)
		case "-arity", "--arity":
			v, ok := next()
			if !ok {
				return fa, fmt.Errorf("-arity needs a number")
			}
			n, err := strconv.Atoi(v)
			if err != nil {
				return fa, fmt.Errorf("-arity: %w", err)
			}
			fa.arity = n
		case "-max-arity", "--max-arity":
			v, ok := next()
			if !ok {
				return fa, fmt.Errorf("-max-arity needs a number")
			}
			n, err := strconv.Atoi(v)
			if err != nil {
				return fa, fmt.Errorf("-max-arity: %w", err)
			}
			fa.maxArity, fa.maxAritySet = n, true
		default:
			positional = append(positional, args[i])
		}
	}
	if len(positional) > 0 {
		fa.action = strings.ToLower(positional[0])
	}
	if len(positional) > 1 {
		fa.name = positional[1]
	}
	return fa, nil
}

// request builds the request ad, reading the source file where one is needed.
func (fa functionArgs) request() (*classad.ClassAd, error) {
	req := classad.New()
	req.InsertAttrString("Action", fa.action)
	if fa.action == "list" {
		return req, nil
	}
	if fa.name == "" {
		return nil, fmt.Errorf("%s needs the function's name", fa.action)
	}
	req.InsertAttrString("Name", fa.name)
	if fa.action == "remove" {
		return req, nil
	}
	if fa.file == "" {
		return nil, fmt.Errorf("%s needs -file <path>", fa.action)
	}
	body, err := os.ReadFile(fa.file)
	if err != nil {
		return nil, err
	}
	kind := fa.kind
	if kind == "" {
		// A .wasm file is a module; anything else is script text. Guessing from the extension is
		// safe to get wrong -- a module read as script fails to parse, and says so -- and it
		// spares the common case a flag.
		kind = userfunc.KindScript
		if strings.EqualFold(filepath.Ext(fa.file), ".wasm") {
			kind = userfunc.KindWasm
		}
	}
	req.InsertAttrString(userfunc.AttrKind, kind)
	if kind == userfunc.KindWasm {
		req.InsertAttrString(userfunc.AttrSource, base64.StdEncoding.EncodeToString(body))
	} else {
		req.InsertAttrString(userfunc.AttrSource, string(body))
	}
	// Sent only when stated. Absent means "work it out", which for a script is reading the count
	// off the def -- zero is a legitimate count, so it cannot stand in for unstated.
	if fa.arity >= 0 {
		req.InsertAttr(userfunc.AttrArity, int64(fa.arity))
	}
	if fa.maxAritySet {
		req.InsertAttr(userfunc.AttrMaxArity, int64(fa.maxArity))
	}
	return req, nil
}

// runFunction sends one DBUserFuncControl request and prints what came back.
func runFunction(ctx context.Context, cfg *config.Config, addr string, args []string) error {
	fa, err := parseFunctionArgs(args)
	if err != nil {
		return err
	}
	switch fa.action {
	case "list", "register", "replace", "remove":
	case "":
		fa.action = "list"
	default:
		fmt.Fprint(os.Stderr, functionUsage)
		return fmt.Errorf("unknown function action %q", fa.action)
	}
	req, err := fa.request()
	if err != nil {
		fmt.Fprint(os.Stderr, functionUsage)
		return err
	}

	sec, err := htcondor.GetSecurityConfig(cfg, command.DBUserFuncControl, "CLIENT")
	if err != nil {
		return err
	}
	sec.Command = command.DBUserFuncControl
	dctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cl, err := cedarclient.ConnectAndAuthenticate(dctx, addr, sec)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", addr, err)
	}
	defer func() { _ = cl.Close() }()
	s := cl.GetStream()

	out := message.NewMessageForStream(s)
	if err := out.PutClassAd(dctx, req); err != nil {
		return err
	}
	if err := out.FinishMessage(dctx); err != nil {
		return err
	}
	respAd, err := message.NewMessageFromStream(s).GetClassAd(dctx)
	if err != nil {
		return err
	}
	if ok, _ := respAd.EvaluateAttrBool("Ok"); !ok {
		msg, _ := respAd.EvaluateAttrString("Error")
		if msg == "" {
			msg = fa.action + " failed"
		}
		return fmt.Errorf("%s", msg)
	}
	count, _ := respAd.EvaluateAttrInt("Count")
	rows := make([]*classad.ClassAd, 0, count)
	for range count {
		ad, rerr := message.NewMessageFromStream(s).GetClassAd(dctx)
		if rerr != nil {
			return rerr
		}
		rows = append(rows, ad)
	}
	if fa.action == "list" {
		printFunctionList(os.Stdout, rows)
		return nil
	}
	note, _ := respAd.EvaluateAttrString("Note")
	fmt.Println(note)
	return nil
}

// printFunctionList prints the listing, state included.
//
// The state column is the point of the listing: a function that is BAD answers ERROR in every
// query that calls it, and an operator should be able to find that out here rather than by
// noticing that a column has gone wrong.
func printFunctionList(w io.Writer, rows []*classad.ClassAd) {
	if len(rows) == 0 {
		fmt.Fprintln(w, "no user functions are registered")
		return
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tKIND\tORIGIN\tARGS\tSTATE\tIDENTITY")
	var reasons []string
	for _, ad := range rows {
		name, _ := ad.EvaluateAttrString(userfunc.AttrName)
		kind, _ := ad.EvaluateAttrString(userfunc.AttrKind)
		origin, _ := ad.EvaluateAttrString("Origin")
		state, _ := ad.EvaluateAttrString(userfunc.AttrState)
		ident, _ := ad.EvaluateAttrString(userfunc.AttrChecksum)
		if ident == "" {
			ident, _ = ad.EvaluateAttrString("Path")
		}
		minA, _ := ad.EvaluateAttrInt(userfunc.AttrArity)
		maxA, _ := ad.EvaluateAttrInt(userfunc.AttrMaxArity)
		args := strconv.FormatInt(minA, 10)
		switch {
		case maxA < 0:
			args += "+"
		case maxA != minA:
			args += ".." + strconv.FormatInt(maxA, 10)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", name, kind, origin, args, state, shortIdentity(ident))
		if why, _ := ad.EvaluateAttrString(userfunc.AttrStateReason); why != "" {
			reasons = append(reasons, name+": "+why)
		}
	}
	_ = tw.Flush()
	for _, r := range reasons {
		fmt.Fprintln(w, "\n"+r)
	}
}

// shortIdentity abbreviates a checksum for the table, keeping enough of it to tell two instances
// apart while leaving the column readable. A path is left alone.
func shortIdentity(s string) string {
	hex, ok := strings.CutPrefix(s, "sha256:")
	if !ok || len(hex) < 12 {
		return s
	}
	return "sha256:" + hex[:12]
}
