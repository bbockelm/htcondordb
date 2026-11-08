package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/message"
	cedarserver "github.com/bbockelm/cedar/server"

	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/server"
	"github.com/bbockelm/htcondordb/userfunc"
)

// ownerUserFunctions is the in-process writer that owns the stored-function table.
//
// The daemon owns it so that NO REMOTE SESSION CAN WRITE IT, at any level. That is not tidiness:
// a record in this table becomes a registered function at the next restart, so a client able to
// write one claims a name in the query language -- the administrative act this command exists to
// gate -- by another route. Ownership closes that route and leaves exactly one way in, the one
// below, which is gated at DAEMON or ADMINISTRATOR.
const ownerUserFunctions = "user-functions"

// claimUserFuncTable gives the stored-function table to this daemon, so that no remote session
// writes it at any level and the only way to register a name is the administrative command.
func claimUserFuncTable(owners *server.TableOwners) error {
	return owners.Claim(ownerUserFunctions, []string{userfunc.Table})
}

// userFuncController answers DBUserFuncControl requests: the administrative path to the user
// functions stored in the database.
//
// Request attributes:
//
//	Action = "list"     (default if absent)
//	Action = "register"  Name, Kind ("script" | "wasm"), Source, Arity, MaxArity
//	Action = "replace"   Name, Kind, Source, Arity, MaxArity   (the argument count may not change)
//	Action = "remove"    Name
//
// Response: Ok (bool); on failure Error (string); on success Note (string) and, for a listing,
// Count (int) followed by Count further ClassAds on the stream, one per registered function.
//
// The listing is a sequence of separate, FLAT ads rather than one ad carrying a list, because the
// ClassAd that goes over this wire is the old one: it has no nested ad and no list of ads, and
// flattening a listing into Name0/Kind0/State0... would make the protocol unreadable to keep it
// in one message for no gain.
type userFuncController struct {
	mgr *userfunc.Manager
}

// handle runs one request and returns the response ad and the listing that follows it.
func (uc *userFuncController) handle(reqAd *classad.ClassAd, who string) (*classad.ClassAd, []*classad.ClassAd) {
	resp := classad.New()
	action, _ := reqAd.EvaluateAttrString("Action")
	if action == "" {
		action = "list"
	}
	var note string
	var list []*classad.ClassAd
	var err error
	switch action {
	case "list":
		for _, st := range uc.mgr.List() {
			list = append(list, st.Ad())
		}
		note = fmt.Sprintf("%d user function(s) registered", len(list))
	case "register":
		note, err = uc.register(reqAd, who)
	case "replace":
		note, err = uc.replace(reqAd, who)
	case "remove":
		name, _ := reqAd.EvaluateAttrString("Name")
		note, err = uc.mgr.Remove(name)
	default:
		err = fmt.Errorf("unknown user-function action %q (want list, register, replace, or remove)", action)
	}
	if err != nil {
		return fail(resp, err.Error()), nil
	}
	resp.InsertAttrBool("Ok", true)
	resp.InsertAttrString("Note", note)
	if action == "list" {
		resp.InsertAttr("Count", int64(len(list)))
	}
	return resp, list
}

func (uc *userFuncController) register(reqAd *classad.ClassAd, who string) (string, error) {
	rec, err := recordFromRequest(reqAd, who)
	if err != nil {
		return "", err
	}
	stored, err := uc.mgr.Register(rec)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s is registered as a %s user function taking %d argument(s), running %s",
		stored.Name, stored.Kind, stored.Arity, stored.Checksum) + trustNote(rec), nil
}

func (uc *userFuncController) replace(reqAd *classad.ClassAd, who string) (string, error) {
	rec, err := recordFromRequest(reqAd, who)
	if err != nil {
		return "", err
	}
	stored, err := uc.mgr.Replace(rec)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s now runs %s; every query that names it reaches the new code from now on, "+
		"with no restart", stored.Name, stored.Checksum) + trustNote(rec), nil
}

// trustNote tells a registrant that a trust attribute was ignored. Ignored rather than refused,
// but never silently: a module that asked for a capability and did not get it should hear so,
// since the reason its calls still hit the deadline is exactly this.
func trustNote(rec userfunc.Record) string {
	if !rec.TrustRequested {
		return ""
	}
	return ". The Trusted attribute is ignored: trust removes a module's wall-clock deadline and " +
		"makes it uninterruptible, which is a capability an operator grants a file they pointed at " +
		"in the configuration -- it is not something a stored module can ask for"
}

// recordFromRequest reads the record out of a request. Arity is carried as "absent means work it
// out" rather than as zero, because zero is a legitimate argument count.
func recordFromRequest(reqAd *classad.ClassAd, who string) (userfunc.Record, error) {
	rec := userfunc.Record{Arity: -1, MaxArity: -1}
	rec.Name, _ = reqAd.EvaluateAttrString("Name")
	rec.Kind, _ = reqAd.EvaluateAttrString(userfunc.AttrKind)
	rec.Source, _ = reqAd.EvaluateAttrString(userfunc.AttrSource)
	if n, ok := reqAd.EvaluateAttrInt(userfunc.AttrArity); ok {
		rec.Arity = int(n)
	}
	if n, ok := reqAd.EvaluateAttrInt(userfunc.AttrMaxArity); ok {
		rec.MaxArity = int(n)
	} else {
		rec.MaxArity = rec.Arity
	}
	if b, ok := reqAd.EvaluateAttrBool("Trusted"); ok && b {
		rec.TrustRequested = true
	}
	// The identity the handshake proved, not one the request names. Provenance that the registrant
	// could write would say whatever they wanted it to say.
	rec.RegisteredBy = who
	if strings.TrimSpace(rec.Name) == "" {
		return rec, fmt.Errorf("a user function needs a name")
	}
	return rec, nil
}

// registerUserFuncControl installs the DBUserFuncControl handler on srv.
//
// DAEMON *or* ADMINISTRATOR: registering a function is an administrative act on a global, shared
// name space, and one a WRITE-level peer must not reach. Both levels are named because a pool's
// daemon identity and its human administrator are different principals and either may legitimately
// do this; cedar admits a peer authorized at any of the levels a command names.
func registerUserFuncControl(srv *cedarserver.Server, uc *userFuncController) {
	srv.Handle(command.DBUserFuncControl, func(hctx context.Context, c *cedarserver.Conn) error {
		req := message.NewMessageFromStream(c.Stream)
		reqAd, err := req.GetClassAd(hctx)
		if err != nil {
			return err
		}
		respAd, list := uc.handle(reqAd, peerIdentity(c))
		resp := message.NewMessageForStream(c.Stream)
		if err := resp.PutClassAd(hctx, respAd); err != nil {
			return err
		}
		if err := resp.FinishMessage(hctx); err != nil { // flush the frame (EOM); PutClassAd only buffers
			return err
		}
		for _, ad := range list {
			msg := message.NewMessageForStream(c.Stream)
			if err := msg.PutClassAd(hctx, ad); err != nil {
				return err
			}
			if err := msg.FinishMessage(hctx); err != nil {
				return err
			}
		}
		return nil
	}, "DAEMON", "ADMINISTRATOR")
}

// peerIdentity is the authenticated identity the dispatch authorized, which is what provenance
// records. An unauthenticated peer never reaches this command, so the empty case is only a peer
// whose negotiation this build did not keep.
func peerIdentity(c *cedarserver.Conn) string {
	if id := c.AuthorizationUser(); id != "" {
		return id
	}
	if c.Negotiation != nil {
		return c.Negotiation.User
	}
	return "unknown"
}
