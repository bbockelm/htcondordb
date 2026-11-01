package flightsvc

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// fixedAuth authenticates to one identity, or refuses.
type fixedAuth struct {
	user string
	err  error
}

func (f fixedAuth) Name() string { return "fixed" }
func (f fixedAuth) Authenticate(context.Context, metadata.MD) (string, error) {
	return f.user, f.err
}

// allowLevels authorizes an identity for the named levels and nothing else.
func allowLevels(user string, levels ...string) Authorizer {
	set := map[string]bool{}
	for _, l := range levels {
		set[l] = true
	}
	return func(perm, _, u string) bool { return u == user && set[perm] }
}

func store(t *testing.T) (*dbrpc.Server, func()) {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		// ClaimId is a private attribute: a READ peer must never see it, and the loopback
		// is what is supposed to guarantee that without a second policy here.
		ad, perr := classad.ParseOld(fmt.Sprintf(
			"Owner = \"alice\"\nClusterId = %d\nClaimId = \"secret-%d\"", i, i))
		if perr != nil {
			t.Fatal(perr)
		}
		if err := d.Put(fmt.Sprintf("k%d", i), ad); err != nil {
			t.Fatal(err)
		}
	}
	s := dbrpc.NewServer(d)
	return s, func() { s.Close(); d.Close() }
}

// TestNewSessionsRefusesWithoutAuthenticator: the failure mode of a permissive default here is
// an open, unauthenticated read endpoint onto the job queue, so a missing authenticator is an
// error rather than something defaulted.
func TestNewSessionsRefusesWithoutAuthenticator(t *testing.T) {
	cat, done := store(t)
	defer done()
	if _, err := NewSessions(cat, nil, allowLevels("alice", "READ"), arrowtype.Options{}); err == nil {
		t.Error("a nil authenticator was accepted")
	}
	if _, err := NewSessions(cat, fixedAuth{user: "alice"}, nil, arrowtype.Options{}); err == nil {
		t.Error("a nil authorizer was accepted")
	}
}

func TestSessionRefusesUnauthenticated(t *testing.T) {
	cat, done := store(t)
	defer done()

	for _, c := range []struct {
		name string
		auth Authenticator
		want codes.Code
	}{
		{"authentication failed", fixedAuth{err: fmt.Errorf("bad token")}, codes.Unauthenticated},
		{"no identity returned", fixedAuth{user: ""}, codes.Unauthenticated},
		{"not authorized to read", fixedAuth{user: "bob"}, codes.PermissionDenied},
	} {
		s, err := NewSessions(cat, c.auth, allowLevels("alice", "READ"), arrowtype.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_, _, gerr := s.For(context.Background())
		if gerr == nil {
			t.Errorf("%s: accepted, want refusal", c.name)
			continue
		}
		if got := status.Code(gerr); got != c.want {
			t.Errorf("%s: code %v, want %v (%v)", c.name, got, c.want, gerr)
		}
	}
}

// TestReadLevelPeerCannotSeePrivateAttributes is what the loopback is FOR. dbrpc already
// strips private attributes at READ; routing through it means a Flight reader gets the same
// treatment by construction, with no second policy in this package to keep in agreement.
func TestReadLevelPeerCannotSeePrivateAttributes(t *testing.T) {
	cat, done := store(t)
	defer done()

	read, err := NewSessions(cat, fixedAuth{user: "alice"}, allowLevels("alice", "READ"), arrowtype.Options{})
	if err != nil {
		t.Fatal(err)
	}
	client, closeRead, err := read.For(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeRead()
	schema := mustSchema(t, client)
	if len(schema.FieldIndices("ClaimId")) != 0 {
		t.Error("a READ peer was served a ClaimId column")
	}
	if len(schema.FieldIndices("Owner")) == 0 {
		t.Error("a READ peer lost Owner too; the filter is too wide to prove anything")
	}

	// A DAEMON peer does see it, matching what the same identity gets over CEDAR.
	// Withholding it here would be a second, quieter policy.
	daemon, err := NewSessions(cat, fixedAuth{user: "alice"},
		allowLevels("alice", "READ", "WRITE", "DAEMON"), arrowtype.Options{})
	if err != nil {
		t.Fatal(err)
	}
	dclient, closeDaemon, err := daemon.For(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeDaemon()
	if len(mustSchema(t, dclient).FieldIndices("ClaimId")) == 0 {
		t.Error("a DAEMON peer was denied ClaimId, which it would see over CEDAR")
	}
}

// TestFlightIsReadOnlyEvenForDaemon: enabling an endpoint must never widen what a principal
// can do. A DAEMON peer reads private attributes and still cannot write.
func TestFlightIsReadOnlyEvenForDaemon(t *testing.T) {
	cat, done := store(t)
	defer done()
	s, err := NewSessions(cat, fixedAuth{user: "alice"},
		allowLevels("alice", "READ", "WRITE", "DAEMON"), arrowtype.Options{})
	if err != nil {
		t.Fatal(err)
	}
	opts := levelOptions(allowLevels("alice", "READ", "WRITE", "DAEMON"), "", "alice")
	if !opts.ReadOnly {
		t.Error("a DAEMON peer's session is not read-only")
	}
	if !opts.IncludePrivate {
		t.Error("a DAEMON peer's session hides private attributes, unlike over CEDAR")
	}
	// And the session the loopback actually serves refuses a write.
	client, closeSess, err := s.For(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer closeSess()
	// The write must be refused, and this records WHICH step refuses it. Measured today:
	// Begin succeeds on a read-only session and the insert is what fails -- so the write
	// path is genuinely exercised here rather than short-circuited at Begin. Asserting
	// "refused somewhere" keeps the test honest if that moves.
	ctx := context.Background()
	ad, _ := classad.ParseOld("Owner = \"mallory\"")
	refusedAt := ""
	if tx, terr := client.Begin(ctx); terr != nil {
		refusedAt = "Begin"
	} else if err := tx.NewClassAd(ctx, "injected", ad.String()); err != nil {
		refusedAt = "NewClassAd"
	} else if err := tx.Commit(ctx); err != nil {
		refusedAt = "Commit"
	}
	if refusedAt == "" {
		t.Error("a write through a Flight session succeeded end to end")
	} else {
		t.Logf("write refused at %s", refusedAt)
	}
}

func TestBearerToken(t *testing.T) {
	for _, c := range []struct {
		md   metadata.MD
		want string
	}{
		{metadata.Pairs("authorization", "Bearer abc123"), "abc123"},
		{metadata.Pairs("authorization", "bearer abc123"), "abc123"},
		{metadata.Pairs("authorization", "abc123"), "abc123"}, // bare, accepted
		{metadata.Pairs("x-flight-token", "abc123"), "abc123"},
		{metadata.Pairs("other", "abc123"), ""},
	} {
		got, ok := BearerToken(c.md)
		if c.want == "" {
			if ok {
				t.Errorf("%v: found %q, want none", c.md, got)
			}
			continue
		}
		if !ok || got != c.want {
			t.Errorf("%v: got %q/%v, want %q", c.md, got, ok, c.want)
		}
	}
}

func mustSchema(t *testing.T, client *dbrpc.Client) *arrow.Schema {
	t.Helper()
	srv, err := NewServer(client, arrowtype.Options{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	schema, _, err := srv.tableSchema(context.Background(), dbrpc.DefaultTable)
	if err != nil {
		t.Fatalf("deriving the schema: %v", err)
	}
	if schema.NumFields() == 0 {
		t.Fatal("empty schema")
	}
	return schema
}
