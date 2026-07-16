package server

import (
	"testing"

	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"

	"github.com/bbockelm/htcondordb/command"
)

// TestServeOptionsFor locks the level -> (read-only, private) mapping that is the
// crux of the security model: READ is read-only and strips secrets; WRITE is full
// read/write but STILL strips secrets; only DAEMON sees private (secret) attributes.
func TestServeOptionsFor(t *testing.T) {
	cases := []struct {
		level          Level
		wantReadOnly   bool
		wantPrivate    bool
		wantPrivileged bool
	}{
		{LevelRead, true, false, false},
		{LevelWrite, false, false, false},
		{LevelDaemon, false, true, true},
	}
	for _, tc := range cases {
		got := serveOptionsFor(tc.level)
		if got.ReadOnly != tc.wantReadOnly || got.IncludePrivate != tc.wantPrivate || got.Privileged != tc.wantPrivileged {
			t.Errorf("serveOptionsFor(%s) = {ReadOnly:%v, IncludePrivate:%v, Privileged:%v}, want {%v, %v, %v}",
				tc.level, got.ReadOnly, got.IncludePrivate, got.Privileged, tc.wantReadOnly, tc.wantPrivate, tc.wantPrivileged)
		}
	}
}

// TestEffectiveLevel verifies the escalation: a peer gets the highest level it
// holds, and only READ (never lower) once the command has been admitted.
func TestEffectiveLevel(t *testing.T) {
	// grants maps user -> the single highest perm the fake authorizer will
	// grant them (plus everything that implies it, which the fake models).
	implies := map[string][]string{
		"DAEMON": {"DAEMON", "WRITE", "READ"},
		"WRITE":  {"WRITE", "READ"},
		"READ":   {"READ"},
	}
	grants := map[string]string{
		"root@pool":   "DAEMON",
		"admin@pool":  "WRITE",
		"alice@pool":  "READ",
		"nobody@pool": "",
	}
	fake := func(perm, _ /*addr*/, user string) bool {
		held := grants[user]
		for _, p := range implies[held] {
			if p == perm {
				return true
			}
		}
		return false
	}
	svc := &Service{authorize: fake}

	cases := []struct {
		user string
		want Level
	}{
		{"root@pool", LevelDaemon},
		{"admin@pool", LevelWrite},
		{"alice@pool", LevelRead},
		{"nobody@pool", LevelRead}, // no WRITE/DAEMON -> falls back to READ
		{"", LevelRead},            // unauthenticated peer that reached the handler
	}
	for _, tc := range cases {
		c := &cedarserver.Conn{
			RemoteAddr:  "192.0.2.1:5000",
			Negotiation: &security.SecurityNegotiation{User: tc.user},
		}
		if got := svc.effectiveLevel(c); got != tc.want {
			t.Errorf("effectiveLevel(user=%q) = %s, want %s", tc.user, got, tc.want)
		}
	}
}

// TestNewRequiresAuthorize guards the constructor contract.
func TestNewRequiresAuthorize(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("New with no Authorize should error")
	}
}

// TestCommandInRetiredRange documents that the session command sits in the
// retired transferd block, well clear of live HTCondor command ints.
func TestCommandInRetiredRange(t *testing.T) {
	if command.DBSession != 74000 {
		t.Fatalf("DBSession = %d, want 74000 (retired TRANSFERD_BASE)", command.DBSession)
	}
}
