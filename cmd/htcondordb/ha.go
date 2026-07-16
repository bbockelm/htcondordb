package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/message"
	cedarserver "github.com/bbockelm/cedar/server"
	"github.com/bbockelm/cedar/stream"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"
	"github.com/hashicorp/raft"

	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/ha/consistent"
	"github.com/bbockelm/htcondordb/ha/leaderfollower"
	"github.com/bbockelm/htcondordb/server"
)

// HA modes (HTCONDORDB_HA_MODE).
const (
	modeStandalone     = "standalone"
	modeLeaderFollower = "leader-follower"
	modeConsistent     = "consistent"
)

// haRuntime captures the daemon's resolved HA configuration.
type haRuntime struct {
	mode       string
	role       string // "leader" or "follower" in leader-follower mode
	leaderAddr string
	// forceReadOnly is true for a follower (writes go to the leader) and for a
	// non-leader raft node (writes are redirected/proxied to the raft leader).
	forceReadOnly bool

	coord *consistent.Coordinator // set in consistent mode
}

// detectHA reads the HA knobs and validates them.
func detectHA(cfg *config.Config) (*haRuntime, error) {
	mode := strings.ToLower(strings.TrimSpace(getStr(cfg, "HTCONDORDB_HA_MODE")))
	if mode == "" {
		mode = modeStandalone
	}
	h := &haRuntime{mode: mode}
	switch mode {
	case modeStandalone:
		return h, nil
	case modeLeaderFollower:
		h.role = strings.ToLower(strings.TrimSpace(getStr(cfg, "HTCONDORDB_ROLE")))
		if h.role == "" {
			h.role = "leader"
		}
		switch h.role {
		case "leader":
			return h, nil
		case "follower":
			h.leaderAddr = strings.TrimSpace(getStr(cfg, "HTCONDORDB_LEADER"))
			if h.leaderAddr == "" {
				return nil, fmt.Errorf("HTCONDORDB_ROLE=follower requires HTCONDORDB_LEADER (the leader's address)")
			}
			h.forceReadOnly = true
			return h, nil
		default:
			return nil, fmt.Errorf("invalid HTCONDORDB_ROLE %q (want leader or follower)", h.role)
		}
	case modeConsistent:
		// The raft-backed consistent mode is coordinated in ha/consistent. Writes are NOT
		// server-side read-only here: they reach the store's commit path, where a propose
		// hook (set in startConsistent) routes them through raft instead of committing
		// locally -- the leader applies them by quorum; a non-leader's proposal returns a
		// not-leader error the client sees. Reads are served locally on every node.
		return h, nil
	default:
		return nil, fmt.Errorf("invalid HTCONDORDB_HA_MODE %q (want standalone, leader-follower, or consistent)", mode)
	}
}

// start launches any background HA machinery: a follower's replicator, or (in
// consistent mode) the raft coordinator and its CEDAR command handlers. srv is
// the command server the raft/control handlers are registered on (before Serve);
// advertise is this daemon's externally reachable command address. It returns
// immediately; background work runs until ctx is cancelled.
func (h *haRuntime) start(ctx context.Context, d *daemon.Daemon, cfg *config.Config, svc *server.Service, srv *cedarserver.Server, advertise string) error {
	switch {
	case h.mode == modeLeaderFollower && h.role == "follower":
		return h.startFollower(ctx, d, cfg, svc)
	case h.mode == modeConsistent:
		return h.startConsistent(ctx, d, cfg, svc, srv, advertise)
	default:
		return nil
	}
}

// close releases HA resources (the raft coordinator). Safe to call always.
func (h *haRuntime) close() {
	if h.coord != nil {
		_ = h.coord.Close()
	}
}

// startFollower starts the leader-follower replicator against the configured
// leader, feeding the local database.
func (h *haRuntime) startFollower(ctx context.Context, d *daemon.Daemon, cfg *config.Config, svc *server.Service) error {
	dial := func(dctx context.Context) (*dbrpc.Client, error) {
		sec, err := htcondor.GetSecurityConfig(cfg, command.DBSession, "CLIENT")
		if err != nil {
			return nil, err
		}
		sec.Command = command.DBSession
		cl, err := cedarclient.ConnectAndAuthenticate(dctx, h.leaderAddr, sec)
		if err != nil {
			return nil, err
		}
		// Closing the dbrpc client closes the underlying CEDAR stream, releasing
		// the connection; a reconnect dials afresh.
		return dbrpc.NewClient(dbrpc.NewCedarConn(ctx, cl.GetStream())), nil
	}

	repl, err := leaderfollower.NewReplicator(leaderfollower.ReplicatorConfig{
		Catalog:   svc.Catalog(),
		Dial:      dial,
		CursorDir: followerCursorFile(cfg),
		Logger:    d.Slog(),
	})
	if err != nil {
		return err
	}
	d.Logger().Info(logging.DestinationGeneral, "starting leader-follower replication", "leader", h.leaderAddr)
	go func() {
		if err := repl.Run(ctx); err != nil && ctx.Err() == nil {
			d.Logger().Error(logging.DestinationGeneral, "replicator stopped", "err", err.Error())
		}
	}()
	return nil
}

// followerCursorFile is the directory where the follower persists its per-table stream
// cursors (HTCONDORDB_CURSOR_DIR, default $(SPOOL)/htcondordb/replica_cursors).
func followerCursorFile(cfg *config.Config) string {
	if v := strings.TrimSpace(getStr(cfg, "HTCONDORDB_CURSOR_DIR")); v != "" {
		return v
	}
	if v := strings.TrimSpace(getStr(cfg, "HTCONDORDB_CURSOR_FILE")); v != "" {
		return v // legacy name; treated as a directory now
	}
	if spool := strings.TrimSpace(getStr(cfg, "SPOOL")); spool != "" {
		return filepath.Join(spool, "htcondordb", "replica_cursors")
	}
	return ""
}

// startConsistent builds the raft coordinator, registers the CEDAR raft
// transport and control command handlers, and starts consensus. Reads are served
// locally on every node (read-only); writes are proposed to raft and committed by
// quorum, with non-leaders redirecting clients to the leader.
func (h *haRuntime) startConsistent(ctx context.Context, d *daemon.Daemon, cfg *config.Config, svc *server.Service, srv *cedarserver.Server, advertise string) error {
	nodeID := strings.TrimSpace(getStr(cfg, "HTCONDORDB_NODE_ID"))
	if nodeID == "" {
		nodeID = advertise
	}
	peers, err := parsePeers(getStr(cfg, "HTCONDORDB_RAFT_PEERS"))
	if err != nil {
		return err
	}

	// CEDAR dial to a peer's raft command: raft's byte stream tunnels over the
	// authenticated, encrypted CEDAR session.
	dial := func(dctx context.Context, addr string, timeout time.Duration) (*stream.Stream, error) {
		sec, err := htcondor.GetSecurityConfig(cfg, command.DBRaft, "CLIENT")
		if err != nil {
			return nil, err
		}
		sec.Command = command.DBRaft
		cl, err := cedarclient.ConnectAndAuthenticate(dctx, addr, sec)
		if err != nil {
			return nil, err
		}
		return cl.GetStream(), nil
	}

	coord, err := consistent.NewCoordinator(consistent.CoordinatorConfig{
		NodeID:      nodeID,
		Advertise:   advertise,
		Catalog:     svc.Catalog(),
		Dial:        dial,
		DataDir:     filepath.Join(databaseDir(d, cfg), "raft"),
		Bootstrap:   configBool(cfg, "HTCONDORDB_RAFT_BOOTSTRAP"),
		Peers:       peers,
		ClusterSize: configInt(cfg, "HTCONDORDB_RAFT_SIZE"),
		Logger:      d.Slog(),
	})
	if err != nil {
		return err
	}
	h.coord = coord

	// Route client writes through raft: every committing transaction's ops become a
	// table-qualified raft batch, applied by the FSM (the store's sole writer) once a
	// quorum commits. A proposal on a non-leader returns a not-leader error to the client.
	// Set before serving begins (start runs before d.Serve).
	svc.RPC().SetProposeHook(func(table string, ops []dbrpc.WriteOp) error {
		b := consistent.NewBatch()
		for _, op := range ops {
			switch op.Kind {
			case dbrpc.WriteNewClassAd:
				b.NewClassAdIn(table, op.Key, op.Value)
			case dbrpc.WriteDestroyClassAd:
				b.DestroyClassAdIn(table, op.Key)
			case dbrpc.WriteSetAttribute:
				b.SetAttributeIn(table, op.Key, op.Name, op.Value)
			case dbrpc.WriteDeleteAttribute:
				b.DeleteAttributeIn(table, op.Key, op.Name)
			}
		}
		if b.Empty() {
			return nil
		}
		return coord.Apply(b)
	})

	// DBRaft carries the raft transport: hand each accepted CEDAR stream to raft
	// and block until raft is done with it (so the server keeps the socket open).
	srv.Handle(command.DBRaft, func(hctx context.Context, c *cedarserver.Conn) error {
		done, err := coord.Layer().DeliverWait(c.Stream)
		if err != nil {
			return err
		}
		select {
		case <-done:
		case <-hctx.Done():
		}
		return nil
	}, "DAEMON")

	// DBControl carries the ClassAd request/response control protocol
	// (leader-discovery, peer registration, write-batch submission).
	srv.Handle(command.DBControl, func(hctx context.Context, c *cedarserver.Conn) error {
		req := message.NewMessageFromStream(c.Stream)
		reqAd, err := req.GetClassAd(hctx)
		if err != nil {
			return err
		}
		respAd := coord.HandleControl(reqAd)
		resp := message.NewMessageForStream(c.Stream)
		return resp.PutClassAd(hctx, respAd)
	}, "WRITE")

	d.Logger().Info(logging.DestinationGeneral, "consistent HA (raft over CEDAR) started",
		"node_id", nodeID, "advertise", advertise, "bootstrap", configBool(cfg, "HTCONDORDB_RAFT_BOOTSTRAP"),
		"cluster_size", configInt(cfg, "HTCONDORDB_RAFT_SIZE"), "peers", len(peers))
	return nil
}

// parsePeers parses "id1@addr1 id2@addr2 ..." into raft servers.
func parsePeers(s string) ([]raft.Server, error) {
	var out []raft.Server
	for _, tok := range strings.Fields(s) {
		id, addr, ok := strings.Cut(tok, "@")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("invalid HTCONDORDB_RAFT_PEERS entry %q (want id@address)", tok)
		}
		out = append(out, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(addr)})
	}
	return out, nil
}

func configBool(cfg *config.Config, key string) bool {
	v := strings.ToLower(strings.TrimSpace(getStr(cfg, key)))
	return v == "true" || v == "1" || v == "yes" || v == "t"
}

func configInt(cfg *config.Config, key string) int {
	var n int
	if _, err := fmt.Sscanf(strings.TrimSpace(getStr(cfg, key)), "%d", &n); err != nil {
		return 0
	}
	return n
}

func getStr(cfg *config.Config, key string) string {
	v, _ := cfg.Get(key)
	return v
}
