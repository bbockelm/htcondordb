package consistent

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// CoordinatorConfig configures a consistent-mode raft node.
type CoordinatorConfig struct {
	// NodeID is this node's stable raft identity (e.g. "htcondordb@host").
	NodeID string
	// Advertise is this node's raft address other nodes dial (a CEDAR sinful).
	Advertise string
	// Catalog is the table catalog the FSM applies to (its sole writer). Replication
	// covers every table, not just the default one.
	Catalog *db.Catalog
	// DefaultTable is where a table-unqualified op applies (default "ads").
	DefaultTable string
	// Dial opens a CEDAR raft connection to a peer. Required.
	Dial DialFunc
	// DataDir holds persistent raft snapshots. Required.
	DataDir string

	// Bootstrap marks this node as the initial leader that initializes a brand
	// new cluster. Only one node should set it, once.
	Bootstrap bool
	// Peers, if set on the bootstrap node, is the explicit initial member set
	// (including self). If empty, the cluster starts single-node and grows as
	// peers register (see ClusterSize / RegisterPeer).
	Peers []raft.Server
	// ClusterSize is N: when the bootstrap node is not given an explicit Peers
	// list, it adopts the first N members (itself plus the first N-1 peers that
	// register at DAEMON level) and then stops growing. 0 means "only those in
	// Peers" (or single-node if Peers is empty).
	ClusterSize int

	// Timeout bounds raft Apply / membership operations (default 10s).
	Timeout time.Duration
	// Logger receives coordinator diagnostics (raft's own logs go to hclog).
	Logger *slog.Logger
}

// Coordinator owns a raft node and exposes the write-proposal and
// leader-discovery surface the daemon needs.
type Coordinator struct {
	cfg     CoordinatorConfig
	log     *slog.Logger
	timeout time.Duration

	fsm   *FSM
	layer *StreamLayer
	trans *raft.NetworkTransport
	raft  *raft.Raft
	store *raftboltdb.BoltStore // durable log + stable store
	snaps raft.SnapshotStore

	mu      sync.Mutex
	members map[raft.ServerID]raft.ServerAddress // registered members (bootstrap tracking)
}

// NewCoordinator constructs the raft node, its CEDAR transport, and (on the
// bootstrap node of a fresh cluster) the initial configuration.
func NewCoordinator(cfg CoordinatorConfig) (*Coordinator, error) {
	if cfg.Catalog == nil {
		return nil, fmt.Errorf("consistent: Catalog is required")
	}
	if cfg.DefaultTable == "" {
		cfg.DefaultTable = "ads"
	}
	if cfg.Dial == nil {
		return nil, fmt.Errorf("consistent: Dial is required")
	}
	if cfg.NodeID == "" || cfg.Advertise == "" {
		return nil, fmt.Errorf("consistent: NodeID and Advertise are required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("consistent: DataDir is required")
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return nil, fmt.Errorf("consistent: creating data dir: %w", err)
	}

	hlog := hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Warn, Output: os.Stderr})

	fsm := NewFSM(cfg.Catalog, cfg.DefaultTable)
	layer := NewStreamLayer(cfg.Advertise, cfg.Dial)
	trans := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  layer,
		MaxPool: 3,
		Timeout: timeout,
		Logger:  hlog,
	})

	snaps, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		trans.Close()
		return nil, fmt.Errorf("consistent: snapshot store: %w", err)
	}
	// Durable log + stable store: membership and the log survive restarts, so a
	// node rejoins with its state intact rather than re-bootstrapping.
	store, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		trans.Close()
		return nil, fmt.Errorf("consistent: bolt store: %w", err)
	}

	rcfg := raft.DefaultConfig()
	rcfg.LocalID = raft.ServerID(cfg.NodeID)
	rcfg.Logger = hlog

	r, err := raft.NewRaft(rcfg, fsm, store, store, snaps, trans)
	if err != nil {
		_ = store.Close()
		trans.Close()
		return nil, fmt.Errorf("consistent: creating raft: %w", err)
	}

	c := &Coordinator{
		cfg:     cfg,
		log:     log,
		timeout: timeout,
		fsm:     fsm,
		layer:   layer,
		trans:   trans,
		raft:    r,
		store:   store,
		snaps:   snaps,
		members: map[raft.ServerID]raft.ServerAddress{},
	}

	if err := c.maybeBootstrap(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// maybeBootstrap initializes a fresh cluster from the bootstrap node.
func (c *Coordinator) maybeBootstrap() error {
	if !c.cfg.Bootstrap {
		return nil
	}
	// A restart with durable state must not re-bootstrap: the existing
	// configuration (including any peers added since) is authoritative.
	if has, err := raft.HasExistingState(c.store, c.store, c.snaps); err == nil && has {
		c.log.Info("consistent: existing raft state found; skipping bootstrap")
		if cfg := c.raft.GetConfiguration(); cfg.Error() == nil {
			for _, s := range cfg.Configuration().Servers {
				c.members[s.ID] = s.Address
			}
		}
		return nil
	}
	servers := c.cfg.Peers
	if len(servers) == 0 {
		// Start single-node; grow via RegisterPeer as members register.
		servers = []raft.Server{{
			ID:      raft.ServerID(c.cfg.NodeID),
			Address: raft.ServerAddress(c.cfg.Advertise),
		}}
	}
	fut := c.raft.BootstrapCluster(raft.Configuration{Servers: servers})
	if err := fut.Error(); err != nil && err != raft.ErrCantBootstrap {
		return fmt.Errorf("consistent: bootstrap: %w", err)
	}
	for _, s := range servers {
		c.members[s.ID] = s.Address
	}
	c.log.Info("consistent: bootstrapped raft cluster", "servers", len(servers), "cluster_size", c.cfg.ClusterSize)
	return nil
}

// Log/stable state is stored durably in boltdb (raft.db) and the FSM state in
// FileSnapshotStore snapshots, so a node's membership and log survive restarts.

// Layer returns the CEDAR stream layer, so the daemon's DBRaft command handler
// can Deliver accepted peer streams into the transport.
func (c *Coordinator) Layer() *StreamLayer { return c.layer }

// Apply proposes a write batch to the cluster. It returns raft.ErrNotLeader if
// this node is not the leader (the caller then redirects the client to
// LeaderAddr), an FSM error if the batch could not be applied, or nil on a
// quorum-committed success.
func (c *Coordinator) Apply(b *Batch) error {
	if b.Empty() {
		return nil
	}
	data, err := b.Encode()
	if err != nil {
		return err
	}
	fut := c.raft.Apply(data, c.timeout)
	if err := fut.Error(); err != nil {
		return err // includes raft.ErrNotLeader
	}
	if resp := fut.Response(); resp != nil {
		if e, ok := resp.(error); ok {
			return e
		}
	}
	return nil
}

// IsLeader reports whether this node is the current raft leader.
func (c *Coordinator) IsLeader() bool { return c.raft.State() == raft.Leader }

// LeaderAddr returns the current leader's raft address and id ("","" if unknown).
func (c *Coordinator) LeaderAddr() (addr string, id string) {
	a, i := c.raft.LeaderWithID()
	return string(a), string(i)
}

// RegisterPeer adds a peer as a voting member. Only the leader can do this; on a
// follower it returns raft.ErrNotLeader. It is idempotent and enforces the
// configured ClusterSize (peers beyond N are refused), implementing the
// "first N hosts to register" bootstrap.
func (c *Coordinator) RegisterPeer(id, addr string) error {
	if !c.IsLeader() {
		return raft.ErrNotLeader
	}
	c.mu.Lock()
	if _, known := c.members[raft.ServerID(id)]; known {
		c.mu.Unlock()
		return nil // already a member
	}
	if c.cfg.ClusterSize > 0 && len(c.members) >= c.cfg.ClusterSize {
		c.mu.Unlock()
		return fmt.Errorf("consistent: cluster is full (%d members); refusing %s", c.cfg.ClusterSize, id)
	}
	c.mu.Unlock()

	fut := c.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, c.timeout)
	if err := fut.Error(); err != nil {
		return fmt.Errorf("consistent: adding voter %s: %w", id, err)
	}
	c.mu.Lock()
	c.members[raft.ServerID(id)] = raft.ServerAddress(addr)
	n := len(c.members)
	c.mu.Unlock()
	c.log.Info("consistent: added raft voter", "id", id, "addr", addr, "members", n, "cluster_size", c.cfg.ClusterSize)
	return nil
}

// Members returns the currently registered member ids (for status/queries).
func (c *Coordinator) Members() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.members))
	for id := range c.members {
		out = append(out, string(id))
	}
	return out
}

// WaitForLeader blocks until a leader is elected or timeout elapses.
func (c *Coordinator) WaitForLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if addr, _ := c.LeaderAddr(); addr != "" {
			return addr, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "", fmt.Errorf("consistent: no leader elected within %s", timeout)
}

// Close shuts the raft node and transport down.
func (c *Coordinator) Close() error {
	fut := c.raft.Shutdown()
	err := fut.Error()
	if cerr := c.trans.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if lerr := c.layer.Close(); lerr != nil && err == nil {
		err = lerr
	}
	if c.store != nil {
		if serr := c.store.Close(); serr != nil && err == nil {
			err = serr
		}
	}
	return err
}
