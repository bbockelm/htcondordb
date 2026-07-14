// Package consistent implements htcondordb's "consistent" HA mode: a strongly
// consistent, quorum-replicated database backed by hashicorp/raft, with the raft
// transport tunneled over CEDAR so replication inherits HTCondor's authentication
// and encryption.
//
// A write is a Batch of mutations. On the leader it is proposed to the raft log;
// once a quorum has durably accepted it, every node's FSM applies the same batch
// to its local ClassAd store, so all replicas converge on identical state and no
// acknowledged write is ever lost while a quorum survives. Non-leader nodes
// redirect (or proxy) writers to the current leader; any node can serve reads.
//
// Membership is bootstrapped from the initial leader, which either is given the
// peer set explicitly or is told the cluster size N and adopts the first N
// daemon-authenticated peers that register, persisting that set through the raft
// configuration so it survives restarts.
package consistent

import (
	"encoding/json"
	"fmt"
)

// OpKind identifies a single mutation within a Batch.
type OpKind uint8

const (
	// OpNewClassAd stores Value (old-ClassAd text) under Key.
	OpNewClassAd OpKind = iota
	// OpDestroyClassAd removes Key.
	OpDestroyClassAd
	// OpSetAttribute sets Key's attribute Name to the expression Value.
	OpSetAttribute
	// OpDeleteAttribute removes Key's attribute Name.
	OpDeleteAttribute
)

// Op is one mutation. Fields not relevant to a kind are empty.
type Op struct {
	Kind  OpKind `json:"k"`
	Key   string `json:"key"`
	Name  string `json:"n,omitempty"`
	Value string `json:"v,omitempty"`
}

// Batch is an atomic set of mutations proposed to raft as one log entry; the FSM
// applies the whole batch in a single transaction, so it commits all-or-nothing
// on every replica.
type Batch struct {
	Ops []Op `json:"ops"`
}

// NewBatch starts an empty batch.
func NewBatch() *Batch { return &Batch{} }

// NewClassAd appends a store-ad mutation (adText is old-ClassAd format).
func (b *Batch) NewClassAd(key, adText string) *Batch {
	b.Ops = append(b.Ops, Op{Kind: OpNewClassAd, Key: key, Value: adText})
	return b
}

// DestroyClassAd appends a delete mutation.
func (b *Batch) DestroyClassAd(key string) *Batch {
	b.Ops = append(b.Ops, Op{Kind: OpDestroyClassAd, Key: key})
	return b
}

// SetAttribute appends a set-attribute mutation (expr is a ClassAd expression).
func (b *Batch) SetAttribute(key, name, expr string) *Batch {
	b.Ops = append(b.Ops, Op{Kind: OpSetAttribute, Key: key, Name: name, Value: expr})
	return b
}

// DeleteAttribute appends a delete-attribute mutation.
func (b *Batch) DeleteAttribute(key, name string) *Batch {
	b.Ops = append(b.Ops, Op{Kind: OpDeleteAttribute, Key: key, Name: name})
	return b
}

// Empty reports whether the batch has no operations.
func (b *Batch) Empty() bool { return len(b.Ops) == 0 }

// Encode serializes the batch for the raft log.
func (b *Batch) Encode() ([]byte, error) { return json.Marshal(b) }

// DecodeBatch parses a raft log payload back into a Batch.
func DecodeBatch(data []byte) (*Batch, error) {
	var b Batch
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("consistent: decoding batch: %w", err)
	}
	return &b, nil
}
