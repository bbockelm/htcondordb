module github.com/bbockelm/htcondordb

go 1.25.7

require (
	github.com/PelicanPlatform/classad v0.5.1
	github.com/PelicanPlatform/classad/db v0.0.0
	github.com/PelicanPlatform/classad/dbrpc v0.0.0
	github.com/bbockelm/cedar v0.5.2
	github.com/bbockelm/golang-htcondor v0.5.0
	github.com/chzyer/readline v1.5.1
	github.com/hashicorp/go-hclog v1.6.3
	github.com/hashicorp/raft v1.7.3
	github.com/hashicorp/raft-boltdb/v2 v2.3.1
)

require (
	github.com/PelicanPlatform/classad/collections v0.4.0 // indirect
	github.com/RoaringBitmap/roaring/v2 v2.19.0 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/bbockelm/gosssd v0.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-metrics v0.5.4 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.2 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/klauspost/compress v1.19.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.21 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/tidwall/btree v1.8.1 // indirect
	go.etcd.io/bbolt v1.3.5 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/time v0.14.0 // indirect
)

// In-development sibling modules: built from the local checkouts. These replaces
// are for local development only and must be resolved to tagged versions before
// this module is published with CI.
replace github.com/PelicanPlatform/classad => ../golang-classads

replace github.com/PelicanPlatform/classad/collections => ../golang-classads/collections

replace github.com/PelicanPlatform/classad/db => ../golang-classads/db

replace github.com/PelicanPlatform/classad/dbrpc => ../golang-classads/dbrpc

replace github.com/bbockelm/cedar => ../golang-cedar

replace github.com/bbockelm/golang-htcondor => ../golang-htcondor
