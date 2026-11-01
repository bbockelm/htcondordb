module github.com/bbockelm/htcondordb

go 1.26.0

// The columnar SIMD engine in classad/collections/vm is built under
// GOEXPERIMENT=simd, and its arm64 half needs simd/archsimd from Go 1.27 (the
// amd64 half compiles on 1.26). Building this module with the experiment on an
// older toolchain fails inside the dependency with "undefined: archsimd.*", so
// the toolchain is guaranteed here rather than left to whatever the host has.
//
// This is a MINIMUM, not a pin: with GOTOOLCHAIN=auto (the default) Go uses the
// newer of the local toolchain and this line, so a host on 1.27.5 stays on
// 1.27.5 and picks up security patches without an edit here. `go` still accepts
// the module at the 1.26.0 language floor above, and a `toolchain` line is
// ignored in a dependency, so consumers of this module keep their own floor.
toolchain go1.27.0

require (
	github.com/PelicanPlatform/classad v0.34.3-0.20261101015711-f90fc4ce5dc9
	github.com/PelicanPlatform/classad/changefeed v0.34.3-0.20261101015711-f90fc4ce5dc9
	github.com/PelicanPlatform/classad/collections v0.34.3-0.20261101015711-f90fc4ce5dc9
	github.com/PelicanPlatform/classad/db v0.34.3-0.20261101015711-f90fc4ce5dc9
	github.com/PelicanPlatform/classad/dbrpc v0.34.3-0.20261101015711-f90fc4ce5dc9
	github.com/bbockelm/cedar v0.7.12
	github.com/bbockelm/golang-htcondor v0.16.4-0.20260926032345-41c11247a087
	github.com/chzyer/readline v1.5.1
	github.com/dustin/go-humanize v1.1.0
	github.com/hashicorp/go-hclog v1.6.3
	github.com/hashicorp/raft v1.8.0
	github.com/hashicorp/raft-boltdb/v2 v2.4.2
	github.com/prometheus/client_golang v1.25.0
	github.com/prometheus/client_model v0.6.3
	golang.org/x/sys v0.49.0
)

require (
	github.com/RoaringBitmap/roaring/v2 v2.29.0 // indirect
	github.com/bbockelm/gosssd v0.0.4 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgraph-io/ristretto/v2 v2.4.2 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/glebarez/go-sqlite v1.23.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-metrics v0.7.0 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/klauspost/compress v1.20.1 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pressly/goose/v3 v3.27.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/sethvargo/go-retry v0.4.0 // indirect
	github.com/tidwall/btree v1.8.1 // indirect
	go.etcd.io/bbolt v1.4.1 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	modernc.org/libc v1.74.3 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
	modernc.org/sqlite v1.55.0 // indirect
)
