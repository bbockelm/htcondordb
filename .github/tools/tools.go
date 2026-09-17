//go:build tools

// Package tools pins the external binaries CI builds. It is never imported;
// the build tag keeps it out of ordinary builds while still making the import
// a real dependency Dependabot can see and bump.
package tools

import (
	// nfpm: builds the release RPMs. Pinned here rather than
	// `go install ...@version` in a workflow, so the version is recorded
	// in this module's go.sum, verified on download, and bumped by
	// Dependabot -- instead of being a number that goes stale in a YAML
	// file nobody reviews.
	_ "github.com/goreleaser/nfpm/v2/cmd/nfpm"
)
