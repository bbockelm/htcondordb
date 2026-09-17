# CI tool pins

This module exists so the external binaries CI builds have a version that
Dependabot maintains. Nothing imports it, and it is not part of the main
module's build.

A tool is added by importing its command package in `tools.go` (under the
`tools` build tag) and running `go get`. CI then builds it from here:

```bash
(cd .github/tools && GOWORK=off go build -o "$RUNNER_TEMP/bin/nfpm" github.com/goreleaser/nfpm/v2/cmd/nfpm)
```

Because the pin lives in this module's `go.mod`/`go.sum`, the version is
verified on download and arrives as an ordinary Dependabot PR. `.github/tools`
is listed in `.github/dependabot.yml` for that reason.
