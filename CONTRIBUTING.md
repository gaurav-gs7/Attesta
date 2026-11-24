# Contributing

Issues and pull requests are welcome. This file says how the repository is put together and what a change needs before it can merge.

## Setup

- Go: the version in `go.mod` (CI builds with the newest 1.x release).
- For the full suite: PostgreSQL 16, `kustomize`, `kubeconform`, `shellcheck`, Python 3 with `pip`.
- For the end-to-end test: Docker and `kind`.
- To change the API types: `controller-gen` v0.23.0.

```bash
make build
make test                                   # go test -race ./...
export ATTESTA_TEST_DATABASE_URL='postgres://user:pass@localhost:5432/postgres?sslmode=disable'
go test ./...                               # with the PostgreSQL tests (each gets its own database)
ATTESTA_REQUIRE_KUSTOMIZE=1 go test -run '^TestRendered' ./internal/templates
make lint                                   # golangci-lint and shellcheck
make e2e                                    # controller end-to-end on kind
```

Without `ATTESTA_TEST_DATABASE_URL`, the PostgreSQL tests skip and the in-memory store is tested alone. CI runs both.

## Layout

| Path | What lives there |
| --- | --- |
| `cmd/<binary>` | `main` packages and each binary's Dockerfile |
| `api/v1` | CRD types. `deploy/crds` and `zz_generated.deepcopy.go` are generated from them with `make generate`; never edit those by hand |
| `internal/` | everything the binaries share |
| `internal/catalog/migrations` | numbered SQL migrations. Never edit one that has been released: add the next number |
| `templates/` | the generated service scaffold |
| `docs/` | operator and design documentation |

## What a change needs

- **One change per commit.** A commit says what changed and why in its message: the problem first, then the fix, then how it is tested. Formatting and generated-file fixes go in their own commits.
- **A test that fails without the change.** Bug fixes come with a regression test. Behaviour visible to a client (HTTP responses, CLI requests, generated files) is pinned by a test of that behaviour, not only of the function behind it.
- **CI green.** `gofmt`, `go mod tidy`, vet, race tests, lint, ShellCheck, govulncheck, generated-code drift, manifest validation, image scans and the kind test all run on every push. The coverage floor in `.github/workflows/ci.yml` only goes up.
- **Documentation in the same change.** A new environment variable, endpoint, CRD field or behaviour goes into the README or `docs/` with it, and a user-visible change into `CHANGELOG.md` under Unreleased.
- **Compatibility stated.** A change to the API, the CRDs or the database schema says in `CHANGELOG.md` what an existing deployment must do.

## Pinning

Every GitHub Action is pinned to a full commit SHA with the version in a comment, and so are the actions in the generated CI template. Update the SHA and the comment together. Dependabot proposes these updates weekly.

## Releases

Releases are tags `vX.Y.Z` on `main`. Before tagging, move the Unreleased section of `CHANGELOG.md` under the new version. Pushing the tag publishes multi-arch images to GHCR and creates the GitHub release with that section as its notes and checksummed binaries.

## Security

Report vulnerabilities privately; see [SECURITY.md](SECURITY.md).
