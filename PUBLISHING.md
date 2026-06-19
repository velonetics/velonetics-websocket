# Publishing `velonetics-websocket` as a standalone Go module

The implementation lives in `forks/velonetics-websocket/` and is consumed by CE via a `replace` directive in the root `go.mod`:

```go
github.com/pucora/velonetics-websocket/v2 => ./forks/velonetics-websocket
```

CI in the main repository tests the fork **with** local `replace` paths. The fork's own GitHub Actions workflow tests **without** `replace` blocks, matching what `go get` users resolve from GitHub tags.

## Prerequisites

1. GitHub org membership with permission to create repositories under `pucora` (or create the repo manually first).
2. `gh` CLI authenticated (`gh auth login`).
3. Published dependencies already tagged on GitHub:
   - `github.com/pucora/lura/v2`
   - `github.com/pucora/velonetics-jose/v2` (test dependency only — not required at runtime for the library)

The publish script creates `github.com/pucora/velonetics-websocket` automatically when it is missing (requires `gh` and org access).

## Publish a version

From the CE repository root:

```bash
./scripts/publish-fork-module.sh velonetics-websocket v2.0.1
```

The script will:

1. Copy `forks/velonetics-websocket/` to a temporary directory
2. Strip `replace` directives from `go.mod` and run `go mod tidy`
3. Copy `LICENSE` from the CE root
4. Push to `git@github.com:pucora/velonetics-websocket.git` on branch `main`
5. Create and push tag `v2.0.1`

## After publishing

Published module: https://github.com/pucora/velonetics-websocket

1. Update the root `go.mod` require line when bumping versions:

   ```go
   github.com/pucora/velonetics-websocket/v2 v2.0.1
   ```

2. Keep the local `replace` directive while developing the fork locally:

   ```go
   github.com/pucora/velonetics-websocket/v2 => ./forks/velonetics-websocket
   ```

   Remove it when you want local builds to use the published module (CI already does this automatically).

3. Re-run main CI (`go test`, `make check-fixtures`, `make ws-compose-test`).

## Module path and versioning

- Module: `github.com/pucora/velonetics-websocket/v2`
- Tags must use the **major version suffix** (`v2.x.y`) because the module path ends in `/v2`.

## CI in the fork

`forks/velonetics-websocket/.github/workflows/go.yml` is copied with the module when publishing. It runs `go test ./...` on every push/PR and on GitHub releases.
