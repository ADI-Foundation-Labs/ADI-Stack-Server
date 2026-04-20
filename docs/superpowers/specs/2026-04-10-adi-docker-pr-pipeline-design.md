# ADI Docker PR Pipeline Design

## Overview

Add GitHub Actions workflows to build multi-arch Docker images for PRs from `feat/` and `bug/` branches, push them to GitHub Container Registry (ghcr.io), and clean them up on PR close. Uses Docker Bake for declarative build config with local/CI parity.

## Context

This repo is a fork of `matter-labs/zksync-os-server`. Upstream workflows reference matter-labs-specific infrastructure (self-hosted runners, GCS artifact registry, sccache service accounts) and do not function on the ADI fork. All upstream workflows will be guarded with a repository check so they only run on the upstream repo, keeping merge conflicts minimal.

All new ADI-specific workflows use the `adi-` prefix to distinguish them from upstream files.

## Deliverables

| File | Action |
|------|--------|
| `docker-bake.hcl` | New |
| `.github/workflows/adi-docker-pr.yml` | New |
| `.github/workflows/adi-docker-pr-cleanup.yml` | New |
| 10 upstream workflow files | Add repo guard to every job |

Development branch: `feat/docker-pr-pipeline`

## docker-bake.hcl

Declarative build configuration with variables that CI sets and developers can override locally.

### Variables

| Variable | Default | CI sets to | Purpose |
|----------|---------|------------|---------|
| `IMAGE` | `ghcr.io/adi-foundation-labs/adi-stack-server` | (same) | Target image name |
| `VERSION` | `dev` | Workspace version from `Cargo.toml` | Semver version prefix |
| `SUFFIX` | `local` | `pr-<number>-<sanitized-branch>` | Tag suffix after version |
| `PLATFORMS` | `linux/amd64` | Per-matrix platform | Build target platform |
| `CACHE_REF` | `""` (disabled) | ghcr buildcache ref per arch | BuildKit layer cache |

### Target

Single target `zksync-os-server`:

```hcl
variable "IMAGE" {
  default = "ghcr.io/adi-foundation-labs/adi-stack-server"
}

variable "VERSION" {
  default = "dev"
}

variable "SUFFIX" {
  default = "local"
}

variable "PLATFORMS" {
  default = "linux/amd64"
}

variable "CACHE_REF" {
  default = ""
}

target "zksync-os-server" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = split(",", PLATFORMS)
  tags       = ["${IMAGE}:${VERSION}-${SUFFIX}"]
  cache-from = CACHE_REF == "" ? [] : ["type=registry,ref=${CACHE_REF}"]
  cache-to   = CACHE_REF == "" ? [] : ["type=registry,ref=${CACHE_REF},mode=max"]
  labels = {
    "org.opencontainers.image.source"  = "https://github.com/ADI-Foundation-Labs/ADI-Stack-Server"
    "org.opencontainers.image.version" = VERSION
  }
}
```

### Local usage

```bash
# Default: builds linux/amd64, tagged as dev-local
docker buildx bake zksync-os-server

# With overrides
VERSION=0.17.1 SUFFIX=mytest docker buildx bake zksync-os-server
```

## adi-docker-pr.yml (build workflow)

### Trigger

- `pull_request` events: `opened`, `synchronize`, `reopened`
- Target branch: `main`
- Branch filter: only `feat/` and `bug/` head branches (checked via `startsWith(github.head_ref, ...)` on the first job)

### Concurrency

```yaml
concurrency:
  group: adi-docker-pr-${{ github.event.pull_request.number }}
  cancel-in-progress: true
```

New pushes to the same PR cancel in-flight builds.

### Job: compute-meta

Runs on `ubuntu-latest`. Extracts version from `Cargo.toml` (`workspace.package.version`) and computes the sanitized suffix.

Outputs:
- `version`: e.g., `0.17.1`
- `suffix`: e.g., `pr-123-feat-add-cool-thing`

Branch sanitization: replace `/` with `-`, strip characters outside `[a-zA-Z0-9._-]`.

### Job: build (matrix)

Parallel matrix across native runners:

| Platform | Runner | Arch label |
|----------|--------|------------|
| `linux/amd64` | `ubuntu-latest` | `amd64` |
| `linux/arm64` | `ubuntu-24.04-arm` | `arm64` |

Steps:
1. Checkout
2. Setup Buildx
3. Login to ghcr.io
4. `docker/bake-action@v7` with `push-by-digest=true` — builds natively on each platform (no QEMU emulation), pushes untagged blobs
5. Export digest to file, upload as artifact (`digests-amd64` / `digests-arm64`)

Per-platform cache refs: `:buildcache-amd64` and `:buildcache-arm64` in ghcr.

### Job: merge

Depends on `compute-meta` and `build`. Runs on `ubuntu-latest`.

Steps:
1. Download all digest artifacts
2. Login to ghcr.io
3. `docker buildx imagetools create -t <IMAGE>:<VERSION>-<SUFFIX> <IMAGE>@<digest1> <IMAGE>@<digest2>`
4. Post a sticky PR comment with the image coordinates and platforms

### Tag format

```
ghcr.io/adi-foundation-labs/adi-stack-server:0.17.1-pr-123-feat-add-cool-thing
```

Pattern: `<version>-pr-<pr-number>-<sanitized-branch-name>`

Overwritten on each push to the PR (same tag, updated manifest).

## adi-docker-pr-cleanup.yml (cleanup workflow)

### Trigger

- `pull_request` event: `closed` (covers both merged and unmerged)
- Target branch: `main`
- Same branch filter: only `feat/` and `bug/`

### Job: cleanup

Runs on `ubuntu-latest`.

Steps:
1. Checkout (to read version from `Cargo.toml`)
2. Compute the tag to delete (same logic as build workflow)
3. `dataaxiom/ghcr-cleanup-action@v1` with:
   - `package: adi-stack-server`
   - `delete-tags: <computed-tag>`
   - `delete-untagged: true` (cleans orphaned per-platform manifests from push-by-digest)

## Upstream workflow guards

Add the following `if` condition to every job in all 10 upstream workflow files:

```yaml
if: github.repository == 'matter-labs/zksync-os-server'
```

Files to guard:
- `ai-triage-ci.yaml`
- `cargo-audit.yml`
- `check-pr-metadata.yml`
- `ci.yml`
- `deploy-docs.yml`
- `docker.yml`
- `release-bins.yml`
- `release-please.yml`
- `secrets_scanner.yaml`
- `spec-tests.yaml`

This approach avoids merge conflicts when syncing from upstream.

## Multi-arch strategy

Docker images are `linux/amd64` + `linux/arm64`. This covers:

- Linux servers (both architectures, including AWS Graviton)
- Mac Intel (amd64 natively)
- Mac Apple Silicon (arm64 via Docker Desktop's Linux VM)
- Windows WSL2 (amd64 or arm64)

There is no `darwin/arm64` Docker platform. Docker on macOS always runs Linux containers.

## Platforms and runner costs

Both `ubuntu-latest` (amd64) and `ubuntu-24.04-arm` (arm64) are free GitHub-hosted runners for public repositories. This repo is public.

## Edge cases

1. **Branch renamed mid-PR**: tag includes PR number, so new builds get the updated name. Old-name tag becomes orphaned. Acceptable for preview images; periodic manual cleanup if needed.
2. **PR closed then reopened**: `reopened` triggers a fresh build. Image may briefly not exist between close-cleanup and reopen-build.
3. **Multiple PRs from same branch**: each PR has a unique number in the tag, so no collisions.
4. **Version bumped in Cargo.toml mid-PR**: tag prefix changes, old-version tag becomes orphaned. Same as case 1.
5. **Cold cache (first build)**: expect ~20-40 min on free runners for a Rust workspace. Subsequent builds use per-arch buildcache.

## Out of scope

- Release/main branch docker builds (future `adi-docker-release.yml`)
- sccache integration for Rust compilation caching
- Taskfile / developer tooling wrapper
- Periodic orphan image cleanup job
- Dockerfile modifications (assumed to work without matter-labs sccache args; verified during implementation)
