# ADI Docker PR Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build multi-arch Docker images on PR creation for `feat/` and `bug/` branches, push to ghcr.io, and clean up on PR close.

**Architecture:** Docker Bake for declarative build config. Two new GitHub Actions workflows (`adi-docker-pr.yml` for build, `adi-docker-pr-cleanup.yml` for cleanup). Matrix strategy with native runners per platform (amd64 + arm64), push-by-digest, merge into multi-arch manifest. All 10 upstream workflows guarded with repo check.

**Tech Stack:** Docker Buildx/Bake, GitHub Actions, ghcr.io, `docker/bake-action@v7`, `dataaxiom/ghcr-cleanup-action@v1`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `docker-bake.hcl` | Create | Declarative build config with IMAGE, VERSION, SUFFIX, PLATFORMS, CACHE_REF variables |
| `.github/workflows/adi-docker-pr.yml` | Create | Build + push multi-arch image on PR open/synchronize/reopen for feat/ and bug/ branches |
| `.github/workflows/adi-docker-pr-cleanup.yml` | Create | Delete image + orphaned manifests on PR close |
| `.github/workflows/ai-triage-ci.yaml` | Modify | Add repo guard to `triage` job (compose with existing `if`) |
| `.github/workflows/cargo-audit.yml` | Modify | Add repo guard to `cargo-audit`, `cargo-deny` jobs |
| `.github/workflows/check-pr-metadata.yml` | Modify | Add repo guard to `lint-pr-title`, `check-breaking-metadata` jobs |
| `.github/workflows/ci.yml` | Modify | Add repo guard to `build`, `format-and-lint`, `test`, `check-wire-version`, `build-prover-tests`, `prover-tests`, `config-tests`, `ci-success` jobs |
| `.github/workflows/deploy-docs.yml` | Modify | Add repo guard to `deploy-docs` job |
| `.github/workflows/docker.yml` | Modify | Add repo guard to `build-images` job |
| `.github/workflows/release-bins.yml` | Modify | Add repo guard to `build-bins`, `test-bins`, `release-bins` jobs |
| `.github/workflows/release-please.yml` | Modify | Add repo guard to `release-please`, `release-bins` jobs (compose with existing `if` on `release-bins`) |
| `.github/workflows/secrets_scanner.yaml` | Modify | Add repo guard to `TruffleHog` job |
| `.github/workflows/spec-tests.yaml` | Modify | Add repo guard to `geth-tests`, `zk-os-tests`, `compatibility-table` jobs |

---

### Task 1: Create feature branch

- [ ] **Step 1: Create and switch to the feature branch**

```bash
git checkout -b feat/docker-pr-pipeline
```

- [ ] **Step 2: Verify branch**

```bash
git branch --show-current
```

Expected: `feat/docker-pr-pipeline`

---

### Task 2: Create `docker-bake.hcl`

**Files:**
- Create: `docker-bake.hcl`

- [ ] **Step 1: Create `docker-bake.hcl`**

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

- [ ] **Step 2: Validate bake file parses**

```bash
docker buildx bake --print zksync-os-server
```

Expected: JSON output showing the target with `dev-local` tag, `linux/amd64` platform, no cache refs.

- [ ] **Step 3: Validate with variable overrides**

```bash
VERSION=0.17.1 SUFFIX=test PLATFORMS="linux/amd64,linux/arm64" docker buildx bake --print zksync-os-server
```

Expected: JSON output showing tag `ghcr.io/adi-foundation-labs/adi-stack-server:0.17.1-test` and both platforms listed.

- [ ] **Step 4: Commit**

```bash
git add docker-bake.hcl
git commit -m "feat(ci): add docker-bake.hcl for declarative Docker builds"
```

---

### Task 3: Create `.github/workflows/adi-docker-pr.yml`

**Files:**
- Create: `.github/workflows/adi-docker-pr.yml`

- [ ] **Step 1: Create the build workflow**

```yaml
name: ADI Docker PR build

on:
  pull_request:
    types: [opened, synchronize, reopened]
    branches: [main]

concurrency:
  group: adi-docker-pr-${{ github.event.pull_request.number }}
  cancel-in-progress: true

permissions:
  contents: read
  packages: write
  pull-requests: write

jobs:

  compute-meta:
    if: startsWith(github.head_ref, 'feat/') || startsWith(github.head_ref, 'bug/')
    runs-on: ubuntu-latest
    outputs:
      version: ${{ steps.v.outputs.version }}
      suffix: ${{ steps.v.outputs.suffix }}
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Compute version and suffix
        id: v
        run: |
          VERSION=$(awk -F\" '/^\[workspace.package\]/{f=1} f && /^version/{print $2; exit}' Cargo.toml)
          SANITIZED=$(echo "${{ github.head_ref }}" | sed 's|/|-|g; s|[^a-zA-Z0-9._-]|-|g')
          SUFFIX="pr-${{ github.event.pull_request.number }}-${SANITIZED}"
          echo "version=${VERSION}" >> "${GITHUB_OUTPUT}"
          echo "suffix=${SUFFIX}" >> "${GITHUB_OUTPUT}"

  build:
    needs: compute-meta
    strategy:
      fail-fast: false
      matrix:
        include:
          - platform: linux/amd64
            runner: ubuntu-latest
            arch: amd64
          - platform: linux/arm64
            runner: ubuntu-24.04-arm
            arch: arm64
    runs-on: ${{ matrix.runner }}
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Login to GitHub Container Registry
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.repository_owner }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Build and push by digest
        id: bake
        uses: docker/bake-action@v7
        env:
          VERSION: ${{ needs.compute-meta.outputs.version }}
          SUFFIX: ${{ needs.compute-meta.outputs.suffix }}
          PLATFORMS: ${{ matrix.platform }}
          CACHE_REF: "ghcr.io/${{ github.repository_owner }}/adi-stack-server:buildcache-${{ matrix.arch }}"
        with:
          targets: zksync-os-server
          set: |
            *.output=type=image,"name=ghcr.io/${{ github.repository_owner }}/adi-stack-server",push-by-digest=true,name-canonical=true,push=true

      - name: Export digest
        run: |
          mkdir -p /tmp/digests
          DIGEST=$(jq -r '."zksync-os-server"."containerimage.digest"' <<< '${{ steps.bake.outputs.metadata }}')
          echo "${DIGEST}" > /tmp/digests/${{ matrix.arch }}.txt

      - name: Upload digest
        uses: actions/upload-artifact@v4
        with:
          name: digests-${{ matrix.arch }}
          path: /tmp/digests/*
          retention-days: 1

  merge:
    needs: [compute-meta, build]
    runs-on: ubuntu-latest
    steps:
      - name: Download digests
        uses: actions/download-artifact@v4
        with:
          path: /tmp/digests
          pattern: digests-*
          merge-multiple: true

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Login to GitHub Container Registry
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.repository_owner }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Create multi-arch manifest
        env:
          IMAGE: "ghcr.io/${{ github.repository_owner }}/adi-stack-server"
          TAG: "${{ needs.compute-meta.outputs.version }}-${{ needs.compute-meta.outputs.suffix }}"
        run: |
          AMD64=$(cat /tmp/digests/amd64.txt)
          ARM64=$(cat /tmp/digests/arm64.txt)
          docker buildx imagetools create -t "${IMAGE}:${TAG}" \
            "${IMAGE}@${AMD64}" "${IMAGE}@${ARM64}"

      - name: Comment on PR
        uses: marocchino/sticky-pull-request-comment@v2
        with:
          header: adi-docker-image
          message: |
            **Docker image built** &#x1f4e6;
            ```
            ghcr.io/${{ github.repository_owner }}/adi-stack-server:${{ needs.compute-meta.outputs.version }}-${{ needs.compute-meta.outputs.suffix }}
            ```
            Platforms: `linux/amd64`, `linux/arm64`
```

- [ ] **Step 2: Validate YAML syntax**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/adi-docker-pr.yml'))" && echo "YAML OK"
```

Expected: `YAML OK`

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/adi-docker-pr.yml
git commit -m "feat(ci): add ADI Docker PR build workflow

Multi-arch (amd64 + arm64) Docker image build for feat/ and bug/ branches.
Uses docker bake with native runners per platform, push-by-digest, and
multi-arch manifest merge. Posts image coordinates as sticky PR comment."
```

---

### Task 4: Create `.github/workflows/adi-docker-pr-cleanup.yml`

**Files:**
- Create: `.github/workflows/adi-docker-pr-cleanup.yml`

- [ ] **Step 1: Create the cleanup workflow**

```yaml
name: ADI Docker PR cleanup

on:
  pull_request:
    types: [closed]
    branches: [main]

permissions:
  contents: read
  packages: write

jobs:

  cleanup:
    if: startsWith(github.head_ref, 'feat/') || startsWith(github.head_ref, 'bug/')
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Compute tag to delete
        id: tag
        run: |
          VERSION=$(awk -F\" '/^\[workspace.package\]/{f=1} f && /^version/{print $2; exit}' Cargo.toml)
          SANITIZED=$(echo "${{ github.head_ref }}" | sed 's|/|-|g; s|[^a-zA-Z0-9._-]|-|g')
          TAG="${VERSION}-pr-${{ github.event.pull_request.number }}-${SANITIZED}"
          echo "tag=${TAG}" >> "${GITHUB_OUTPUT}"

      - name: Delete image by tag
        uses: dataaxiom/ghcr-cleanup-action@v1
        with:
          package: adi-stack-server
          delete-tags: ${{ steps.tag.outputs.tag }}
          delete-untagged: true
          token: ${{ secrets.GITHUB_TOKEN }}
```

- [ ] **Step 2: Validate YAML syntax**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/adi-docker-pr-cleanup.yml'))" && echo "YAML OK"
```

Expected: `YAML OK`

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/adi-docker-pr-cleanup.yml
git commit -m "feat(ci): add ADI Docker PR cleanup workflow

Deletes the PR preview Docker image and orphaned per-platform manifests
when PR is closed (merged or unmerged)."
```

---

### Task 5: Guard upstream workflow — `ai-triage-ci.yaml`

**Files:**
- Modify: `.github/workflows/ai-triage-ci.yaml`

The `triage` job has an existing `if: github.event.workflow_run.conclusion == 'failure'`. Compose with `&&`.

- [ ] **Step 1: Add repo guard to `triage` job**

Change:
```yaml
    if: github.event.workflow_run.conclusion == 'failure'
```

To:
```yaml
    if: github.repository == 'matter-labs/zksync-os-server' && github.event.workflow_run.conclusion == 'failure'
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ai-triage-ci.yaml
git commit -m "ci: guard ai-triage-ci.yaml for upstream repo only"
```

---

### Task 6: Guard upstream workflow — `cargo-audit.yml`

**Files:**
- Modify: `.github/workflows/cargo-audit.yml`

Jobs: `cargo-audit`, `cargo-deny` — neither has an existing `if`.

- [ ] **Step 1: Add repo guard to both jobs**

Add after each job name line:
```yaml
    if: github.repository == 'matter-labs/zksync-os-server'
```

Add to `cargo-audit` (after `cargo-audit:` line, before `runs-on:`):
```yaml
    if: github.repository == 'matter-labs/zksync-os-server'
```

Add to `cargo-deny` (after `cargo-deny:` line, before `runs-on:`):
```yaml
    if: github.repository == 'matter-labs/zksync-os-server'
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/cargo-audit.yml
git commit -m "ci: guard cargo-audit.yml for upstream repo only"
```

---

### Task 7: Guard upstream workflow — `check-pr-metadata.yml`

**Files:**
- Modify: `.github/workflows/check-pr-metadata.yml`

Jobs: `lint-pr-title`, `check-breaking-metadata` — neither has an existing `if`.

- [ ] **Step 1: Add repo guard to both jobs**

Add `if: github.repository == 'matter-labs/zksync-os-server'` after each job name line, before `runs-on:`.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/check-pr-metadata.yml
git commit -m "ci: guard check-pr-metadata.yml for upstream repo only"
```

---

### Task 8: Guard upstream workflow — `ci.yml`

**Files:**
- Modify: `.github/workflows/ci.yml`

Jobs: `build`, `format-and-lint`, `test`, `check-wire-version`, `build-prover-tests`, `prover-tests`, `config-tests`, `ci-success`.

`ci-success` has an existing `if: always() && !cancelled()`. Compose with `&&`.

- [ ] **Step 1: Add repo guard to all jobs without existing `if`**

Add `if: github.repository == 'matter-labs/zksync-os-server'` to jobs: `build`, `format-and-lint`, `test`, `check-wire-version`, `build-prover-tests`, `prover-tests`, `config-tests`.

- [ ] **Step 2: Compose repo guard with `ci-success` existing condition**

Change:
```yaml
    if: always() && !cancelled()
```

To:
```yaml
    if: github.repository == 'matter-labs/zksync-os-server' && always() && !cancelled()
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: guard ci.yml for upstream repo only"
```

---

### Task 9: Guard upstream workflow — `deploy-docs.yml`

**Files:**
- Modify: `.github/workflows/deploy-docs.yml`

Jobs: `deploy-docs` — no existing `if`.

- [ ] **Step 1: Add repo guard**

Add `if: github.repository == 'matter-labs/zksync-os-server'` after `deploy-docs:` line, before `runs-on:`.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/deploy-docs.yml
git commit -m "ci: guard deploy-docs.yml for upstream repo only"
```

---

### Task 10: Guard upstream workflow — `docker.yml`

**Files:**
- Modify: `.github/workflows/docker.yml`

Jobs: `build-images` — no existing `if`.

- [ ] **Step 1: Add repo guard**

Add `if: github.repository == 'matter-labs/zksync-os-server'` after `build-images:` line (line 26 area), before `name:`.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/docker.yml
git commit -m "ci: guard docker.yml for upstream repo only"
```

---

### Task 11: Guard upstream workflow — `release-bins.yml`

**Files:**
- Modify: `.github/workflows/release-bins.yml`

Jobs: `build-bins`, `test-bins`, `release-bins` — none have existing `if`.

- [ ] **Step 1: Add repo guard to all three jobs**

Add `if: github.repository == 'matter-labs/zksync-os-server'` after each job name line, before `runs-on:` or `strategy:`.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/release-bins.yml
git commit -m "ci: guard release-bins.yml for upstream repo only"
```

---

### Task 12: Guard upstream workflow — `release-please.yml`

**Files:**
- Modify: `.github/workflows/release-please.yml`

Jobs: `release-please` (no existing `if`, uses reusable workflow), `release-bins` (has existing `if: ${{ always() && needs.release-please.outputs.releases_created == 'true' }}`).

- [ ] **Step 1: Add repo guard to `release-please` job**

Add after `release-please:` line, before `uses:`:
```yaml
    if: github.repository == 'matter-labs/zksync-os-server'
```

- [ ] **Step 2: Compose repo guard with `release-bins` existing condition**

Change:
```yaml
    if: ${{ always() && needs.release-please.outputs.releases_created == 'true' }}
```

To:
```yaml
    if: ${{ github.repository == 'matter-labs/zksync-os-server' && always() && needs.release-please.outputs.releases_created == 'true' }}
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release-please.yml
git commit -m "ci: guard release-please.yml for upstream repo only"
```

---

### Task 13: Guard upstream workflow — `secrets_scanner.yaml`

**Files:**
- Modify: `.github/workflows/secrets_scanner.yaml`

Jobs: `TruffleHog` — no existing `if`.

- [ ] **Step 1: Add repo guard**

Add `if: github.repository == 'matter-labs/zksync-os-server'` after `TruffleHog:` line, before `runs-on:`.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/secrets_scanner.yaml
git commit -m "ci: guard secrets_scanner.yaml for upstream repo only"
```

---

### Task 14: Guard upstream workflow — `spec-tests.yaml`

**Files:**
- Modify: `.github/workflows/spec-tests.yaml`

Jobs: `geth-tests`, `zk-os-tests`, `compatibility-table` — none have existing `if`.

- [ ] **Step 1: Add repo guard to all three jobs**

Add `if: github.repository == 'matter-labs/zksync-os-server'` after each job name line, before `runs-on:` or `env:`.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/spec-tests.yaml
git commit -m "ci: guard spec-tests.yaml for upstream repo only"
```

---

### Task 15: Push branch and create PR

- [ ] **Step 1: Push the feature branch**

```bash
git push -u origin feat/docker-pr-pipeline
```

- [ ] **Step 2: Create the PR**

```bash
gh pr create --title "feat(ci): add ADI Docker PR build pipeline" --body "$(cat <<'EOF'
## Summary

- Add `docker-bake.hcl` for declarative Docker build config (local + CI parity)
- Add `adi-docker-pr.yml` workflow: builds multi-arch (amd64 + arm64) Docker images for PRs from `feat/` and `bug/` branches, pushes to ghcr.io, posts image coordinates as sticky PR comment
- Add `adi-docker-pr-cleanup.yml` workflow: deletes PR preview images on PR close
- Guard all 10 upstream workflow files with `if: github.repository == 'matter-labs/zksync-os-server'` so they skip on the ADI fork

## Tag format

\`\`\`
ghcr.io/adi-foundation-labs/adi-stack-server:<version>-pr-<number>-<sanitized-branch>
\`\`\`

Example: `ghcr.io/adi-foundation-labs/adi-stack-server:0.17.1-pr-42-feat-docker-pr-pipeline`

## Test plan

- [ ] PR itself triggers `adi-docker-pr.yml` (branch is `feat/docker-pr-pipeline`)
- [ ] `compute-meta` job outputs correct version and suffix
- [ ] `build` matrix runs amd64 on `ubuntu-latest` and arm64 on `ubuntu-24.04-arm`
- [ ] `merge` job creates multi-arch manifest with correct tag
- [ ] Sticky PR comment appears with image coordinates
- [ ] `docker buildx bake --print zksync-os-server` works locally
- [ ] All upstream workflows show as skipped (not failed) on the PR
- [ ] On PR close, cleanup workflow deletes the image from ghcr.io
EOF
)"
```

- [ ] **Step 3: Verify pipeline runs**

Check the Actions tab on the PR. The `ADI Docker PR build` workflow should trigger automatically since the branch matches `feat/`.
