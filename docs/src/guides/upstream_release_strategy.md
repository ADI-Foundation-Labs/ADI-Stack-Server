# Upstream Release Strategy (ADI + Matter Labs)

This is the canonical strategy for syncing upstream releases from
`matter-labs/zksync-os-server` into `ADI-Foundation-Labs/ADI-Stack-Server`.

## Final Decisions

1. `main` stays the ADI branch with the latest ADI changes.
2. We do not maintain a mirrored upstream `main` in this repository.
3. We maintain upstream release branches only, named `upstream/vX.Y.Z`.
4. For every upstream release `vX.Y.Z`, ADI publishes the exact same tag name `vX.Y.Z`.
5. Upstream syncs happen via PRs, never direct pushes to `main`.

## Historical Audit (Current Repository)

Observed from local git history:

1. The repo currently has one upstream release branch: `origin/upstream/v0.12.1`.
2. Historical release tags are mixed:
   - Existing exact tag example: `v0.8.4`
   - Legacy ADI suffix tags: `v0.10.0-b`, `v0.10.1-b*`, `v0.12.1-b`, `v0.13.0-b`
3. Several legacy version tags are not ancestors of current `main`.
   - This indicates prior releases were sometimes cut from side branches.
4. `origin/upstream/v0.12.1` is not an ancestor of current `main`.
   - This confirms historical non-linear upgrade flow.
5. There is no persistent local `upstream` remote configured by default in this clone.

Implication: we need stricter release ancestry rules and explicit upstream sync branches.

## Consolidated Strategy From Discussion Notes

What all notes agreed on:

1. Current pain comes from ad-hoc merges into a branch with custom changes.
2. Upstream sync must be standardized and automated.
3. `git rerere` should be enabled to reuse conflict resolutions.
4. AI should assist conflict handling, but protected branches need human approval.

What differed:

1. Rebase-first model vs merge-forward model.
2. Upstream mirror branch model vs ADI-on-main model.

Chosen model for this repo:

1. Merge-forward into `main` (no history rewrite).
2. Upstream release branches (`upstream/vX.Y.Z`) as the merge source.
3. Exact upstream tag parity after merge.

Reason: this matches the repo's operating reality and avoids rebasing long-lived ADI history.

## Branch and Tag Conventions

Branches:

1. `main`:
   - ADI production branch.
   - Must contain only reviewed merges.
2. `upstream/vX.Y.Z`:
   - Branch tip created from upstream tag `vX.Y.Z`.
   - Never used for feature work.
3. `merge/upstream-vX.Y.Z`:
   - Temporary integration branch from `main`.
   - Used to resolve conflicts and run CI before PR.

Tags:

1. Release tag format: `vX.Y.Z` (exactly upstream).
2. Tag location: merge commit on `main` that contains the upstream sync + ADI changes.
3. Legacy `-b` tags remain historical and are not reused.

## Standard Runbook (Per Upstream Release)

Assume upstream released `vX.Y.Z`.

0. Define project/worktree paths:

```bash
PROJECT_NAME="ADI-Stack-Server"
TAG="vX.Y.Z"
WT_ROOT="${HOME}/.local/git/wortrees/${PROJECT_NAME}"
WT_PATH="${WT_ROOT}/merge-upstream-${TAG}"
```

1. Prepare remotes and fetch:

```bash
git remote add upstream https://github.com/matter-labs/zksync-os-server.git 2>/dev/null || true
git fetch origin --tags
```

2. Guard: ensure release tag does not already exist in ADI repo:

```bash
git ls-remote --exit-code --tags origin "refs/tags/${TAG}" >/dev/null && {
  echo "Tag ${TAG} already exists in origin; stop."
  exit 1
}
```

3. Refresh upstream release branch:

```bash
git fetch --force upstream "refs/tags/${TAG}:refs/heads/upstream/${TAG}"
# optional: publish/update tracking branch in origin
git push -f origin "upstream/${TAG}"
```

This keeps a deterministic in-repo branch for each upstream release tag.

4. Create integration branch from ADI `main` in a dedicated worktree:

```bash
git checkout main
git pull --ff-only origin main
mkdir -p "${WT_ROOT}"
git worktree add -B "merge/upstream-${TAG}" "${WT_PATH}" origin/main
cd "${WT_PATH}"
```

5. Merge upstream release branch:

```bash
git merge --no-ff "upstream/${TAG}"
```

6. Resolve conflicts:

1. Enable `rerere` once:
   `git config --global rerere.enabled true`
2. Resolve low-risk conflicts first (lockfiles, metadata, docs).
3. Resolve core runtime/API logic with human review.
4. Use AI in suggest-only mode first; patch mode is optional.

7. Validate:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features --workspace -- -D warnings
cargo nextest run --workspace
```

8. Open PR from worktree branch:

1. Source: `merge/upstream-vX.Y.Z`
2. Target: `main`
3. Include:
   - upstream tag merged
   - conflict files list
   - key manual decisions
   - test results

9. After PR merge, create release tag and release from `main`:

```bash
cd /path/to/your/main/clone
git checkout main
git pull --ff-only origin main
git tag "vX.Y.Z"
git push origin "vX.Y.Z"
```

10. Cleanup local worktree:

```bash
git worktree remove "${WT_PATH}"
```

11. Publish GitHub Release notes:

1. Base upstream: `matter-labs/zksync-os-server@vX.Y.Z`
2. ADI delta summary (what changed on top)
3. CI/test status

## Review and Safety Checks

Use this minimum checklist for every upstream update:

1. `upstream/vX.Y.Z` branch tip matches upstream tag.
2. Merge PR to `main` is reviewed by at least one maintainer.
3. No direct push to `main`.
4. Tag `vX.Y.Z` is created only after merge to `main`.
5. Release notes include upstream base and ADI delta.

## Automation Plan (Recommended)

1. Scheduled workflow (daily or weekly):
   - checks latest upstream release tag
   - compares to latest ADI release tag
2. If new upstream tag exists:
   - creates/updates `merge/upstream-vX.Y.Z`
   - opens PR to `main`
   - posts conflict/test summary
3. Use token strategy that allows downstream workflows to run:
   - GitHub App or PAT for PR-creating bot workflows
4. Human approval remains mandatory for merge and release publishing.

## Handling Legacy History

Legacy `-b` tags and side branches are kept for traceability, but future releases follow:

1. exact `vX.Y.Z` tag naming
2. tags cut from `main` only
3. upstream release branch + PR integration flow only
