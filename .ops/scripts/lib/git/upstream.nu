# Git upstream helpers for matter-labs/zksync-os-server style repos.
# Use with: use .ops/nushell/git/upstream.nu *

def parse-semver-tag [ref: string] {
  # refs/tags/v1.2.3 or refs/tags/v1.2.3^{} -> v1.2.3 (semver only)
  let s = ($ref | str replace 'refs/tags/' '' | str replace '^{}' '')
  let parsed = ($s | parse -r '^v(?P<maj>[0-9]+)\.(?P<min>[0-9]+)\.(?P<patch>[0-9]+)$')
  if ($parsed | length) >= 1 { $s } else { null }
}

# List semver release tags (vX.Y.Z) from remote 'upstream'.
export def upstream-tags [] {
  ^git ls-remote --tags upstream
  | lines
  | each { |line|
    let parts = ($line | split row "\t")
    let ref = ($parts | get --optional 1)
    if $ref != null { parse-semver-tag $ref } else { null }
  }
  | where { |x| $x != null }
  | uniq
  | sort
}

# List branch names under refs/heads/upstream/* on remote 'origin' (e.g. v0.12.1 for upstream/v0.12.1).
export def origin-upstream-branches [] {
  ^git ls-remote origin
  | lines
  | each { |line|
    let parts = ($line | split row "\t")
    let ref = ($parts | get --optional 1)
    if $ref != null and ($ref | str starts-with 'refs/heads/upstream/') {
      $ref | str replace 'refs/heads/upstream/' ''
    } else { null }
  }
  | where { |x| $x != null }
  | uniq
}

# Check that a tag (e.g. v1.2.3) exists on remote 'upstream'.
export def upstream-has-tag [tag: string] {
  let ref = $"refs/tags/($tag)"
  (^git ls-remote --exit-code upstream $ref | complete).exit_code == 0
}

# Create local branch upstream/<tag> from upstream tag, optionally push to origin.
# Returns a record: { tag, branch, pushed }.
export def create-upstream-branch-from-tag [
  tag: string
  --push (-p) = false
] {
  let branch = $"upstream/($tag)"
  let tag_ref = $"refs/tags/($tag)"
  let branch_ref = $"refs/heads/($branch)"

  if not (upstream-has-tag $tag) {
    error make {
      msg: $"Tag ($tag) not found on upstream. Run: git fetch upstream --tags"
    }
  }

  ^git fetch --force upstream $"($tag_ref):($branch_ref)"
  mut pushed = false
  if $push {
    ^git push -f origin $branch
    $pushed = true
  }

  { tag: $tag, branch: $branch, pushed: $pushed }
}

# Check that branch upstream/<tag> exists locally (e.g. after create-upstream-branch-from-tag).
export def local-upstream-branch-exists [tag: string] {
  let ref = $"upstream/($tag)"
  (^git rev-parse --verify $ref | complete).exit_code == 0
}

# Create integration branch merge/upstream-<tag> from ADI main (runbook step 4).
# Optionally merge upstream/<tag> into it (step 5) and/or push to origin.
# Returns a record: { tag, merge_branch, merged, pushed }.
export def create-merge-branch-from-tag [
  tag: string
  --merge (-m) = false
  --push (-p) = false
] {
  let merge_branch = $"merge/upstream-($tag)"
  let upstream_branch = $"upstream/($tag)"

  if not (local-upstream-branch-exists $tag) {
    error make {
      msg: $"Local branch ($upstream_branch) not found. Create it first: task upgrade:create-upstream-branch -- ($tag)"
    }
  }

  ^git fetch origin
  ^git checkout main
  ^git pull --ff-only origin main
  ^git checkout -b $merge_branch

  mut merged = false
  if $merge {
    ^git merge --no-ff $upstream_branch
    $merged = true
  }

  mut pushed = false
  if $push {
    ^git push -u origin $merge_branch
    $pushed = true
  }

  { tag: $tag, merge_branch: $merge_branch, merged: $merged, pushed: $pushed }
}
