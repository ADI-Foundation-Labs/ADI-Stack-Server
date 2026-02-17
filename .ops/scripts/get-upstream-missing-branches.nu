# List upstream release tags that do not have a corresponding origin/upstream/vX.Y.Z branch.
# Requires: git, upstream remote, origin remote. Run from repo root (e.g. task upgrade:upstream-missing-branches).
# Output: table with upstream_tag and branch_expected (upstream/vX.Y.Z).

use lib/git/upstream.nu *

let upstream = (upstream-tags)
let branches = (origin-upstream-branches)

let missing = (
  $upstream
  | each { |tag|
    let exists = ($branches | any { |b| $b == $tag })
    if not $exists {
      { upstream_tag: $tag, branch_expected: $"upstream/($tag)" }
    } else { null }
  }
  | where { |x| $x != null }
)

if ($missing | length) == 0 {
  print "No missing branches: every upstream release tag has origin/upstream/<tag>."
} else {
  $missing | print
}
