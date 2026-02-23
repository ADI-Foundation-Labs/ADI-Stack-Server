# Create upstream/vX.Y.Z branch from upstream tag and optionally push to origin.
# Requires: git, upstream remote. Run from repo root (e.g. task upgrade:create-upstream-branch -- vX.Y.Z).
# Output: table with tag, branch, pushed.

use lib/git/upstream.nu *

def main [tag: string, --push (-p), --force (-f)] {
  let result = if $push and $force {
    create-upstream-branch-from-tag $tag --push --force
  } else if $push {
    create-upstream-branch-from-tag $tag --push
  } else if $force {
    create-upstream-branch-from-tag $tag --force
  } else {
    create-upstream-branch-from-tag $tag
  }
  [ $result ] | print
}
