# Create merge/upstream-vX.Y.Z from main (runbook step 4); optionally merge upstream (step 5) and push.
# Requires: git, upstream/vX.Y.Z branch (create first with task upgrade:create-upstream-branch).
# Run from repo root. Output: table with tag, merge_branch, merged, pushed.
# Usage: nu create-merge-branch-from-tag.nu vX.Y.Z [--merge] [--push]

use lib/git/upstream.nu *

def main [
  tag: string
  --merge (-m) = false
  --push (-p) = false
] {
  let result = (create-merge-branch-from-tag $tag --merge $merge --push $push)
  [ $result ] | print
}
