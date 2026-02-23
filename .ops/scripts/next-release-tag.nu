# Compute next release tag in the format vX.Y.Z-bN.
#
# Usage:
#   nu .ops/scripts/next-release-tag.nu v0.14.2
#   nu .ops/scripts/next-release-tag.nu   # uses merge-status current tag

use lib/git/upstream.nu *

def main [upstream_tag?: string] {
  let status = (read-merge-status)
  let from_status = ($status | get --optional tags.current_upstream_tag)
  let base_tag = if $upstream_tag == null {
    if $from_status == null {
      error make { msg: "upstream_tag is required (or set tags.current_upstream_tag in .ops/merge-status.yaml)" }
    }
    $from_status
  } else {
    $upstream_tag
  }

  let next = (next-b-release-tag $base_tag)
  {
    upstream_tag: $base_tag
    next_release_tag: $next
    exists_already: (git-tag-exists $next)
  } | print
}
