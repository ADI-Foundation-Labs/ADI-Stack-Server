# Read / initialize merge status state file.
#
# Usage:
#   nu .ops/scripts/merge-status.nu                 # show
#   nu .ops/scripts/merge-status.nu --init          # create if missing
#   nu .ops/scripts/merge-status.nu --init --force  # overwrite with defaults

use lib/git/upstream.nu *

def main [
  --init (-i)
  --force (-f)
] {
  let path = (merge-status-path)

  if $init {
    if (($path | path exists) and not $force) {
      {
        initialized: false
        reason: "exists"
        status_file: $path
      } | print
      return
    }

    let p = (write-merge-status (default-merge-status))
    {
      initialized: true
      status_file: $p
    } | print
    return
  }

  let status = (read-merge-status)
  # Top-level summary (so nested records are not collapsed as "record n fields")
  $status | reject tags merge radar | print
  print ""
  print "tags"
  print "----"
  $status.tags | print
  print ""
  print "merge"
  print "-----"
  $status.merge | reject conflict_files | print
  if ($status.merge?.has_conflicts) {
    print ""
    print "merge.conflict_files"
    print "--------------------"
    $status.merge.conflict_files | print
  }
  print ""
  print "radar"
  print "-----"
  $status.radar | print
}
