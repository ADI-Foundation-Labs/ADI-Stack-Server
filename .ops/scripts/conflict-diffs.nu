# Generate per-file conflict patch artifacts between BASE (upstream ref) and TARGET (default: main).
# For each predicted conflict file, writes:
#   - <safe-name>.adi.patch      : merge-base -> target
#   - <safe-name>.upstream.patch : merge-base -> base
#
# Usage:
#   nu .ops/scripts/conflict-diffs.nu v0.14.2
#   nu .ops/scripts/conflict-diffs.nu v0.14.2 merge/upstream-v0.14.2
#   nu .ops/scripts/conflict-diffs.nu v0.14.2 --file Cargo.toml
#   nu .ops/scripts/conflict-diffs.nu                         # uses merge-status defaults

use lib/git/upstream.nu *

def save-lines [items: list<string>, path: string] {
  let content = if (($items | length) == 0) {
    ""
  } else {
    ($items | str join (char nl)) + (char nl)
  }
  $content | save -f $path
}

def sanitize-name [path: string] {
  $path
  | str replace -a "/" "__"
  | str replace -ra '[^A-Za-z0-9_.-]' '_'
}

def main [
  base_ref?: string
  target_ref?: string
  --file (-f): string
  --output-dir (-o): string
] {
  let status = (read-merge-status)
  let status_current_tag = ($status | get --optional tags.current_upstream_tag)
  let status_base_ref = ($status | get --optional merge.base_ref)
  let status_merge_branch = ($status | get --optional merge.merge_branch)
  let status_worktree = ($status | get --optional merge.worktree_path)
  let cwd = (pwd | into string)
  let active_merge = (has-active-merge)
  let using_defaults = ($base_ref == null and $target_ref == null)

  if ($active_merge and $using_defaults) {
    if ($status_worktree != null and not ($cwd | str starts-with $status_worktree)) {
      error make {
        msg: $"Active merge exists at ($status_worktree). Run this command from that worktree or pass explicit refs."
      }
    }
  }

  let is_tag = if $base_ref != null and ($base_ref | str starts-with "v") and not ($base_ref | str contains "/") { true } else { false }

  let base = if $base_ref == null {
    if ($status_base_ref != null and (git-ref-exists $status_base_ref)) {
      $status_base_ref
    } else if $status_current_tag == null {
      error make { msg: "base_ref is required (or set tags.current_upstream_tag in .ops/merge-status.yaml)" }
    } else if not (git-ref-exists $"upstream/($status_current_tag)") {
      error make { msg: $"Default base ref upstream/($status_current_tag) does not exist locally. Create/fetch upstream branch first." }
    }
    else {
      $"upstream/($status_current_tag)"
    }
  } else if $is_tag {
    $"upstream/($base_ref)"
  } else {
    $base_ref
  }

  let target = if $target_ref != null {
    $target_ref
  } else if $is_tag {
    let implied = $"merge/upstream-($base_ref)"
    if (git-ref-exists $implied) {
      $implied
    } else {
      "main"
    }
  } else {
    if ($status_merge_branch != null and (git-ref-exists $status_merge_branch)) {
      $status_merge_branch
    } else {
      "main"
    }
  }

  if not (git-ref-exists $base) {
    error make { msg: $"Unknown base ref: ($base)" }
  }
  if not (git-ref-exists $target) {
    error make { msg: $"Unknown target ref: ($target)" }
  }

  let repo_root = (^git rev-parse --show-toplevel | str trim)
  let base_slug = (slugify-ref $base)
  let target_slug = (slugify-ref $target)
  let out_dir = if $output_dir == null {
    $"($repo_root)/.ops/.tmp/upstream-radar/($base_slug)__($target_slug)/conflict-diffs"
  } else {
    $output_dir
  }
  ^mkdir -p $out_dir

  let mb = (merge-base-ref $base $target)
  let mb_short = (^git rev-parse --short $mb | str trim)

  let conflict_pred = (predict-conflict-files $base $target)
  let overlap = (intersect-files (changed-files-between $mb $target) (changed-files-between $mb $base))
  let predicted = if $conflict_pred.source == "merge-tree" { $conflict_pred.files } else { $overlap }
  let conflict_source = if $conflict_pred.source == "merge-tree" { "merge-tree" } else { "overlap-fallback" }

  if (($predicted | length) == 0) {
    {
      base_ref: $base
      target_ref: $target
      merge_base: $mb_short
      conflict_source: $conflict_source
      predicted_conflicts: 0
      generated_pairs: 0
      output_dir: $out_dir
      note: "No predicted conflict files."
    } | print
    return
  }

  let selected = if $file == null {
    $predicted
  } else {
    if not ($predicted | any { |f| $f == $file }) {
      error make { msg: $"Requested file is not in predicted conflicts: ($file)" }
    }
    [ $file ]
  }

  save-lines $selected $"($out_dir)/selected-files.txt"
  $conflict_pred.log | save -f $"($out_dir)/merge_tree.log"

  let generated = (
    $selected
    | each { |f|
      let safe = (sanitize-name $f)
      let adi_patch = $"($out_dir)/($safe).adi.patch"
      let upstream_patch = $"($out_dir)/($safe).upstream.patch"

      (^git diff $"($mb)..($target)" -- $f) | save -f $adi_patch
      (^git diff $"($mb)..($base)" -- $f) | save -f $upstream_patch

      {
        file: $f
        adi_patch: $adi_patch
        upstream_patch: $upstream_patch
      }
    }
  )

  let env_lines = [
    $"BASE_REF=($base)"
    $"TARGET_REF=($target)"
    $"MERGE_BASE=($mb)"
    $"MERGE_BASE_SHORT=($mb_short)"
    $"CONFLICT_SOURCE=($conflict_source)"
    $"PREDICTED_CONFLICT_FILES=(($predicted | length))"
    $"GENERATED_PATCH_PAIRS=(($generated | length))"
  ]
  save-lines $env_lines $"($out_dir)/summary.env"

  $generated | to json | save -f $"($out_dir)/manifest.json"

  {
    base_ref: $base
    target_ref: $target
    merge_base: $mb_short
    conflict_source: $conflict_source
    predicted_conflicts: ($predicted | length)
    generated_pairs: ($generated | length)
    output_dir: $out_dir
    manifest: $"($out_dir)/manifest.json"
  } | print
}
