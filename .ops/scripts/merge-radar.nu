# Build a visual merge complexity report for BASE (upstream release ref) vs TARGET (default: main).
# Outputs machine-readable artifacts under:
#   .ops/.tmp/upstream-radar/<base_slug>__<target_slug>/
#
# Usage:
#   nu .ops/scripts/merge-radar.nu v0.14.2
#   nu .ops/scripts/merge-radar.nu v0.14.2 merge/upstream-v0.14.2 --max-list 80
#   nu .ops/scripts/merge-radar.nu v0.14.2 main --output-dir /tmp/radar
#   nu .ops/scripts/merge-radar.nu                     # uses merge-status defaults

use lib/git/upstream.nu *

def save-lines [items: list<string>, path: string] {
  let content = if (($items | length) == 0) {
    ""
  } else {
    ($items | str join (char nl)) + (char nl)
  }
  $content | save -f $path
}

def render-list-items [items: list<string>, max_list: int] {
  if (($items | length) == 0) {
    [ "- _none_" ]
  } else {
    let shown = ($items | first $max_list | each { |f| $"- `($f)`" })
    if (($items | length) > $max_list) {
      let more = (($items | length) - $max_list)
      ($shown ++ [ $"- _... ($more) more_" ])
    } else {
      $shown
    }
  }
}

def section-lines [title: string, items: list<string>, max_list: int] {
  ([ $"## ($title)" ] ++ (render-list-items $items $max_list) ++ [ "" ])
}

def parse-counts [text: string] {
  let parsed = ($text | str trim | parse -r '^(?P<left>[0-9]+)\s+(?P<right>[0-9]+)$')
  if (($parsed | length) == 0) {
    error make { msg: $"Cannot parse commit counts from: ($text)" }
  }
  {
    left: (($parsed | get 0.left) | into int)
    right: (($parsed | get 0.right) | into int)
  }
}

def main [
  base_ref?: string
  target_ref?: string
  --output-dir (-o): string
  --max-list (-n): int = 40
  --no-status (-S)
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
    $"($repo_root)/.ops/.tmp/upstream-radar/($base_slug)__($target_slug)"
  } else {
    $output_dir
  }

  ^mkdir -p $"($out_dir)/lists"

  let mb = (merge-base-ref $base $target)
  let mb_short = (^git rev-parse --short $mb | str trim)
  let base_short = (^git rev-parse --short $base | str trim)
  let target_short = (^git rev-parse --short $target | str trim)

  let counts = (parse-counts (^git rev-list --left-right --count $"($target)...($base)"))
  let target_only_commits = $counts.left
  let base_only_commits = $counts.right

  let adi_changed = (changed-files-between $mb $target)
  let upstream_changed = (changed-files-between $mb $base)
  let overlap = (intersect-files $adi_changed $upstream_changed)
  let adi_only = (added-files-between $base $target)
  let upstream_only = (added-files-between $target $base)

  let conflict_pred = (predict-conflict-files $base $target)
  let conflicts = if $conflict_pred.source == "merge-tree" {
    $conflict_pred.files
  } else {
    $overlap
  }
  let conflict_source = if $conflict_pred.source == "merge-tree" { "merge-tree" } else { "overlap-fallback" }

  save-lines $adi_changed $"($out_dir)/lists/adi_changed.txt"
  save-lines $upstream_changed $"($out_dir)/lists/upstream_changed.txt"
  save-lines $overlap $"($out_dir)/lists/overlap.txt"
  save-lines $conflicts $"($out_dir)/lists/conflicts.txt"
  save-lines $adi_only $"($out_dir)/lists/adi_only.txt"
  save-lines $upstream_only $"($out_dir)/lists/upstream_only.txt"
  $conflict_pred.log | save -f $"($out_dir)/merge_tree.log"

  let adi_changed_count = ($adi_changed | length)
  let upstream_changed_count = ($upstream_changed | length)
  let overlap_count = ($overlap | length)
  let conflict_count = ($conflicts | length)
  let adi_only_count = ($adi_only | length)
  let upstream_only_count = ($upstream_only | length)

  let risk_level = if ($conflict_count >= 12 or $overlap_count >= 40) {
    "HIGH"
  } else if ($conflict_count >= 4 or $overlap_count >= 15) {
    "MEDIUM"
  } else {
    "LOW"
  }

  let env_lines = [
    $"BASE_REF=($base)"
    $"TARGET_REF=($target)"
    $"BASE_COMMIT=($base_short)"
    $"TARGET_COMMIT=($target_short)"
    $"MERGE_BASE=($mb)"
    $"MERGE_BASE_SHORT=($mb_short)"
    $"TARGET_ONLY_COMMITS=($target_only_commits)"
    $"BASE_ONLY_COMMITS=($base_only_commits)"
    $"ADI_CHANGED_FILES=($adi_changed_count)"
    $"UPSTREAM_CHANGED_FILES=($upstream_changed_count)"
    $"OVERLAP_FILES=($overlap_count)"
    $"PREDICTED_CONFLICT_FILES=($conflict_count)"
    $"ADI_ONLY_FILES=($adi_only_count)"
    $"UPSTREAM_ONLY_FILES=($upstream_only_count)"
    $"RISK_LEVEL=($risk_level)"
    $"CONFLICT_SOURCE=($conflict_source)"
  ]
  save-lines $env_lines $"($out_dir)/summary.env"

  let report_lines = (
    [
      "# Merge Radar"
      ""
      "| Metric | Value |"
      "|---|---|"
      $"| Base ref | ($base) (($base_short)) |"
      $"| Target ref | ($target) (($target_short)) |"
      $"| Merge base | ($mb_short) |"
      $"| Target-only commits | ($target_only_commits) |"
      $"| Base-only commits | ($base_only_commits) |"
      $"| ADI changed files | ($adi_changed_count) |"
      $"| Upstream changed files | ($upstream_changed_count) |"
      $"| Overlap files | ($overlap_count) |"
      $"| Predicted conflict files | ($conflict_count) (($conflict_source)) |"
      $"| ADI-only files at tip | ($adi_only_count) |"
      $"| Upstream-only files at tip | ($upstream_only_count) |"
      $"| Risk level | **($risk_level)** |"
      ""
    ]
    ++ (section-lines "Predicted Conflicts" $conflicts $max_list)
    ++ (section-lines "Overlap (Both Changed Since Merge Base)" $overlap $max_list)
    ++ (section-lines "Files Only In ADI Tip" $adi_only $max_list)
    ++ (section-lines "Files Only In Upstream Tip" $upstream_only $max_list)
  )
  ($report_lines | str join (char nl)) | save -f $"($out_dir)/report.md"

  let summary = {
    base_ref: $base
    target_ref: $target
    merge_base: $mb_short
    target_only_commits: $target_only_commits
    base_only_commits: $base_only_commits
    adi_changed_files: $adi_changed_count
    upstream_changed_files: $upstream_changed_count
    overlap_files: $overlap_count
    predicted_conflict_files: $conflict_count
    conflict_source: $conflict_source
    risk_level: $risk_level
    output_dir: $out_dir
    report: $"($out_dir)/report.md"
  }

  if not $no_status {
    let status_file = (update-merge-status-from-radar $base $target $summary)
    ($summary | upsert status_updated true | upsert status_file $status_file) | print
  } else {
    ($summary | upsert status_updated false) | print
  }
}
