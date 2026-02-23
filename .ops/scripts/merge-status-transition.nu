# Transition merge status lifecycle and optionally set release tag.
#
# Phases:
#   local-tested | ci-passed | merged-main | released | aborted | idle
#
# Usage:
#   nu .ops/scripts/merge-status-transition.nu local-tested
#   nu .ops/scripts/merge-status-transition.nu ci-passed
#   nu .ops/scripts/merge-status-transition.nu merged-main
#   nu .ops/scripts/merge-status-transition.nu released
#   nu .ops/scripts/merge-status-transition.nu released --release-tag v0.14.2-b3
#   nu .ops/scripts/merge-status-transition.nu aborted --note "manual rollback"

use lib/git/upstream.nu *

def phase-to-status [phase: string] {
  match $phase {
    "local-tested" => "local_testing_passed"
    "ci-passed" => "ci_passed"
    "merged-main" => "merged_to_main"
    "released" => "released"
    "aborted" => "aborted"
    "idle" => "idle"
    _ => (error make {
      msg: $"Unknown phase: ($phase). Allowed: local-tested, ci-passed, merged-main, released, aborted, idle"
    })
  }
}

def main [
  phase: string
  --release-tag (-r): string
  --note (-n): string
] {
  let status_file = (merge-status-path)
  let st = (read-merge-status)
  let new_status = (phase-to-status $phase)
  let now = (now-iso)

  if ($phase != "idle" and $phase != "aborted") {
    assert-active-merge
  }

  let merge = ($st | get --optional merge | default {})
  let tags = ($st | get --optional tags | default { previous_upstream_tag: null, current_upstream_tag: null })
  let current_tag = ($tags | get --optional current_upstream_tag)

  let resolved_release_tag = if $phase == "released" {
    if $release_tag != null {
      $release_tag
    } else {
      if $current_tag == null {
        error make { msg: "Cannot compute release tag: tags.current_upstream_tag is null" }
      }
      next-b-release-tag $current_tag
    }
  } else {
    ($merge | get --optional release_tag | default null)
  }

  if ($phase == "released" and $resolved_release_tag != null and (git-tag-exists $resolved_release_tag)) {
    error make { msg: $"Release tag already exists: ($resolved_release_tag)" }
  }

  let next_merge = (
    $merge
    | upsert status $new_status
    | upsert last_radar_at ($merge | get --optional last_radar_at | default null)
    | upsert release_tag $resolved_release_tag
  )

  let next = (
    $st
    | upsert merge $next_merge
    | upsert updated_at $now
  )

  let cleaned = if ($phase == "idle" or $phase == "aborted") {
    (
      $next
      | upsert merge {
        status: $new_status
        base_ref: null
        target_ref: "main"
        upstream_branch: null
        merge_branch: null
        worktree_path: null
        created_at: null
        last_radar_at: null
        release_tag: null
        last_merge_at: null
        last_merge_exit_code: null
        has_conflicts: null
        conflict_files: []
      }
      | upsert radar {
        risk_level: null
        conflict_source: null
        target_only_commits: null
        base_only_commits: null
        adi_changed_files: null
        upstream_changed_files: null
        overlap_files: null
        predicted_conflict_files: null
        output_dir: null
        report: null
      }
    )
  } else {
    $next
  }

  let final = if ($note == null) {
    $cleaned
  } else {
    $cleaned | upsert merge.note $note
  }

  let path = (write-merge-status $final)
  {
    phase: $phase
    merge_status: $new_status
    release_tag: $resolved_release_tag
    status_file: $path
  } | table
}
