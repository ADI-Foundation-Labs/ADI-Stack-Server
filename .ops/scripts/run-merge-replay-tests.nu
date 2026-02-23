# Replay historical merge cases and assert resulting tree against expected tagged commit.
#
# Usage:
#   nu .ops/scripts/run-merge-replay-tests.nu
#   nu .ops/scripts/run-merge-replay-tests.nu --cases .ops/merge-replay-cases.yaml --case-id v0.12.1-b
#   nu .ops/scripts/run-merge-replay-tests.nu --limit 5 --keep-worktrees

def sanitize [s: string] {
  $s
  | str replace -a "/" "_"
  | str replace -a ":" "_"
  | str replace -a " " "_"
  | str replace -ra '[^A-Za-z0-9_.-]' '_'
}

def list-conflicts [worktree_path: string] {
  ^git -C $worktree_path diff --name-only --diff-filter=U
  | lines
  | where { |l| ($l | str trim) != "" }
  | sort
  | uniq
}

def run-single-case [case: record, run_root: string, keep_worktrees: bool] {
  let case_id = ($case | get case_id)
  let safe_id = (sanitize $case_id)
  let wt_path = $"($run_root)/($safe_id)"
  let pre_merge_commit = ($case | get pre_merge_commit)
  let upstream_commit = ($case | get upstream.commit)
  let expected_commit = ($case | get expected.commit)
  let expected_tree = ($case | get expected.tree)
  let mode = ($case | get assertions.mode | default "exact_tree")
  let allowed_paths = ($case | get assertions.allowed_paths | default [])

  ^mkdir -p $run_root

  let clone = (^git clone --no-checkout . $wt_path | complete)
  if $clone.exit_code != 0 {
    return {
      case_id: $case_id
      status: "error"
      reason: "clone_failed"
      message: (($clone.stderr | str trim) + (if ($clone.stdout | str trim) == "" { "" } else { " " + ($clone.stdout | str trim) }))
    }
  }

  let checkout = (^git -C $wt_path checkout --detach $pre_merge_commit | complete)
  if $checkout.exit_code != 0 {
    return {
      case_id: $case_id
      status: "error"
      reason: "checkout_failed"
      message: (($checkout.stderr | str trim) + (if ($checkout.stdout | str trim) == "" { "" } else { " " + ($checkout.stdout | str trim) }))
      worktree_path: $wt_path
    }
  }

  let merge = (^git -C $wt_path merge --no-ff --no-edit $upstream_commit | complete)
  let conflicts = (list-conflicts $wt_path)
  let has_conflicts = (($conflicts | length) > 0)

  mut status = "failed"
  mut reason = "unknown"
  mut actual_commit = ""
  mut actual_tree = ""
  mut delta_paths = []

  if $has_conflicts {
    $status = "failed"
    $reason = "merge_conflicts"
  } else if $merge.exit_code != 0 {
    $status = "failed"
    $reason = "merge_failed"
  } else {
    $actual_commit = (^git -C $wt_path rev-parse HEAD | str trim)
    let actual_tree_ref = ($actual_commit + "^{tree}")
    $actual_tree = (^git -C $wt_path rev-parse $actual_tree_ref | str trim)

    if $mode == "exact_tree" {
      if $actual_tree == $expected_tree {
        $status = "passed"
        $reason = "exact_tree_match"
      } else {
        $status = "failed"
        $reason = "exact_tree_mismatch"
        $delta_paths = (
          ^git -C $wt_path diff --name-only $expected_commit $actual_commit
          | lines
          | where { |l| ($l | str trim) != "" }
          | sort
          | uniq
        )
      }
    } else if $mode == "allow_paths_delta" {
      $delta_paths = (
        ^git -C $wt_path diff --name-only $expected_commit $actual_commit
        | lines
        | where { |l| ($l | str trim) != "" }
        | sort
        | uniq
      )

      let disallowed = ($delta_paths | where { |p| not ($allowed_paths | any { |a| $a == $p }) })
      if (($disallowed | length) == 0) {
        $status = "passed"
        $reason = "allowed_delta_only"
      } else {
        $status = "failed"
        $reason = "disallowed_delta_paths"
      }
    } else {
      $status = "failed"
      $reason = $"unsupported_assertion_mode: ($mode)"
    }
  }

  let rec = {
    case_id: $case_id
    release_tag: ($case | get release_tag)
    version: ($case | get version)
    pre_merge_commit: $pre_merge_commit
    upstream_commit: $upstream_commit
    expected_commit: $expected_commit
    expected_tree: $expected_tree
    actual_commit: (if $actual_commit == "" { null } else { $actual_commit })
    actual_tree: (if $actual_tree == "" { null } else { $actual_tree })
    status: $status
    reason: $reason
    has_conflicts: $has_conflicts
    conflicts: $conflicts
    delta_paths: $delta_paths
    merge_stdout: ($merge.stdout | str trim)
    merge_stderr: ($merge.stderr | str trim)
    worktree_path: $wt_path
  }

  # Keep run clones by default for inspection/debugging. They live under .ops/.tmp and are gitignored.
  # `--keep-worktrees` is retained for compatibility with previous worktree-based runner.

  $rec
}

def main [
  --cases (-c): string = ".ops/merge-replay-cases.yaml"
  --output (-o): string
  --case-id (-i): string
  --limit (-n): int
  --keep-worktrees (-k)
  --run-root (-r): string
] {
  if not ($cases | path exists) {
    error make { msg: $"Cases file not found: ($cases). Run: task upgrade:test:build-replay-cases" }
  }

  let data = (open $cases)
  let all_cases = ($data | get cases)

  let filtered = if $case_id == null {
    $all_cases
  } else {
    $all_cases | where { |c| ($c | get case_id) == $case_id }
  }

  let selected = if $limit == null { $filtered } else { $filtered | first $limit }
  if (($selected | length) == 0) {
    error make { msg: "No replay cases selected." }
  }

  let timestamp = (date now | format date "%Y%m%d%H%M%S")
  let run_root = if $run_root == null {
    $".ops/.tmp/merge-replay-runs/($timestamp)"
  } else {
    $run_root
  }
  ^mkdir -p $run_root

  let results = (
    $selected
    | each { |case| run-single-case $case $run_root $keep_worktrees }
  )

  let passed = ($results | where { |r| ($r | get status) == "passed" } | length)
  let failed = ($results | where { |r| ($r | get status) != "passed" } | length)
  let summary = {
    schema_version: 1
    generated_at: (date now | format date "%Y-%m-%dT%H:%M:%S%:z")
    cases_file: $cases
    total: ($results | length)
    passed: $passed
    failed: $failed
    run_root: $run_root
    keep_worktrees: $keep_worktrees
    results: $results
  }

  let final_output = if $output == null {
    $"($run_root)/results.yaml"
  } else {
    $output
  }
  let out_dir = ($final_output | path dirname)
  ^mkdir -p $out_dir
  ($summary | to yaml) | save -f $final_output

  {
    output: $final_output
    total: ($results | length)
    passed: $passed
    failed: $failed
    run_root: $run_root
  } | print

  if $failed > 0 {
    error make { msg: $"Replay failures detected: ($failed). See ($final_output)" }
  }
}
