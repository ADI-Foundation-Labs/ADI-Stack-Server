# Start upstream merge process end-to-end with guardrails.
#
# Flow:
# 1) Validate tag + active merge guardrails.
# 2) Sync upstream/<tag> from upstream tag.
# 3) Ensure merge worktree exists at ~/.local/git/wortrees/<project>/merge-upstream-<tag>.
# 4) Run merge in worktree (or report existing unresolved conflicts).
# 5) Update .ops/merge-status.yaml.
# 6) Run merge radar for up-to-date status metrics.
#
# Usage:
#   nu .ops/scripts/start-upstream-merge.nu v0.14.2
#   nu .ops/scripts/start-upstream-merge.nu v0.14.2 --push-upstream --push-merge
#   nu .ops/scripts/start-upstream-merge.nu v0.14.2 --worktree-root /tmp/worktrees

use lib/git/upstream.nu *

def list-conflict-files [worktree_path: string] {
  ^git -C $worktree_path diff --name-only --diff-filter=U
  | lines
  | where { |l| ($l | str trim) != "" }
  | sort
  | uniq
}

def list-dirty-files [worktree_path: string] {
  ^git -C $worktree_path status --porcelain
  | lines
  | where { |l| ($l | str trim) != "" }
}

def main [
  tag_input: string
  --push-upstream (-u)
  --push-merge (-p)
  --force (-f)
  --skip-radar (-R)
  --max-list (-n): int = 40
  --worktree-root (-w): string
] {
  let tag = (extract-semver-tag $tag_input)
  if $tag == null {
    error make {
      msg: $"Invalid tag input: ($tag_input). Expected semver tag like v0.14.2"
    }
  }

  let upstream_branch = $"upstream/($tag)"
  let merge_branch = $"merge/upstream-($tag)"
  let project = (git-project-name)
  let wt_root = if $worktree_root == null {
    $"($env.HOME)/.local/git/wortrees/($project)"
  } else {
    $worktree_root
  }
  let wt_path = $"($wt_root)/merge-upstream-($tag)"

  let st_before = (read-merge-status)
  let active = (has-active-merge)
  let active_tag = ($st_before | get --optional tags.current_upstream_tag)
  let active_status = ($st_before | get --optional merge.status | default "idle")
  let active_branch = ($st_before | get --optional merge.merge_branch | default "<unknown>")
  let active_worktree = ($st_before | get --optional merge.worktree_path | default "<unknown>")

  if ($active and not $force) {
    if ($active_tag != $tag) {
      error make {
        msg: $"Active merge detected for ($active_tag) in status ($active_status), branch=($active_branch), worktree=($active_worktree). Finish/abort it first or rerun with --force."
      }
    }

    if ($active_tag == $tag and $active_worktree != "<unknown>" and $active_worktree != $wt_path) {
      error make {
        msg: $"Active merge for ($tag) is tracked at ($active_worktree), but requested worktree path is ($wt_path). Re-run with --worktree-root matching active state, or use --force."
      }
    }
  }

  if not (upstream-has-tag $tag) {
    error make {
      msg: $"Tag ($tag) not found on upstream. Run: git fetch upstream --tags"
    }
  }

  ^git fetch upstream --tags
  ^git fetch origin --tags

  let tag_ref = $"refs/tags/($tag)"
  let branch_ref = $"refs/heads/($upstream_branch)"
  ^git fetch --force upstream $"($tag_ref):($branch_ref)"

  mut pushed_upstream_branch = false
  if $push_upstream {
    ^git push -f origin $upstream_branch
    $pushed_upstream_branch = true
  }

  mut worktree_created = false
  if ($wt_path | path exists) {
    let wt_check = (^git -C $wt_path rev-parse --is-inside-work-tree | complete)
    if $wt_check.exit_code != 0 {
      error make {
        msg: $"Path exists but is not a git worktree: ($wt_path)"
      }
    }
    let current_branch = (^git -C $wt_path rev-parse --abbrev-ref HEAD | str trim)
    if $current_branch != $merge_branch {
      error make {
        msg: $"Worktree path ($wt_path) is on branch ($current_branch), expected ($merge_branch)."
      }
    }
  } else {
    ^mkdir -p $wt_root
    let add = (^git worktree add -B $merge_branch $wt_path origin/main | complete)
    if $add.exit_code != 0 {
      let msg = ($add.stderr | str trim)
      let extra = if $msg == "" { $add.stdout | str trim } else { $msg }
      error make {
        msg: $"Failed to create worktree at ($wt_path): ($extra)"
      }
    }
    $worktree_created = true
  }

  let unresolved_before = (list-conflict-files $wt_path)
  let dirty_before = (list-dirty-files $wt_path)

  if ((($unresolved_before | length) == 0) and (($dirty_before | length) > 0)) {
    error make {
      msg: $"Merge worktree is dirty before merge. Clean it first: ($wt_path)"
    }
  }

  mut merge_invoked = false
  mut merge_exit_code = 0
  mut merge_stdout = ""
  mut merge_stderr = ""

  if (($unresolved_before | length) == 0) {
    let merge_run = (^git -C $wt_path merge --no-ff $upstream_branch | complete)
    $merge_invoked = true
    $merge_exit_code = $merge_run.exit_code
    $merge_stdout = ($merge_run.stdout | str trim)
    $merge_stderr = ($merge_run.stderr | str trim)
  } else {
    $merge_invoked = false
    $merge_exit_code = 1
    $merge_stderr = "Existing unresolved conflicts detected. Merge command was not re-run."
  }

  let unresolved_after = (list-conflict-files $wt_path)
  let has_conflicts = (($unresolved_after | length) > 0)
  let merge_failed = (($merge_exit_code != 0) and not $has_conflicts)

  let st = (read-merge-status)
  let current_tag = ($st | get --optional tags.current_upstream_tag)
  let previous_tag = ($st | get --optional tags.previous_upstream_tag)
  let next_previous = if ($current_tag != null and $current_tag != $tag) {
    $current_tag
  } else {
    $previous_tag
  }
  let created_at = ($st | get --optional merge.created_at | default (now-iso))

  let next = (
    $st
    | upsert tags {
      previous_upstream_tag: $next_previous
      current_upstream_tag: $tag
    }
    | upsert merge {
      status: "merge_attempted"
      base_ref: $upstream_branch
      target_ref: "main"
      upstream_branch: $upstream_branch
      merge_branch: $merge_branch
      worktree_path: $wt_path
      created_at: $created_at
      last_radar_at: ($st | get --optional merge.last_radar_at | default null)
      release_tag: null
      last_merge_at: (now-iso)
      last_merge_exit_code: $merge_exit_code
      has_conflicts: $has_conflicts
      conflict_files: $unresolved_after
    }
  )

  let status_file = (write-merge-status $next)

  mut pushed_merge_branch = false
  if $push_merge and not $has_conflicts and not $merge_failed {
    ^git -C $wt_path push -u origin $merge_branch
    $pushed_merge_branch = true
  }

  mut radar_updated = false
  if not $skip_radar {
    if $max_list == 40 {
      ^nu .ops/scripts/merge-radar.nu $upstream_branch $merge_branch
    } else {
      ^nu .ops/scripts/merge-radar.nu $upstream_branch $merge_branch --max-list $max_list
    }
    if $has_conflicts {
      ^nu .ops/scripts/conflict-diffs.nu $upstream_branch $merge_branch
    }
    $radar_updated = true
  }

  let summary = {
    tag: $tag
    upstream_branch: $upstream_branch
    merge_branch: $merge_branch
    worktree_path: $wt_path
    worktree_created: $worktree_created
    merge_invoked: $merge_invoked
    merge_exit_code: $merge_exit_code
    merge_stdout: $merge_stdout
    merge_stderr: $merge_stderr
    has_conflicts: $has_conflicts
    merge_failed: $merge_failed
    conflict_files: $unresolved_after
    pushed_upstream_branch: $pushed_upstream_branch
    pushed_merge_branch: $pushed_merge_branch
    radar_updated: $radar_updated
    status_file: $status_file
    next_step: (if $has_conflicts {
      $"Resolve conflicts in ($wt_path), run tests, then transition status."
    } else if $merge_failed {
      $"Merge command failed without unresolved conflicts. Inspect merge_stderr and worktree: ($wt_path)"
    } else {
      $"Run local tests in ($wt_path), then: task upgrade:status:transition -- local-tested"
    })
  }

  $summary | print
}
