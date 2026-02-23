# Create merge/upstream-vX.Y.Z from main in a worktree (runbook step 4); optionally merge (step 5) and push.
# Worktree: ~/.local/git/wortrees/<project>/merge-upstream-<tag>.
# Requires: git, upstream/vX.Y.Z branch (create first with task upgrade:create-upstream-branch).
# Run from repo root. Output: table with tag, merge_branch, worktree_path, merged, pushed.
# Usage: nu create-merge-branch-from-tag.nu vX.Y.Z [--merge] [--push] [--reset] [-y]

use lib/git/upstream.nu *

def local-branch-exists [branch: string] {
  ((^git rev-parse --verify $"refs/heads/($branch)" | complete).exit_code == 0)
}

def delete-local-branch-if-exists [branch: string] {
  if not (local-branch-exists $branch) {
    return false
  }

  let del = (^git branch -D $branch | complete)
  if $del.exit_code != 0 {
    let err = ($del.stderr | str trim)
    let out = ($del.stdout | str trim)
    let msg = if $err != "" { $err } else if $out != "" { $out } else { $"Failed to delete branch ($branch)" }
    error make { msg: $msg }
  }
  true
}

def remove-worktree-if-exists [worktree_path: string] {
  if not ($worktree_path | path exists) {
    return false
  }

  let rm = (^git worktree remove --force $worktree_path | complete)
  if $rm.exit_code != 0 {
    let err = ($rm.stderr | str trim)
    let out = ($rm.stdout | str trim)
    let msg = if $err != "" { $err } else if $out != "" { $out } else { $"Failed to remove worktree ($worktree_path)" }
    error make { msg: $msg }
  }
  true
}

def run-create-merge-branch [tag: string, merge: bool, push: bool, force: bool] {
  if $merge and $push and $force {
    create-merge-branch-from-tag $tag --merge --push --force
  } else if $merge and $push {
    create-merge-branch-from-tag $tag --merge --push
  } else if $merge and $force {
    create-merge-branch-from-tag $tag --merge --force
  } else if $merge {
    create-merge-branch-from-tag $tag --merge
  } else if $push and $force {
    create-merge-branch-from-tag $tag --push --force
  } else if $push {
    create-merge-branch-from-tag $tag --push
  } else if $force {
    create-merge-branch-from-tag $tag --force
  } else {
    create-merge-branch-from-tag $tag
  }
}

def reset-merge-state [
  tag: string
  --yes (-y)
] {
  let merge_branch = $"merge/upstream-($tag)"
  let project = (git-project-name)
  let default_wt_path = $"($env.HOME)/.local/git/wortrees/($project)/merge-upstream-($tag)"

  let status = (read-merge-status)
  let tracked_tag = ($status | get --optional tags.current_upstream_tag)
  let tracked_wt = ($status | get --optional merge.worktree_path)
  let tracked_wt_valid = (
    $tracked_tag == $tag
    and $tracked_wt != null
    and ($tracked_wt | str trim) != ""
  )

  let worktree_candidates = if $tracked_wt_valid {
    [ $default_wt_path, $tracked_wt ] | uniq
  } else {
    [ $default_wt_path ]
  }

  if not $yes {
    print $"Reset requested for tag ($tag)."
    print "This will:"
    print $"  1) Remove merge worktree paths: (($worktree_candidates | str join ', '))"
    print $"  2) Delete local branch: ($merge_branch)"
    print $"  3) Keep upstream branch untouched: upstream/($tag)"
    let answer = (input "Type 'yes' to continue: " | str trim | str downcase)
    if $answer != "yes" {
      error make {
        msg: "Aborted by user. Pass -y/--yes to bypass confirmation."
      }
    }
  }

  # Do not switch current branch here; reset only merge branch/worktree state.

  mut removed_worktrees = []
  for wt in $worktree_candidates {
    if (remove-worktree-if-exists $wt) {
      $removed_worktrees = ($removed_worktrees ++ [ $wt ])
    }
  }
  ^git worktree prune

  let merge_branch_deleted = (delete-local-branch-if-exists $merge_branch)

  {
    reset_requested: true
    worktree_candidates: $worktree_candidates
    removed_worktrees: $removed_worktrees
    merge_branch_deleted: $merge_branch_deleted
    upstream_branch_untouched: true
  }
}

def main [
  tag: string
  --merge (-m)
  --push (-p)
  --force (-f)
  --reset (-r)
  --yes (-y)
] {
  let reset_summary = if $reset {
    if $yes {
      reset-merge-state $tag --yes
    } else {
      reset-merge-state $tag
    }
  } else {
    null
  }

  let effective_force = ($force or $reset)
  let created = (run-create-merge-branch $tag $merge $push $effective_force)
  let result = if $reset_summary != null {
    ($created | upsert reset $reset_summary)
  } else {
    $created
  }

  [ $result ] | print
}
