# Open merge worktree directory in an IDE binary (cursor, code, etc).
#
# Usage:
#   nu .ops/scripts/open-merge-worktree.nu cursor
#   nu .ops/scripts/open-merge-worktree.nu code v0.14.2
#   nu .ops/scripts/open-merge-worktree.nu cursor --dry-run
#   task upgrade:open:merge-worktree -- cursor

use lib/git/upstream.nu *

def expected-worktree-path [tag: string, worktree_root?: string] {
  let project = (git-project-name)
  let root = if $worktree_root == null {
    $"($env.HOME)/.local/git/wortrees/($project)"
  } else {
    $worktree_root
  }
  $"($root)/merge-upstream-($tag)"
}

def resolve-tag [status: record, tag_input?: string] {
  if $tag_input != null {
    let parsed = (extract-semver-tag $tag_input)
    if $parsed == null {
      error make {
        msg: $"Invalid tag input: ($tag_input). Expected semver tag like v0.14.2"
      }
    }
    $parsed
  } else {
    let tracked = ($status | get --optional tags.current_upstream_tag)
    if $tracked == null {
      error make {
        msg: "No current upstream tag in .ops/merge-status.yaml. Pass tag explicitly: task upgrade:open:merge-worktree -- <ide-binary> vX.Y.Z"
      }
    }
    $tracked
  }
}

def main [
  ide_binary: string
  tag_input?: string
  --worktree-root (-w): string
  --dry-run (-n)
] {
  let status = (read-merge-status)
  let active = (has-active-merge)
  let tracked_tag = ($status | get --optional tags.current_upstream_tag)
  let tracked_worktree = ($status | get --optional merge.worktree_path)
  let tracked_merge_branch = ($status | get --optional merge.merge_branch)
  let tracked_status = ($status | get --optional merge.status | default "idle")

  let tag = (resolve-tag $status $tag_input)
  let expected_merge_branch = $"merge/upstream-($tag)"
  let expected_worktree = (expected-worktree-path $tag $worktree_root)

  if ($active and $tracked_tag != null and $tracked_tag != $tag) {
    error make {
      msg: $"Active merge detected for ($tracked_tag), status=($tracked_status). Open that worktree instead or finish/abort it first."
    }
  }

  if ($tracked_tag == $tag and $tracked_merge_branch != null and $tracked_merge_branch != $expected_merge_branch) {
    error make {
      msg: $"merge-status mismatch for tag ($tag): tracked merge branch is ($tracked_merge_branch), expected ($expected_merge_branch)."
    }
  }

  if ($tracked_tag == $tag and $tracked_worktree != null and $tracked_worktree != "" and $worktree_root != null and $tracked_worktree != $expected_worktree) {
    error make {
      msg: $"Active/tracked merge for ($tag) points to ($tracked_worktree), but --worktree-root resolves to ($expected_worktree)."
    }
  }

  let worktree_path = if ($tracked_tag == $tag and $tracked_worktree != null and $tracked_worktree != "") {
    $tracked_worktree
  } else {
    $expected_worktree
  }

  if not ($worktree_path | path exists) {
    error make {
      msg: $"Merge worktree path does not exist: ($worktree_path). Start merge first: task upgrade:start:merge -- ($tag)"
    }
  }

  let wt_check = (^git -C $worktree_path rev-parse --is-inside-work-tree | complete)
  if $wt_check.exit_code != 0 {
    error make {
      msg: $"Path is not a git worktree: ($worktree_path)"
    }
  }

  let current_branch = (^git -C $worktree_path rev-parse --abbrev-ref HEAD | str trim)
  if $current_branch != $expected_merge_branch {
    error make {
      msg: $"Worktree branch mismatch at ($worktree_path): current=($current_branch), expected=($expected_merge_branch)"
    }
  }

  let ide_check = (^which $ide_binary | complete)
  if $ide_check.exit_code != 0 {
    error make {
      msg: $"IDE binary not found in PATH: ($ide_binary)"
    }
  }

  if $dry_run {
    {
      ide_binary: $ide_binary
      tag: $tag
      worktree_path: $worktree_path
      merge_branch: $current_branch
      command: $"($ide_binary) ($worktree_path)"
      opened: false
    } | print
    return
  }

  run-external $ide_binary $worktree_path

  {
    ide_binary: $ide_binary
    tag: $tag
    worktree_path: $worktree_path
    merge_branch: $current_branch
    opened: true
  } | print
}
