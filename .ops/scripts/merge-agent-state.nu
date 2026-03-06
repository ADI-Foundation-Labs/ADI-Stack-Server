#!/usr/bin/env nu
# Maintain state for mode-based upstream merge conflict resolution.
#
# Usage examples:
#   nu .ops/scripts/merge-agent-state.nu --init
#   nu .ops/scripts/merge-agent-state.nu --init v0.14.2
#   nu .ops/scripts/merge-agent-state.nu --next-group --json
#   nu .ops/scripts/merge-agent-state.nu --next-group --human --json
#   nu .ops/scripts/merge-agent-state.nu --group "watcher-conflicts"

use lib/git/upstream.nu *

def normalize-path-list [raw: list<string>] {
  $raw
  | each { |line| $line | into string | str trim | str replace -a '\' '/' }
  | where { |line| $line != "" }
  | uniq
  | sort
}

def parse-bool [value: any, default: bool] {
  if $value == null {
    return $default
  }
  let t = ($value | describe)
  if ($t | str starts-with "bool") {
    return $value
  }
  let s = ($value | into string | str downcase | str trim)
  if ($s == "true" or $s == "yes" or $s == "1") {
    true
  } else if ($s == "false" or $s == "no" or $s == "0") {
    false
  } else {
    $default
  }
}

def parse-int [value: any, default: int] {
  if $value == null {
    return $default
  }
  let s = ($value | into string | str trim)
  let p = ($s | parse -r '^(?P<n>-?[0-9]+)$')
  if (($p | length) == 0) {
    $default
  } else {
    ($p | get 0.n | into int)
  }
}

def unresolved-conflicts [worktree_path: string] {
  let run = (^git -C $worktree_path diff --name-only --diff-filter=U | complete)
  if $run.exit_code != 0 {
    let err = ($run.stderr | str trim)
    let out = ($run.stdout | str trim)
    let msg = if $err != "" { $err } else if $out != "" { $out } else { "git diff failed" }
    error make {
      msg: $"Failed to read unresolved conflicts in ($worktree_path): ($msg)"
    }
  }
  normalize-path-list ($run.stdout | lines)
}

def load-yaml-if-exists [path: string] {
  if not ($path | path exists) {
    return null
  }
  let raw = (open --raw $path)
  if (($raw | str trim) == "") {
    return null
  }
  $raw | from yaml
}

def normalize-human-needed [items: any] {
  if $items == null {
    return []
  }
  if (($items | describe) !~ '^list') {
    return []
  }

  $items
  | each { |item|
    let t = ($item | describe)
    if ($t | str starts-with "record") {
      {
        file: ($item | get --optional file)
        reason: (($item | get --optional reason | default "unspecified") | into string)
        suggestion: ($item | get --optional suggestion)
      }
    } else {
      {
        file: ($item | into string)
        reason: "unspecified"
        suggestion: null
      }
    }
  }
}

def normalize-groups [groups: any] {
  if $groups == null {
    return []
  }
  if (($groups | describe) !~ '^list') {
    return []
  }

  $groups
  | each { |raw|
    let t = ($raw | describe)
    if not ($t | str starts-with "record") {
      return null
    }

    let name = (($raw | get --optional name | default "") | into string | str trim)
    if $name == "" {
      return null
    }

    let files = (
      normalize-path-list (
        $raw
        | get --optional files
        | default []
        | each { |x| $x | into string }
      )
    )
    let status = (($raw | get --optional status | default "pending") | into string | str downcase | str trim)
    let difficulty = (($raw | get --optional difficulty | default "medium") | into string | str downcase | str trim)
    let human = (
      (parse-bool ($raw | get --optional human_input_needed) false)
      or ($difficulty == "human_input_needed")
    )

    {
      name: $name
      purpose: (($raw | get --optional purpose | default "") | into string)
      description: (($raw | get --optional description | default "") | into string)
      files: $files
      difficulty: $difficulty
      human_input_needed: $human
      status: $status
      commit_message: (($raw | get --optional commit_message | default "") | into string)
      notes: ($raw | get --optional notes | default null)
      order: (parse-int ($raw | get --optional order) 999999)
    }
  }
  | where { |x| $x != null }
}

def compute-resolution-queues [groups: list<any>] {
  let sorted = ($groups | sort-by order name)
  let public_groups = ($sorted | each { |g| $g | reject order })

  let pending_non_human = (
    $sorted
    | where { |g|
      let status = ($g | get status)
      (($status == "pending") or ($status == "ready") or ($status == "in-progress")) and not ($g | get human_input_needed)
    }
  )
  let pending_human = (
    $sorted
    | where { |g|
      let status = ($g | get status)
      (($status == "pending") or ($status == "ready") or ($status == "in-progress")) and ($g | get human_input_needed)
    }
  )
  let completed = ($sorted | where { |g| ($g | get status) == "completed" })
  let blocked = ($sorted | where { |g| ($g | get status) == "blocked" })

  {
    groups: $public_groups
    pending_groups: ($pending_non_human | each { |g| $g.name })
    pending_human_groups: ($pending_human | each { |g| $g.name })
    completed_groups: ($completed | each { |g| $g.name })
    blocked_groups: ($blocked | each { |g| $g.name })
    next_group: (if (($pending_non_human | length) > 0) { $pending_non_human | first | get name } else { null })
    next_human_group: (if (($pending_human | length) > 0) { $pending_human | first | get name } else { null })
  }
}

def default-analysis [unresolved: list<string>] {
  {
    generated_at: null
    easy: []
    medium: []
    hard: []
    human_input_needed: (
      $unresolved
      | each { |f|
        {
          file: $f
          reason: "untriaged"
          suggestion: null
        }
      }
    )
    notes: ""
  }
}

def default-status [] {
  {
    analysis: "pending"
    grouping: "pending"
  }
}

def build-state [
  existing: any
  tag: string
  base_ref: string
  target_ref: string
  worktree_path: string
  radar_output_dir: string
  unresolved: list<string>
] {
  let existing_status = if $existing == null { null } else { $existing | get --optional status }
  let status = if $existing_status == null {
    default-status
  } else {
    {
      analysis: (($existing_status | get --optional analysis | default "pending") | into string)
      grouping: (($existing_status | get --optional grouping | default "pending") | into string)
    }
  }

  let existing_analysis = if $existing == null { null } else { $existing | get --optional analysis }
  let analysis = if $existing_analysis == null {
    default-analysis $unresolved
  } else {
    {
      generated_at: ($existing_analysis | get --optional generated_at)
      easy: (normalize-path-list (($existing_analysis | get --optional easy | default []) | each { |x| $x | into string }))
      medium: (normalize-path-list (($existing_analysis | get --optional medium | default []) | each { |x| $x | into string }))
      hard: (normalize-path-list (($existing_analysis | get --optional hard | default []) | each { |x| $x | into string }))
      human_input_needed: (normalize-human-needed ($existing_analysis | get --optional human_input_needed | default []))
      notes: (($existing_analysis | get --optional notes | default "") | into string)
    }
  }

  let existing_groups = if $existing == null { [] } else { $existing | get --optional groups | default [] }
  let normalized_groups = (normalize-groups $existing_groups)
  let queues = (compute-resolution-queues $normalized_groups)

  let existing_resolution = if $existing == null { null } else { $existing | get --optional resolution }
  let last_completed_group = if $existing_resolution == null { null } else { $existing_resolution | get --optional last_completed_group }
  let last_commit = if $existing_resolution == null { null } else { $existing_resolution | get --optional last_commit }

  {
    schema_version: 1
    updated_at: (now-iso)
    tag: $tag
    base_ref: $base_ref
    target_ref: $target_ref
    worktree_path: $worktree_path
    radar_output_dir: $radar_output_dir
    unresolved_files: $unresolved
    status: $status
    analysis: $analysis
    groups: ($queues | get groups)
    resolution: {
      pending_groups: ($queues | get pending_groups)
      pending_human_groups: ($queues | get pending_human_groups)
      completed_groups: ($queues | get completed_groups)
      blocked_groups: ($queues | get blocked_groups)
      next_group: ($queues | get next_group)
      next_human_group: ($queues | get next_human_group)
      last_completed_group: $last_completed_group
      last_commit: $last_commit
    }
  }
}

def main [
  tag_input?: string
  --tag (-t): string
  --worktree (-w): string
  --base-ref (-b): string
  --target-ref (-T): string
  --state-file (-s): string
  --init (-i)
  --next-group (-n)
  --human (-H)
  --analyze (-a)
  --group (-g): string
  --json (-j)
] {
  let status = (read-merge-status)
  let status_tag = ($status | get --optional tags.current_upstream_tag)
  let raw_tag = if $tag != null { $tag } else if $tag_input != null { $tag_input } else { $status_tag }
  let parsed_tag = if $raw_tag == null { null } else { extract-semver-tag $raw_tag }
  if $parsed_tag == null {
    error make {
      msg: "Cannot resolve tag. Pass vX.Y.Z or set tags.current_upstream_tag in .ops/merge-status.yaml."
    }
  }
  let tag_value = $parsed_tag
  let expected_branch = $"merge/upstream-($tag_value)"

  let tracked_tag = ($status | get --optional tags.current_upstream_tag)
  let default_base = if ($tracked_tag == $tag_value) {
    $status | get --optional merge.base_ref | default $"upstream/($tag_value)"
  } else {
    $"upstream/($tag_value)"
  }
  let default_target = if ($tracked_tag == $tag_value) {
    $status | get --optional merge.merge_branch | default $expected_branch
  } else {
    $expected_branch
  }
  let default_worktree = if ($tracked_tag == $tag_value) {
    $status | get --optional merge.worktree_path
  } else {
    null
  }

  let base_ref = if $base_ref == null { $default_base } else { $base_ref }
  let target_ref = if $target_ref == null { $default_target } else { $target_ref }
  let worktree_path = if $worktree == null { $default_worktree } else { $worktree }
  if $worktree_path == null or ($worktree_path | str trim) == "" {
    error make {
      msg: "Cannot resolve merge worktree path. Pass --worktree or ensure merge.worktree_path is set."
    }
  }
  assert-worktree $worktree_path $expected_branch $tag_value

  let repo_root = (^git rev-parse --show-toplevel | str trim)
  let default_radar_dir = (radar-output-dir $repo_root $base_ref $target_ref)
  let radar_output_dir = ($status | get --optional radar.output_dir | default $default_radar_dir)
  ^mkdir -p $radar_output_dir

  let resolved_state_file = if $state_file == null {
    $"($radar_output_dir)/merge-agent-state.yaml"
  } else {
    $state_file
  }
  let existing_state = (load-yaml-if-exists $resolved_state_file)

  let unresolved = (unresolved-conflicts $worktree_path)
  let state = (build-state $existing_state $tag_value $base_ref $target_ref $worktree_path $radar_output_dir $unresolved)

  # Always persist refreshed unresolved/queues to keep state current.
  let state_dir = ($resolved_state_file | path dirname)
  ^mkdir -p $state_dir
  ($state | to yaml) | save -f $resolved_state_file

  let groups = ($state | get groups)
  let selected_group = if $group != null {
    let matches = ($groups | where { |g| ($g | get name) == $group })
    if (($matches | length) > 0) { $matches | first } else { null }
  } else if $next_group {
    let next_name = if $analyze {
      let pending = ($groups | where { |g| ($g | get status) == "pending" })
      if (($pending | length) > 0) { $pending | first | get name } else { null }
    } else if $human {
      $state | get resolution.next_human_group
    } else {
      $state | get resolution.next_group
    }
    if $next_name == null {
      null
    } else {
      let matches = ($groups | where { |g| ($g | get name) == $next_name })
      if (($matches | length) > 0) { $matches | first } else { null }
    }
  } else {
    null
  }

  let output = if ($group != null or $next_group) {
    {
      state_file: $resolved_state_file
      tag: $tag_value
      base_ref: $base_ref
      target_ref: $target_ref
      worktree_path: $worktree_path
      unresolved_files: ($state | get unresolved_files)
      status: ($state | get status)
      resolution: ($state | get resolution)
      selected_group: $selected_group
    }
  } else {
    ($state | upsert state_file $resolved_state_file)
  }

  if $json {
    (($output | to json --indent 2) + (char nl)) | print -n
  } else {
    $output | table -e | print
  }
}
