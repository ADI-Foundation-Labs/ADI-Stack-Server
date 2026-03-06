# Git upstream helpers for matter-labs/zksync-os-server style repos.
# Use with: use .ops/nushell/git/upstream.nu *

def parse-semver-tag [ref: string] {
  # refs/tags/v1.2.3 or refs/tags/v1.2.3^{} -> v1.2.3 (semver only)
  let s = ($ref | str replace 'refs/tags/' '' | str replace '^{}' '')
  let parsed = ($s | parse -r '^v(?P<maj>[0-9]+)\.(?P<min>[0-9]+)\.(?P<patch>[0-9]+)$')
  if ($parsed | length) >= 1 { $s } else { null }
}

# List semver release tags (vX.Y.Z) from remote 'upstream'.
export def upstream-tags [] {
  ^git ls-remote --tags upstream
  | lines
  | each { |line|
    let parts = ($line | split row "\t")
    let ref = ($parts | get --optional 1)
    if $ref != null { parse-semver-tag $ref } else { null }
  }
  | where { |x| $x != null }
  | uniq
  | sort
}

# List branch names under refs/heads/upstream/* on remote 'origin' (e.g. v0.12.1 for upstream/v0.12.1).
export def origin-upstream-branches [] {
  ^git ls-remote origin
  | lines
  | each { |line|
    let parts = ($line | split row "\t")
    let ref = ($parts | get --optional 1)
    if $ref != null and ($ref | str starts-with 'refs/heads/upstream/') {
      $ref | str replace 'refs/heads/upstream/' ''
    } else { null }
  }
  | where { |x| $x != null }
  | uniq
}

# Check that a tag (e.g. v1.2.3) exists on remote 'upstream'.
export def upstream-has-tag [tag: string] {
  let ref = $"refs/tags/($tag)"
  (^git ls-remote --exit-code upstream $ref | complete).exit_code == 0
}

# Project name from repo root (basename of git toplevel). Used for worktree path.
export def git-project-name [] {
  let root = (^git rev-parse --show-toplevel | str trim)
  $root | split row (char path_sep) | last
}

# RFC3339-ish timestamp with timezone offset.
export def now-iso [] {
  date now | format date "%Y-%m-%dT%H:%M:%S%:z"
}

# Extract semver tag (vX.Y.Z) from arbitrary text/ref.
export def extract-semver-tag [text: string] {
  let parsed = ($text | parse -r '(?P<tag>v[0-9]+\.[0-9]+\.[0-9]+)')
  if (($parsed | length) > 0) { $parsed | get 0.tag } else { null }
}

# Path to merge status file tracked in repo.
export def merge-status-path [] {
  let root = (^git rev-parse --show-toplevel | str trim)
  $"($root)/.ops/merge-status.yaml"
}

export def default-merge-status [] {
  {
    schema_version: 1
    project: (git-project-name)
    updated_at: null
    tags: {
      previous_upstream_tag: null
      current_upstream_tag: null
    }
    merge: {
      status: "idle"
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
    radar: {
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
  }
}

export def read-merge-status [] {
  let path = (merge-status-path)
  if ($path | path exists) {
    let raw = (open --raw $path)
    if (($raw | str trim) == "") {
      default-merge-status
    } else {
      $raw | from yaml
    }
  } else {
    default-merge-status
  }
}

# Write status file and return its path.
export def write-merge-status [status: record] {
  let path = (merge-status-path)
  let normalized = (
    $status
    | upsert schema_version 1
    | upsert project (git-project-name)
    | upsert updated_at (now-iso)
  )
  let dir = ($path | path dirname)
  ^mkdir -p $dir
  ($normalized | to yaml) | save -f $path
  $path
}

export def is-active-merge-status [status: string] {
  let active = [
    "merge_attempted"
    "analysis_only"
    "local_testing_passed"
    "ci_passed"
    "merged_to_main"
  ]
  ($active | any { |s| $s == $status })
}

# Parse agent args string into list, splitting on spaces.
export def parse-agent-args [raw: string] {
  if ($raw | str trim) == "" { [] } else { $raw | split row " " | where { |x| ($x | str trim) != "" } }
}

# Resolve agent binary and args from .ops/agent-configs.yaml.
# Returns: { name: string, binary: string, args: list<string> }
export def resolve-agent-config [
  repo_root: string
  agent_name?: string
  extra_args?: list<string>
] {
  let extra = ($extra_args | default [])
  let configs_path = $"($repo_root)/.ops/agent-configs.yaml"
  let configs = if ($configs_path | path exists) { (open $configs_path) } else { {} }
  let default_agent = ($configs | get --optional default | default "claude")
  let target_agent = if $agent_name == null { $default_agent } else { $agent_name }
  let agent_config = ($configs | get --optional agents | get --optional $target_agent)
  let binary = if $agent_config != null { ($agent_config | get binary) } else { $target_agent }
  let config_args = if $agent_config != null {
    ($agent_config | get --optional args | default [] | each { |x| $x | into string })
  } else { [] }
  { name: $target_agent, binary: $binary, args: ($config_args ++ $extra) }
}

# Compute the radar output directory path from base and target refs.
export def radar-output-dir [repo_root: string, base_ref: string, target_ref: string] {
  $"($repo_root)/.ops/.tmp/upstream-radar/((slugify-ref $base_ref))__((slugify-ref $target_ref))"
}

# Validate that a worktree exists, is a git worktree, and is on the expected branch.
export def assert-worktree [worktree_path: string, expected_branch: string, tag: string] {
  if not ($worktree_path | path exists) {
    error make {
      msg: $"Merge worktree path does not exist: ($worktree_path). Start merge first: task upgrade:start:merge -- ($tag)"
    }
  }

  let wt_check = (^git -C $worktree_path rev-parse --is-inside-work-tree | complete)
  if $wt_check.exit_code != 0 {
    error make { msg: $"Path is not a git worktree: ($worktree_path)" }
  }

  let current_branch = (^git -C $worktree_path rev-parse --abbrev-ref HEAD | str trim)
  if $current_branch != $expected_branch {
    error make {
      msg: $"Worktree branch mismatch at ($worktree_path): current=($current_branch), expected=($expected_branch)"
    }
  }
}

# Format agent args for dry-run command preview.
export def agent-args-preview [final_agent_args: list<string>] {
  if ($final_agent_args | length) > 0 { $" ...($final_agent_args | str join ' ') " } else { " " }
}

export def has-active-merge [] {
  let status = (read-merge-status)
  let m = ($status | get --optional merge.status | default "idle")
  is-active-merge-status $m
}

export def assert-no-active-merge [
  requested_tag?: string
  --force (-f)
] {
  if $force { return }

  if not (has-active-merge) { return }

  let st = (read-merge-status)
  let active_tag = ($st | get --optional tags.current_upstream_tag)
  let merge_branch = ($st | get --optional merge.merge_branch | default "<unknown>")
  let worktree = ($st | get --optional merge.worktree_path | default "<unknown>")
  let phase = ($st | get --optional merge.status | default "<unknown>")

  let same_tag = ($requested_tag != null and $active_tag != null and $requested_tag == $active_tag)
  let suffix = if $same_tag {
    "Active merge for the same tag already exists."
  } else {
    "Finish/abort the active merge first, or rerun with --force."
  }

  error make {
    msg: $"Active merge detected: tag=($active_tag), status=($phase), branch=($merge_branch), worktree=($worktree). ($suffix)"
  }
}

export def assert-active-merge [] {
  if (has-active-merge) { return }
  error make {
    msg: "No active merge detected in .ops/merge-status.yaml."
  }
}

export def git-tag-exists [tag: string] {
  ((^git rev-parse --verify $"refs/tags/($tag)" | complete).exit_code == 0)
}

export def next-b-release-tag [upstream_tag: string] {
  let candidates = (
    ^git tag --list $"($upstream_tag)-b*"
    | lines
    | where { |t| ($t | str trim) != "" }
  )

  let nums = (
    $candidates
    | each { |t|
      if $t == $"($upstream_tag)-b" {
        1
      } else {
        let p = ($t | parse -r $"^($upstream_tag)-b(?P<n>[0-9]+)$")
        if (($p | length) > 0) {
          (($p | get 0.n) | into int)
        } else {
          null
        }
      }
    }
    | where { |x| $x != null }
  )

  let max_n = if (($nums | length) == 0) { 0 } else { $nums | math max }
  let next_n = ($max_n + 1)
  $"($upstream_tag)-b($next_n)"
}

export def update-merge-status-from-radar [
  base_ref: string
  target_ref: string
  radar_summary: record
] {
  let status = (read-merge-status)

  let current_tag = ($status | get --optional tags.current_upstream_tag)
  let previous_tag = ($status | get --optional tags.previous_upstream_tag)
  let parsed_tag = (extract-semver-tag $base_ref)

  let new_tags = if $parsed_tag != null {
    let next_previous = if ($current_tag != null and $current_tag != $parsed_tag) {
      $current_tag
    } else {
      $previous_tag
    }
    {
      previous_upstream_tag: $next_previous
      current_upstream_tag: $parsed_tag
    }
  } else {
    ($status | get --optional tags | default { previous_upstream_tag: null, current_upstream_tag: null })
  }

  let merge_existing = ($status | get --optional merge | default {})
  let existing_status = ($merge_existing | get --optional status | default "idle")
  let merge_status = if ($existing_status == "idle") {
    "analysis_only"
  } else {
    $existing_status
  }
  let merge_state = (
    $merge_existing
    | upsert status $merge_status
    | upsert base_ref $base_ref
    | upsert target_ref $target_ref
    | upsert last_radar_at (now-iso)
  )

  let radar_state = {
    risk_level: ($radar_summary | get risk_level)
    conflict_source: ($radar_summary | get conflict_source)
    target_only_commits: ($radar_summary | get target_only_commits)
    base_only_commits: ($radar_summary | get base_only_commits)
    adi_changed_files: ($radar_summary | get adi_changed_files)
    upstream_changed_files: ($radar_summary | get upstream_changed_files)
    overlap_files: ($radar_summary | get overlap_files)
    predicted_conflict_files: ($radar_summary | get predicted_conflict_files)
    output_dir: ($radar_summary | get output_dir)
    report: ($radar_summary | get report)
  }

  let next = (
    $status
    | upsert tags $new_tags
    | upsert merge $merge_state
    | upsert radar $radar_state
  )
  write-merge-status $next
}

# Check that a ref exists and resolves to a commit.
export def git-ref-exists [ref: string] {
  ((^git rev-parse --verify $"($ref)^{commit}" | complete).exit_code == 0)
}

# Get merge-base hash between two refs.
export def merge-base-ref [base_ref: string, target_ref: string] {
  ^git merge-base $base_ref $target_ref | str trim
}

# List changed files between two refs (inclusive commit range semantics of git diff ref..ref).
export def changed-files-between [from_ref: string, to_ref: string] {
  ^git diff --name-only $"($from_ref)..($to_ref)"
  | lines
  | where { |l| ($l | str trim) != "" }
  | sort
  | uniq
}

# List files added in target compared to base.
export def added-files-between [base_ref: string, target_ref: string] {
  ^git diff --name-only --diff-filter=A $base_ref $target_ref
  | lines
  | where { |l| ($l | str trim) != "" }
  | sort
  | uniq
}

# Intersection of two file lists.
export def intersect-files [a: list<string>, b: list<string>] {
  $a | where { |x| $b | any { |y| $y == $x } }
}

def parse-merge-tree-conflict-line [line: string] {
  let p1 = ($line | parse -r '^CONFLICT \([^)]*\): .* in (?P<path>.*)$')
  if (($p1 | length) > 0) {
    return ($p1 | get 0.path)
  }

  let p2 = ($line | parse -r '^CONFLICT \([^)]*\): .* of (?P<path>.*) left in tree$')
  if (($p2 | length) > 0) {
    return ($p2 | get 0.path)
  }

  null
}

# Predict conflict files using git merge-tree.
# Returns: { source, files, log, exit_code }
export def predict-conflict-files [base_ref: string, target_ref: string] {
  let run = (^git merge-tree $base_ref $target_ref | complete)
  let log_text = ($run.stdout + (if ($run.stderr | is-empty) { "" } else { (char nl) + $run.stderr }))

  if $run.exit_code == 0 {
    let files = (
      $log_text
      | lines
      | each { |line| parse-merge-tree-conflict-line $line }
      | where { |x| $x != null and $x != "" }
      | sort
      | uniq
    )

    {
      source: "merge-tree"
      files: $files
      log: $log_text
      exit_code: $run.exit_code
    }
  } else {
    {
      source: "merge-tree-failed"
      files: []
      log: $log_text
      exit_code: $run.exit_code
    }
  }
}

# Convert a ref into a filesystem-safe slug.
export def slugify-ref [ref: string] {
  $ref
  | str replace -a "/" "_"
  | str replace -a ":" "_"
  | str replace -ra '[^A-Za-z0-9_.-]' '_'
}
