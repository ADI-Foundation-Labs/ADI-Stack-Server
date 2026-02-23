#!/usr/bin/env nu
# Build and persist step-by-step conflict-resolution state for upstream merges.
#
# This script keeps a compact merge-resolution state file so agent prompts can
# resolve a small deterministic batch instead of all conflicted files at once.
#
# Usage examples:
#   nu .ops/scripts/merge-resolution-state.nu
#   nu .ops/scripts/merge-resolution-state.nu --next --batch-size 3
#   nu .ops/scripts/merge-resolution-state.nu v0.14.2 --next --json
#   nu .ops/scripts/merge-resolution-state.nu --state-file .ops/.tmp/custom-state.yaml

use lib/git/upstream.nu *

def save-lines [items: list<string>, path: string] {
  let content = if (($items | length) == 0) {
    ""
  } else {
    ($items | str join (char nl)) + (char nl)
  }
  let dir = ($path | path dirname)
  ^mkdir -p $dir
  $content | save -f $path
}

def normalize-path-list [raw: list<string>] {
  $raw
  | each { |line| $line | into string | str trim | str replace -a '\' '/' }
  | where { |line| $line != "" }
  | uniq
  | sort
}

def read-lines-if-exists [path: string] {
  if not ($path | path exists) {
    return []
  }
  normalize-path-list (open --raw $path | lines)
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

def load-json-if-exists [path: string] {
  if not ($path | path exists) {
    return null
  }
  let raw = (open --raw $path)
  if (($raw | str trim) == "") {
    return null
  }
  $raw | from json
}

def ensure-worktree [worktree_path: string] {
  if not ($worktree_path | path exists) {
    error make { msg: $"Merge worktree path does not exist: ($worktree_path)" }
  }

  let wt_check = (^git -C $worktree_path rev-parse --is-inside-work-tree | complete)
  if $wt_check.exit_code != 0 {
    error make { msg: $"Path is not a git worktree: ($worktree_path)" }
  }
}

def unresolved-conflicts [worktree_path: string] {
  let run = (^git -C $worktree_path diff --name-only --diff-filter=U | complete)
  if $run.exit_code != 0 {
    let err = ($run.stderr | str trim)
    let out = ($run.stdout | str trim)
    let msg = if $err != "" { $err } else if $out != "" { $out } else { "git diff failed" }
    error make { msg: $"Failed to read unresolved conflicts in ($worktree_path): ($msg)" }
  }
  normalize-path-list ($run.stdout | lines)
}

def normalize-string-list [items: any] {
  if $items == null {
    return []
  }
  if (($items | describe) !~ '^list') {
    return []
  }
  normalize-path-list ($items | each { |x| $x | into string })
}

def regenerate-routing-manifest [
  repo_root: string
  radar_output_dir: string
  conflict_files: list<string>
] {
  let select_script = $"($repo_root)/.agents/skills/upstream-merge-conflicts/scripts/select_merge_guides.nu"
  if not ($select_script | path exists) {
    error make {
      msg: $"Routing script not found: ($select_script). Expected upstream-merge-conflicts skill assets."
    }
  }

  let lists_dir = $"($radar_output_dir)/lists"
  let all_conflicts_file = $"($lists_dir)/merge-state-all-files.txt"
  let routing_path = $"($radar_output_dir)/merge-guide-routing-state.json"

  save-lines $conflict_files $all_conflicts_file

  let run = (^nu $select_script --conflicts-file $all_conflicts_file --output $routing_path | complete)
  if $run.exit_code != 0 {
    let err = ($run.stderr | str trim)
    let out = ($run.stdout | str trim)
    let msg = if $err != "" { $err } else if $out != "" { $out } else { "select_merge_guides failed" }
    error make { msg: $"Failed to generate routing manifest: ($msg)" }
  }

  $routing_path
}

def primary-rule-priority [rule: string] {
  if $rule == "cargo-manifests-and-lockfiles" {
    10
  } else if $rule == "rust-source" {
    20
  } else if $rule == "ci-and-automation-config" {
    30
  } else if $rule == "repository-meta-files" {
    40
  } else if $rule == "docs-and-markdown" {
    50
  } else if $rule == "fallback" {
    99
  } else {
    80
  }
}

def resolve-routing-entry [routing_rows: list<any>, file: string] {
  let fallback_guides = [ "references/filetypes/fallback.md" ]
  let matches = ($routing_rows | where { |row| (($row | get --optional file | default "") | into string) == $file })

  if (($matches | length) == 0) {
    return {
      file: $file
      primary_rule: "fallback"
      rule_ids: [ "fallback" ]
      guides: $fallback_guides
      priority: (primary-rule-priority "fallback")
    }
  }

  let row = ($matches | first)
  let primary_rule = (($row | get --optional primary_rule | default "fallback") | into string)
  let rule_ids = (
    $row
    | get --optional rule_ids
    | default [ $primary_rule ]
    | each { |x| $x | into string }
    | uniq
  )
  let guides = (
    $row
    | get --optional guides
    | default $fallback_guides
    | each { |x| $x | into string }
    | uniq
    | sort
  )

  {
    file: $file
    primary_rule: $primary_rule
    rule_ids: $rule_ids
    guides: $guides
    priority: (primary-rule-priority $primary_rule)
  }
}

def select-next-batch [
  remaining_files: list<string>
  routing_rows: list<any>
  patch_manifest: list<any>
  batch_size: int
] {
  if (($remaining_files | length) == 0) {
    return {
      strategy: "single-primary-rule"
      primary_rule: null
      batch_size_requested: $batch_size
      files: []
      required_guides: []
      routing: []
      patches: []
      remaining_after_batch: []
    }
  }

  let enriched = (
    $remaining_files
    | each { |file| resolve-routing-entry $routing_rows $file }
    | sort-by priority file
  )

  let chosen_rule = ($enriched | first | get primary_rule)
  let chosen = ($enriched | where { |row| ($row | get primary_rule) == $chosen_rule } | first $batch_size)
  let selected_files = ($chosen | each { |row| $row.file })
  let required_guides = (
    $chosen
    | each { |row| $row.guides }
    | flatten
    | uniq
    | sort
  )
  let routing = (
    $chosen
    | each { |row|
      {
        file: $row.file
        primary_rule: $row.primary_rule
        rule_ids: $row.rule_ids
        guides: $row.guides
      }
    }
  )

  let patches = (
    $selected_files
    | each { |file|
      let matches = ($patch_manifest | where { |row| (($row | get --optional file | default "") | into string) == $file })
      if (($matches | length) == 0) {
        {
          file: $file
          adi_patch: null
          upstream_patch: null
        }
      } else {
        let m = ($matches | first)
        {
          file: $file
          adi_patch: ($m | get --optional adi_patch)
          upstream_patch: ($m | get --optional upstream_patch)
        }
      }
    }
  )

  let remaining_after_batch = (
    $remaining_files
    | where { |f| not ($selected_files | any { |s| $s == $f }) }
  )

  {
    strategy: "single-primary-rule"
    primary_rule: $chosen_rule
    batch_size_requested: $batch_size
    files: $selected_files
    required_guides: $required_guides
    routing: $routing
    patches: $patches
    remaining_after_batch: $remaining_after_batch
  }
}

def main [
  tag_input?: string
  --tag (-t): string
  --worktree (-w): string
  --base-ref (-b): string
  --target-ref (-T): string
  --state-file (-s): string
  --batch-size (-n): int = 3
  --next (-N)
  --json (-j)
] {
  if $batch_size < 1 {
    error make { msg: "--batch-size must be >= 1" }
  }

  let status = (read-merge-status)
  let repo_root = (^git rev-parse --show-toplevel | str trim)
  let status_tag = ($status | get --optional tags.current_upstream_tag)

  let raw_tag = if $tag != null {
    $tag
  } else if $tag_input != null {
    $tag_input
  } else {
    $status_tag
  }

  let parsed_tag = if $raw_tag == null { null } else { extract-semver-tag $raw_tag }
  if $parsed_tag == null {
    error make {
      msg: $"Cannot resolve upstream tag. Provide vX.Y.Z explicitly or set tags.current_upstream_tag in .ops/merge-status.yaml."
    }
  }
  let tag_value = $parsed_tag

  let base = if $base_ref != null {
    $base_ref
  } else {
    ($status | get --optional merge.base_ref | default $"upstream/($tag_value)")
  }

  let target = if $target_ref != null {
    $target_ref
  } else {
    ($status | get --optional merge.merge_branch | default "main")
  }

  let worktree_path = if $worktree != null {
    $worktree
  } else {
    ($status | get --optional merge.worktree_path)
  }
  if $worktree_path == null or ($worktree_path | str trim) == "" {
    error make {
      msg: "Cannot resolve merge worktree path. Pass --worktree or set merge.worktree_path in .ops/merge-status.yaml."
    }
  }

  ensure-worktree $worktree_path

  let default_radar_dir = $"($repo_root)/.ops/.tmp/upstream-radar/((slugify-ref $base))__((slugify-ref $target))"
  let radar_output_dir = (
    $status
    | get --optional radar.output_dir
    | default $default_radar_dir
  )
  ^mkdir -p $radar_output_dir

  let resolved_state_file = if $state_file == null {
    $"($radar_output_dir)/merge-resolution-state.yaml"
  } else {
    $state_file
  }

  let existing_state = (load-yaml-if-exists $resolved_state_file)
  let existing_matches = (
    $existing_state != null
    and (($existing_state | get --optional tag) == $tag_value)
    and (($existing_state | get --optional base_ref) == $base)
    and (($existing_state | get --optional target_ref) == $target)
    and (($existing_state | get --optional worktree_path) == $worktree_path)
  )
  let existing_all_files = if $existing_matches {
    normalize-string-list ($existing_state | get --optional all_files | default [])
  } else {
    []
  }

  let unresolved_now = (unresolved-conflicts $worktree_path)
  let status_conflicts = (normalize-string-list ($status | get --optional merge.conflict_files | default []))
  let radar_conflicts_file = $"($radar_output_dir)/lists/conflicts.txt"
  let radar_conflicts = (read-lines-if-exists $radar_conflicts_file)

  let all_files = (
    normalize-path-list ($existing_all_files ++ $status_conflicts ++ $radar_conflicts ++ $unresolved_now)
  )
  let remaining_files = (normalize-path-list $unresolved_now)
  let resolved_files = (
    $all_files
    | where { |f| not ($remaining_files | any { |r| $r == $f }) }
  )

  let routing_manifest = (regenerate-routing-manifest $repo_root $radar_output_dir $all_files)
  let routing_json = (load-json-if-exists $routing_manifest)
  let routing_rows = if $routing_json == null {
    []
  } else {
    $routing_json | get --optional routing | default []
  }

  let patch_manifest_path = $"($radar_output_dir)/conflict-diffs/manifest.json"
  let patch_manifest = if ($patch_manifest_path | path exists) {
    (load-json-if-exists $patch_manifest_path | default [])
  } else {
    []
  }

  let next_batch = (select-next-batch $remaining_files $routing_rows $patch_manifest $batch_size)

  let total = ($all_files | length)
  let remaining = ($remaining_files | length)
  let resolved = ($resolved_files | length)
  let progress_percent = if $total == 0 { 100 } else { (($resolved * 100) / $total) }

  let state = {
    schema_version: 1
    updated_at: (now-iso)
    tag: $tag_value
    base_ref: $base
    target_ref: $target
    worktree_path: $worktree_path
    radar_output_dir: $radar_output_dir
    radar_conflicts_file: (if ($radar_conflicts_file | path exists) { $radar_conflicts_file } else { null })
    routing_manifest: $routing_manifest
    patch_manifest: (if ($patch_manifest_path | path exists) { $patch_manifest_path } else { null })
    all_files: $all_files
    remaining_files: $remaining_files
    resolved_files: $resolved_files
    summary: {
      total_files: $total
      remaining_files: $remaining
      resolved_files: $resolved
      progress_percent: $progress_percent
    }
    next_batch: $next_batch
  }

  let state_dir = ($resolved_state_file | path dirname)
  ^mkdir -p $state_dir
  ($state | to yaml) | save -f $resolved_state_file

  let output = if $next {
    {
      state_file: $resolved_state_file
      tag: $tag_value
      base_ref: $base
      target_ref: $target
      worktree_path: $worktree_path
      radar_output_dir: $radar_output_dir
      summary: ($state | get summary)
      next_batch: ($state | get next_batch)
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
