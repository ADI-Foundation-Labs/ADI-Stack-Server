# Run AI agent command for upstream merge conflict resolution or validation.
#
# Usage:
#   nu .ops/scripts/run-merge-agent.nu v0.14.2
#   nu .ops/scripts/run-merge-agent.nu v0.14.2 codex
#   nu .ops/scripts/run-merge-agent.nu v0.14.2 claude --agent-args -p
#   nu .ops/scripts/run-merge-agent.nu v0.14.2 codex --next-batch --batch-size 3
#   nu .ops/scripts/run-merge-agent.nu --validate v0.14.2
#   task upgrade:agent:resolve -- v0.14.2 [agent] [--agent-args -p]
#   task upgrade:agent:resolve:next -- v0.14.2 [agent] [--batch-size N]
#   task upgrade:agent:validate -- v0.14.2 [agent]

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

def validate-worktree [worktree_path: string, expected_branch: string, tag: string] {
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
  if $current_branch != $expected_branch {
    error make {
      msg: $"Worktree branch mismatch at ($worktree_path): current=($current_branch), expected=($expected_branch)"
    }
  }
}

def resolve-skill-path [worktree_path: string] {
  let skill_relative = ".agents/skills/upstream-merge-conflicts/SKILL.md"
  let skill_in_worktree = $"($worktree_path)/($skill_relative)"
  if ($skill_in_worktree | path exists) {
    return {
      skill_path: $skill_in_worktree
      source: "worktree-absolute"
    }
  }

  let repo_root = (^git rev-parse --show-toplevel | str trim)
  let skill_in_repo_root = $"($repo_root)/($skill_relative)"
  if ($skill_in_repo_root | path exists) {
    return {
      skill_path: $skill_in_repo_root
      source: "repo-root-absolute"
    }
  }

  error make {
    msg: $"Skill file not found at either ($skill_in_worktree) or ($skill_in_repo_root)."
  }
}

def format-bullets [items: list<string>] {
  if (($items | length) == 0) {
    "- <none>"
  } else {
    $items | each { |x| $"- `($x)`" } | str join (char nl)
  }
}

def format-routing-bullets [routing: list<any>] {
  if (($routing | length) == 0) {
    "- <none>"
  } else {
    (
      $routing
      | each { |row|
        let f = ($row | get --optional file | default "<unknown>" | into string)
        let pr = ($row | get --optional primary_rule | default "fallback" | into string)
        $"- `($f)` => `($pr)`"
      }
      | str join (char nl)
    )
  }
}

def format-patch-bullets [patches: list<any>] {
  if (($patches | length) == 0) {
    "- <none>"
  } else {
    (
      $patches
      | each { |row|
        let f = ($row | get --optional file | default "<unknown>" | into string)
        let adi = ($row | get --optional adi_patch | default "<none>" | into string)
        let upstream = ($row | get --optional upstream_patch | default "<none>" | into string)
        $"- `($f)`: adi_patch=`($adi)`, upstream_patch=`($upstream)`"
      }
      | str join (char nl)
    )
  }
}

def build-validate-prompt [skill_path: string, tag: string] {
  $"use skill: ($skill_path)\nValidate upstream merge for tag ($tag) in this worktree. Confirm unresolved conflicts are zero, run merge validation commands from the skill workflow, and report only concrete failures and next fixes."
}

def build-resolve-prompt [skill_path: string, tag: string] {
  $"use skill: ($skill_path)\nResolve upstream merge conflicts for tag ($tag) in this worktree using the reproducible workflow in the skill."
}

def load-next-batch-state [
  repo_root: string
  tag: string
  worktree_path: string
  base_ref: string
  target_ref: string
  batch_size: int
  state_file?: string
] {
  let state_script = $"($repo_root)/.ops/scripts/merge-resolution-state.nu"
  if not ($state_script | path exists) {
    error make {
      msg: $"Merge state script not found: ($state_script)"
    }
  }

  mut args = [
    $state_script
    "--next"
    "--json"
    "--batch-size"
    ($batch_size | into string)
    "--tag"
    $tag
    "--worktree"
    $worktree_path
    "--base-ref"
    $base_ref
    "--target-ref"
    $target_ref
  ]
  if $state_file != null {
    $args = ($args ++ [ "--state-file", $state_file ])
  }

  let run = (^nu ...$args | complete)
  if $run.exit_code != 0 {
    let err = ($run.stderr | str trim)
    let out = ($run.stdout | str trim)
    let msg = if $err != "" { $err } else if $out != "" { $out } else { "merge-resolution-state failed" }
    error make {
      msg: $"Failed to load next batch state: ($msg)"
    }
  }

  let text = ($run.stdout | str trim)
  if $text == "" {
    error make {
      msg: "merge-resolution-state returned empty output."
    }
  }

  $text | from json
}

def build-next-batch-prompt [skill_path: string, tag: string, batch_state: record] {
  let summary = ($batch_state | get --optional summary | default {})
  let next_batch = ($batch_state | get --optional next_batch | default {})
  let files = ($next_batch | get --optional files | default [])
  let guides = ($next_batch | get --optional required_guides | default [])
  let routing = ($next_batch | get --optional routing | default [])
  let patches = ($next_batch | get --optional patches | default [])
  let primary_rule = ($next_batch | get --optional primary_rule | default "fallback")
  let state_file = ($batch_state | get --optional state_file | default "<unknown>")
  let radar_dir = ($batch_state | get --optional radar_output_dir | default "<unknown>")
  let remaining_count = ($summary | get --optional remaining_files | default 0)
  let resolved_count = ($summary | get --optional resolved_files | default 0)
  let total_count = ($summary | get --optional total_files | default 0)

  let file_lines = (format-bullets ($files | each { |x| $x | into string }))
  let guide_lines = (format-bullets ($guides | each { |x| $x | into string }))
  let routing_lines = (format-routing-bullets $routing)
  let patch_lines = (format-patch-bullets $patches)

  [
    $"use skill: ($skill_path)"
    $"Resolve upstream merge conflicts for tag ($tag), but ONLY for this selected batch."
    ""
    "State:"
    $" - state_file: ($state_file)"
    $" - radar_output_dir: ($radar_dir)"
    $" - progress: ($resolved_count)/($total_count) resolved, ($remaining_count) remaining"
    ""
    "Batch constraints:"
    " - Resolve and stage only the files listed under Batch files."
    " - Do not edit files outside this batch unless strictly required to complete a listed file merge."
    " - Stop after this batch."
    " - At the end, report unresolved files from: git diff --name-only --diff-filter=U"
    ""
    "Batch primary rule:"
    $" - ($primary_rule)"
    ""
    "Batch files:"
    $file_lines
    ""
    "Required guide files (load only these):"
    $guide_lines
    ""
    "Routing:"
    $routing_lines
    ""
    "Patch artifacts (if available):"
    $patch_lines
  ] | str join (char nl)
}

def main [
  tag_input: string
  agent_name?: string
  --validate (-v)
  --dry-run (-n)
  --worktree-root (-w): string
  --next-batch (-B)
  --batch-size (-b): int = 3
  --state-file (-s): string
  --agent-args (-A): string = ""
] {
  # Parse --agent-args string into list so task can pass e.g. --agent-args '-p'
  let agent_args = (if ($agent_args | str trim) == "" { [] } else { $agent_args | split row " " | where { |x| ($x | str trim) != "" } })
  if $batch_size < 1 {
    error make { msg: "--batch-size must be >= 1" }
  }
  if ($validate and $next_batch) {
    error make { msg: "--validate and --next-batch cannot be used together" }
  }

  let parsed_tag = (extract-semver-tag $tag_input)
  if $parsed_tag == null {
    error make {
      msg: $"Invalid tag input: ($tag_input). Expected semver tag like v0.14.2"
    }
  }

  let tag = $parsed_tag
  let expected_branch = $"merge/upstream-($tag)"

  let status = (read-merge-status)
  let tracked_tag = ($status | get --optional tags.current_upstream_tag)
  let tracked_worktree = ($status | get --optional merge.worktree_path)
  let tracked_base_ref = ($status | get --optional merge.base_ref)
  let tracked_merge_branch = ($status | get --optional merge.merge_branch)
  let tracked_worktree_valid = ($tracked_worktree != null and ($tracked_worktree | str trim) != "")
  let expected_worktree = (expected-worktree-path $tag $worktree_root)
  let repo_root = (^git rev-parse --show-toplevel | str trim)

  let configs_path = $"($repo_root)/.ops/agent-configs.yaml"
  let configs = if ($configs_path | path exists) { (open $configs_path) } else { {} }
  let default_agent = ($configs | get --optional default | default "claude")
  let target_agent = if $agent_name == null { $default_agent } else { $agent_name }
  let agent_config = ($configs | get --optional agents | get --optional $target_agent)
  let agent_binary = if $agent_config != null { ($agent_config | get binary) } else { $target_agent }
  let config_args = if $agent_config != null { ($agent_config | get --optional args | default [] | each { |x| $x | into string }) } else { [] }
  let final_agent_args = ($config_args ++ $agent_args)

  let worktree_path = if ($tracked_tag == $tag and $tracked_worktree_valid) {
    $tracked_worktree
  } else {
    $expected_worktree
  }
  let base_ref = if ($tracked_tag == $tag and $tracked_base_ref != null and ($tracked_base_ref | str trim) != "") {
    $tracked_base_ref
  } else {
    $"upstream/($tag)"
  }
  let target_ref = if ($tracked_tag == $tag and $tracked_merge_branch != null and ($tracked_merge_branch | str trim) != "") {
    $tracked_merge_branch
  } else {
    $expected_branch
  }

  validate-worktree $worktree_path $expected_branch $tag

  let agent_check = (^which $agent_binary | complete)
  if $agent_check.exit_code != 0 {
    error make {
      msg: $"Agent binary not found in PATH: ($agent_binary)"
    }
  }

  let skill_info = (resolve-skill-path $worktree_path)
  let skill_path = ($skill_info | get skill_path)
  mut prompt = ""
  mut mode = ""
  mut next_batch_info = {}

  if $validate {
    $prompt = (build-validate-prompt $skill_path $tag)
    $mode = "validate"
  } else if $next_batch {
    let batch_state = (
      load-next-batch-state $repo_root $tag $worktree_path $base_ref $target_ref $batch_size $state_file
    )
    let next_batch_state = ($batch_state | get --optional next_batch | default {})
    let next_files = ($next_batch_state | get --optional files | default [])

    if (($next_files | length) == 0) {
      {
        mode: "resolve-next-batch"
        tag: $tag
        agent: $target_agent
        worktree_path: $worktree_path
        message: "No unresolved conflicts remain in merge state."
        summary: ($batch_state | get --optional summary | default {})
        state_file: ($batch_state | get --optional state_file)
      } | print
      return
    }

    $prompt = (build-next-batch-prompt $skill_path $tag $batch_state)
    $mode = "resolve-next-batch"
    $next_batch_info = {
      state_file: ($batch_state | get --optional state_file)
      primary_rule: ($next_batch_state | get --optional primary_rule)
      files: $next_files
      required_guides: ($next_batch_state | get --optional required_guides | default [])
      remaining_after_batch: ($next_batch_state | get --optional remaining_after_batch | default [])
      progress: ($batch_state | get --optional summary | default {})
    }
  } else {
    $prompt = (build-resolve-prompt $skill_path $tag)
    $mode = "resolve"
  }

  if $dry_run {
    let args_preview = (if ($final_agent_args | length) > 0 { $" ...($final_agent_args | str join ' ') " } else { " " })
    {
      mode: $mode
      tag: $tag
      agent: $target_agent
      agent_binary: $agent_binary
      agent_args: $final_agent_args
      worktree_path: $worktree_path
      expected_branch: $expected_branch
      skill_path: $skill_path
      skill_source: ($skill_info | get source)
      base_ref: $base_ref
      target_ref: $target_ref
      next_batch: $next_batch_info
      command_preview: $"cd ($worktree_path) && ($agent_binary)($args_preview)<prompt>"
    } | print
    return
  }

  cd $worktree_path
  exec $agent_binary ...($final_agent_args | append $prompt)
}
