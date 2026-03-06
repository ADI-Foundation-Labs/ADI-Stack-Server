#!/usr/bin/env nu
# Run AI agent in mode-specific upstream merge workflows.
#
# Modes:
# - analyze
# - plan-groups
# - resolve-group
# - resolve-human-group
#
# Usage:
#   nu .ops/scripts/run-merge-agent-modes.nu v0.14.2 codex --mode analyze
#   nu .ops/scripts/run-merge-agent-modes.nu v0.14.2 claude --mode analyze --agent-args -p
#   nu .ops/scripts/run-merge-agent-modes.nu v0.14.2 codex --mode plan-groups
#   nu .ops/scripts/run-merge-agent-modes.nu v0.14.2 codex --mode resolve-group --next-group
#   nu .ops/scripts/run-merge-agent-modes.nu v0.14.2 codex --mode resolve-human-group --next-group
#   nu .ops/scripts/run-merge-agent-modes.nu v0.14.2 codex --mode resolve-group --group watcher-core

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

def resolve-mode-file [repo_root: string, mode: string] {
  let rel = if $mode == "analyze" {
    ".agents/skills/upstream-merge-conflicts/references/mode-initial-analyze.md"
  } else if $mode == "plan-groups" {
    ".agents/skills/upstream-merge-conflicts/references/mode-plan-groups.md"
  } else if $mode == "analyze-group" {
    ".agents/skills/upstream-merge-conflicts/references/mode-analyze-group.md"
  } else if $mode == "resolve-group" {
    ".agents/skills/upstream-merge-conflicts/references/mode-resolve-group.md"
  } else if $mode == "resolve-human-group" {
    ".agents/skills/upstream-merge-conflicts/references/mode-resolve-human-group.md"
  } else {
    error make { msg: $"Unsupported mode: ($mode)" }
  }

  let abs = $"($repo_root)/($rel)"
  if not ($abs | path exists) {
    error make {
      msg: $"Mode file not found: ($abs)"
    }
  }
  $abs
}

def run-state-script [repo_root: string, args: list<string>] {
  let state_script = $"($repo_root)/.ops/scripts/merge-agent-state.nu"
  if not ($state_script | path exists) {
    error make { msg: $"State script not found: ($state_script)" }
  }

  let run_args = ([ $state_script ] ++ $args)
  let run = (^nu ...$run_args | complete)
  if $run.exit_code != 0 {
    let err = ($run.stderr | str trim)
    let out = ($run.stdout | str trim)
    let msg = if $err != "" { $err } else if $out != "" { $out } else { "merge-agent-state failed" }
    error make { msg: $"Failed to run merge-agent-state: ($msg)" }
  }

  let text = ($run.stdout | str trim)
  if $text == "" {
    error make { msg: "merge-agent-state returned empty output." }
  }
  $text | from json
}

def build-analyze-prompt [
  mode_file: string
  tag: string
  state_file: string
  base_ref: string
  target_ref: string
] {
  [
    $"use skill: ($mode_file)"
    $"Run MODE initial-analyze for upstream tag ($tag)."
    ""
    $"State file: ($state_file)"
    "Merge refs:"
    $" - base_ref: ($base_ref)"
    $" - target_ref: ($target_ref)"
    ""
    "Analyze unresolved conflicts and update state analysis categories (easy, medium, hard, human_input_needed with reasons)."
    "Ensure every unresolved file is categorized exactly once."
    "Mark status.analysis=completed when finished."
    "Do not resolve conflicts yet."
  ] | str join (char nl)
}

def build-plan-prompt [
  mode_file: string
  tag: string
  state_file: string
] {
  [
    $"use skill: ($mode_file)"
    $"Run MODE plan-groups for upstream tag ($tag)."
    ""
    $"State file: ($state_file)"
    ""
    "Create merge groups from the current analysis with these constraints:"
    "1) Each group has a clear meaning and concise commit intent."
    "2) Keep total difficulty per group bounded; avoid stacking multiple hard files together."
    "3) Every non-human group must be independently committable."
    "4) Split human_input_needed into smaller groups with explicit decision descriptions."
    ""
    "Write groups to state with fields: name, purpose, description, files, difficulty, human_input_needed, status=pending, commit_message, order."
    "Mark status.grouping=completed."
  ] | str join (char nl)
}

def build-analyze-group-prompt [
  mode_file: string
  tag: string
  state_file: string
  group_payload: any
] {
  let group_json = (($group_payload | to json --indent 2) + (char nl))
  let group_name = ($group_payload | get --optional name | default "<unknown>")

  [
    $"use skill: ($mode_file)"
    $"Run MODE analyze-group for upstream tag ($tag)."
    ""
    $"State file: ($state_file)"
    $"Target group: ($group_name)"
    ""
    "Group payload JSON:"
    $group_json
    "Analyze this group using the commit history from both sides."
    "Do not resolve conflicts yet. Propose resolution strategies."
    "Update group state with resolve_options (list of strings)."
    "If exactly one option is provided, set status to 'analyzed'."
    "If multiple options are provided, set status to 'blocked' and human_input_needed to true."
  ] | str join (char nl)
}

def build-resolve-prompt [
  mode_file: string
  tag: string
  state_file: string
  group_payload: any
  human_mode: bool
] {
  let group_json = (($group_payload | to json --indent 2) + (char nl))
  let mode_name = if $human_mode { "resolve-human-group" } else { "resolve-group" }
  let group_name = ($group_payload | get --optional name | default "<unknown>")

  [
    $"use skill: ($mode_file)"
    $"Run MODE ($mode_name) for upstream tag ($tag)."
    ""
    $"State file: ($state_file)"
    $"Target group: ($group_name)"
    ""
    "Group payload JSON:"
    $group_json
    "Resolve only files from this group, stage changes, and create one commit for this group."
    "Use commit_message if present, otherwise create a concise message aligned with the group purpose."
    ""
    "After commit, update group status in state:"
    "- set completed when merged and committed"
    "- set blocked with notes if unresolved decisions remain"
    ""
    "Also update resolution.last_completed_group and resolution.last_commit (HEAD hash)."
  ] | str join (char nl)
}

def main [
  tag_input: string
  agent_name?: string
  --mode (-m): string = "analyze"
  --group (-g): string
  --next-group (-n)
  --human (-H)
  --state-file (-s): string
  --dry-run (-d)
  --worktree-root (-w): string
  --agent-args (-A): string = ""
] {
  # Parse --agent-args string into list so task can pass e.g. --agent-args '-p'
  let agent_args = (parse-agent-args $agent_args)
  let parsed_tag = (extract-semver-tag $tag_input)
  if $parsed_tag == null {
    error make {
      msg: $"Invalid tag input: ($tag_input). Expected semver tag like v0.14.2"
    }
  }

  let mode_value = ($mode | str downcase | str trim)
  let valid_modes = [ "analyze", "plan-groups", "analyze-group", "resolve-group", "resolve-human-group" ]
  if not ($valid_modes | any { |m| $m == $mode_value }) {
    error make { msg: $"Unsupported mode ($mode_value). Expected one of: analyze, plan-groups, analyze-group, resolve-group, resolve-human-group." }
  }

  let require_group = ($mode_value == "resolve-group" or $mode_value == "resolve-human-group" or $mode_value == "analyze-group")
  if ($require_group and $group == null and not $next_group) {
    error make { msg: "Resolve and analyze modes require either --group <name> or --next-group." }
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

  let agent = (resolve-agent-config $repo_root $agent_name $agent_args)
  let target_agent = $agent.name
  let agent_binary = $agent.binary
  let final_agent_args = $agent.args

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

  assert-worktree $worktree_path $expected_branch $tag

  let agent_check = (^which $agent_binary | complete)
  if $agent_check.exit_code != 0 {
    error make { msg: $"Agent binary not found in PATH: ($agent_binary)" }
  }

  let mode_file = (resolve-mode-file $repo_root $mode_value)

  mut init_args = [
    "--init"
    "--json"
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
    $init_args = ($init_args ++ [ "--state-file", $state_file ])
  }
  let state_info = (run-state-script $repo_root $init_args)
  let resolved_state_file = ($state_info | get --optional state_file)

  mut selected_group = null
  if $require_group {
    mut select_args = [
      "--json"
      "--tag"
      $tag
      "--worktree"
      $worktree_path
      "--base-ref"
      $base_ref
      "--target-ref"
      $target_ref
      "--state-file"
      $resolved_state_file
    ]

    if $group != null {
      $select_args = ($select_args ++ [ "--group", $group ])
    } else {
      $select_args = ($select_args ++ [ "--next-group" ])
      if ($mode_value == "analyze-group") {
        $select_args = ($select_args ++ [ "--analyze" ])
      } else if ($mode_value == "resolve-human-group" or $human) {
        $select_args = ($select_args ++ [ "--human" ])
      }
    }

    let selected = (run-state-script $repo_root $select_args)
    $selected_group = ($selected | get --optional selected_group)
    if $selected_group == null {
      {
        mode: $mode_value
        tag: $tag
        state_file: $resolved_state_file
        message: "No matching pending group was found."
        resolution: ($selected | get --optional resolution | default {})
      } | print
      return
    }
  }

  let prompt = if $mode_value == "analyze" {
    build-analyze-prompt $mode_file $tag $resolved_state_file $base_ref $target_ref
  } else if $mode_value == "plan-groups" {
    build-plan-prompt $mode_file $tag $resolved_state_file
  } else if $mode_value == "analyze-group" {
    build-analyze-group-prompt $mode_file $tag $resolved_state_file $selected_group
  } else if $mode_value == "resolve-group" {
    build-resolve-prompt $mode_file $tag $resolved_state_file $selected_group false
  } else {
    build-resolve-prompt $mode_file $tag $resolved_state_file $selected_group true
  }

  if $dry_run {
    {
      mode: $mode_value
      tag: $tag
      agent: $target_agent
      agent_binary: $agent_binary
      agent_args: $final_agent_args
      worktree_path: $worktree_path
      base_ref: $base_ref
      target_ref: $target_ref
      mode_file: $mode_file
      state_file: $resolved_state_file
      selected_group: $selected_group
      command_preview: $"cd ($worktree_path) && ($agent_binary)(agent-args-preview $final_agent_args)<prompt>"
      prompt_content: $prompt
    } | print
    return
  }

  print $"(ansi blue_bold)================================================================================(ansi reset)"
  print $"(ansi cyan_bold)🤖 Starting Upstream Merge Agent(ansi reset)"
  print $"(ansi blue_bold)================================================================================(ansi reset)"
  print $"(ansi white_bold)Mode:(ansi reset)       ($mode_value)"
  print $"(ansi white_bold)Tag:(ansi reset)        ($tag)"
  print $"(ansi white_bold)Worktree:(ansi reset)   ($worktree_path)"
  print $"(ansi white_bold)Agent:(ansi reset)      ($target_agent) \(($agent_binary)\)"
  
  if $selected_group != null {
      let gname = ($selected_group | get --optional name | default "<unknown>")
      print $"(ansi white_bold)Group:(ansi reset)      ($gname)"
  }
  
  print ""
  print $"(ansi cyan_bold)📝 Executing Prompt:(ansi reset)"
  print $"(ansi dark_gray)--------------------------------------------------------------------------------(ansi reset)"
  print $"(ansi dark_gray)($prompt)(ansi reset)"
  print $"(ansi dark_gray)--------------------------------------------------------------------------------(ansi reset)"
  print ""
  print $"(ansi cyan_bold)⏳ Waiting for agent response...(ansi reset)"
  print ""

  cd $worktree_path
  exec $agent_binary ...($final_agent_args | append $prompt)
}
