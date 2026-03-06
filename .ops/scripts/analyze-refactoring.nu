#!/usr/bin/env nu

use lib/git/upstream.nu *

def main [
  tag_input: string
  agent_name?: string
  --dry-run (-d)
  --agent-args (-A): string = ""
] {
  let parsed_agent_args = (parse-agent-args $agent_args)

  let parsed_tag = (extract-semver-tag $tag_input)
  if $parsed_tag == null {
    error make { msg: $"Invalid tag input: ($tag_input). Expected semver tag like v0.14.2" }
  }
  let tag = $parsed_tag

  let status = (read-merge-status)
  let base_ref = ($status | get --optional merge.base_ref | default $"upstream/($tag)")
  let target_ref = ($status | get --optional merge.merge_branch | default $"merge/upstream-($tag)")
  
  let repo_root = (^git rev-parse --show-toplevel | str trim)
  let default_radar_dir = (radar-output-dir $repo_root $base_ref $target_ref)
  let radar_dir = ($status | get --optional radar.output_dir | default $default_radar_dir)

  if not ($radar_dir | path exists) {
    error make { msg: $"Radar output directory not found at ($radar_dir). Run merge and radar first." }
  }

  let state_file = $"($radar_dir)/merge-agent-state.yaml"
  let conflict_files = if ($state_file | path exists) {
    let state = (open --raw $state_file | from yaml)
    $state | get --optional unresolved_files | default []
  } else {
    []
  }

  let radar_report = $"($radar_dir)/report.md"
  let radar_content = if ($radar_report | path exists) {
    open --raw $radar_report
  } else {
    "No radar report found."
  }

  let conflicts_json = ($conflict_files | to json)

  let agent = (resolve-agent-config $repo_root $agent_name $parsed_agent_args)
  let target_agent = $agent.name
  let agent_binary = $agent.binary
  let final_agent_args = $agent.args

  let mode_file = $"($repo_root)/.agents/skills/upstream-merge-conflicts/references/mode-suggest-refactoring.md"

  let prompt = [
    $"use skill: ($mode_file)"
    $"Analyze the resolved merge for tag ($tag)."
    ""
    "Conflicted files from this merge:"
    $conflicts_json
    ""
    "Radar context:"
    $radar_content
    ""
    "Examine ADI and upstream history for these files to suggest architectural refactorings."
  ] | str join (char nl)

  if $dry_run {
    {
      mode: "suggest-refactoring"
      tag: $tag
      agent: $target_agent
      agent_binary: $agent_binary
      agent_args: $final_agent_args
      radar_dir: $radar_dir
      conflict_count: ($conflict_files | length)
      command_preview: $"cd ($repo_root) && ($agent_binary)(agent-args-preview $final_agent_args)<prompt>"
    } | print
    return
  }

  print $"(ansi blue_bold)================================================================================(ansi reset)"
  print $"(ansi cyan_bold)🤖 Starting Agent: Analyze Refactoring(ansi reset)"
  print $"(ansi blue_bold)================================================================================(ansi reset)"
  print $"(ansi white_bold)Tag:(ansi reset)        ($tag)"
  print $"(ansi white_bold)Radar Dir:(ansi reset)  ($radar_dir)"
  print $"(ansi white_bold)Conflicts:(ansi reset)  (($conflict_files | length))"
  print $"(ansi white_bold)Agent:(ansi reset)      ($target_agent) \(($agent_binary)\)"
  print ""
  print $"(ansi cyan_bold)📝 Executing Prompt:(ansi reset)"
  print $"(ansi dark_gray)--------------------------------------------------------------------------------(ansi reset)"
  print $"(ansi dark_gray)($prompt)(ansi reset)"
  print $"(ansi dark_gray)--------------------------------------------------------------------------------(ansi reset)"
  print ""
  print $"(ansi cyan_bold)⏳ Waiting for agent response...(ansi reset)"
  print ""

  cd $repo_root
  exec $agent_binary ...($final_agent_args | append $prompt)
}
