#!/usr/bin/env nu

use lib/git/upstream.nu *

def main [
  agent_name?: string
  --upstream-ref (-u): string = "upstream/main"
  --adi-ref (-a): string = "HEAD"
  --dry-run (-d)
  --agent-args (-A): string = ""
] {
  let parsed_agent_args = (parse-agent-args $agent_args)

  let repo_root = (^git rev-parse --show-toplevel | str trim)
  
  # Fetch upstream to ensure we have the latest
  ^git fetch upstream --tags 2>/dev/null | ignore

  # Find the merge base between ADI ref and upstream ref
  let merge_base_cmd = (^git merge-base $upstream_ref $adi_ref | complete)
  if $merge_base_cmd.exit_code != 0 {
    error make { msg: $"Could not find merge base between ($adi_ref) and ($upstream_ref)" }
  }
  let merge_base = ($merge_base_cmd.stdout | str trim)

  # Get files changed on ADI side since merge base
  let adi_changes = (^git diff --name-only $merge_base $adi_ref | lines | str trim | where { |x| $x != "" })
  # Get files changed on Upstream side since merge base
  let upstream_changes = (^git diff --name-only $merge_base $upstream_ref | lines | str trim | where { |x| $x != "" })

  # Find overlapping files
  let overlap = ($adi_changes | filter { |f| $upstream_changes | any { |u| $u == $f } })

  if ($overlap | length) == 0 {
    print "No overlapping files found between ADI changes and upstream changes. Conflict risk is likely low."
    return
  }

  # For overlapping files, get the ADI diff and Upstream diff to supply as context
  let diff_context = ($overlap | each { |f|
    let adi_file_diff = (^git diff $merge_base $adi_ref -- $f)
    let upstream_file_diff = (^git diff $merge_base $upstream_ref -- $f)
    [
      $"=== File: ($f) ==="
      $"--- ADI Changes since ($merge_base) ---"
      $adi_file_diff
      $"--- Upstream Changes since ($merge_base) ---"
      $upstream_file_diff
      ""
    ] | str join (char nl)
  } | str join (char nl))

  let agent = (resolve-agent-config $repo_root $agent_name $parsed_agent_args)
  let target_agent = $agent.name
  let agent_binary = $agent.binary
  let final_agent_args = $agent.args

  let mode_file = $"($repo_root)/.agents/skills/upstream-merge-conflicts/references/mode-predict-conflicts.md"

  let prompt = [
    $"use skill: ($mode_file)"
    $"Analyze conflict risk for local ref ($adi_ref) against upstream ref ($upstream_ref)."
    ""
    "Overlapping changed files and their respective diffs from the merge base:"
    $diff_context
    ""
    "Predict severe merge conflicts and suggest alternative ADI implementations."
  ] | str join (char nl)

  if $dry_run {
    {
      mode: "predict-conflicts"
      agent: $target_agent
      agent_binary: $agent_binary
      agent_args: $final_agent_args
      upstream_ref: $upstream_ref
      adi_ref: $adi_ref
      merge_base: $merge_base
      overlapping_files: $overlap
      command_preview: $"cd ($repo_root) && ($agent_binary)(agent-args-preview $final_agent_args)<prompt>"
    } | print
    return
  }

  print $"(ansi blue_bold)================================================================================(ansi reset)"
  print $"(ansi cyan_bold)🤖 Starting Agent: Predict Conflicts(ansi reset)"
  print $"(ansi blue_bold)================================================================================(ansi reset)"
  print $"(ansi white_bold)Upstream Ref:(ansi reset) ($upstream_ref)"
  print $"(ansi white_bold)ADI Ref:(ansi reset)      ($adi_ref)"
  print $"(ansi white_bold)Merge Base:(ansi reset)   ($merge_base)"
  print $"(ansi white_bold)Overlapping:(ansi reset)  (($overlap | length))"
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
