#!/usr/bin/env nu
# Determines the next recommended step in the upgrade flow and prints a visual map.

use lib/git/upstream.nu *

def get-agent-state [radar_dir: any, base_ref: any, target_ref: any] {
    let actual_dir = if $radar_dir != null {
        $radar_dir
    } else if $base_ref != null and $target_ref != null {
        let repo_root = (^git rev-parse --show-toplevel | str trim)
        radar-output-dir $repo_root $base_ref $target_ref
    } else {
        return null
    }
    
    let state_file = $"($actual_dir)/merge-agent-state.yaml"
    if ($state_file | path exists) {
        let raw = (open --raw $state_file)
        if ($raw | str trim) != "" {
            $raw | from yaml
        } else {
            null
        }
    } else {
        null
    }
}

def main [tag?: string] {
    let status = (read-merge-status)
    let merge_status = ($status | get --optional merge.status | default "idle")
    
    let radar_dir = ($status | get --optional radar.output_dir)
    let current_tag = if $tag != null { $tag } else if ($status | get --optional tags.current_upstream_tag) != null { $status | get tags.current_upstream_tag } else { "<tag>" }
    let base_ref = ($status | get --optional merge.base_ref | default $"upstream/($current_tag)")
    let target_ref = ($status | get --optional merge.merge_branch | default $"merge/upstream-($current_tag)")
    let agent_state = get-agent-state $radar_dir $base_ref $target_ref

    let t = if $current_tag == "<tag>" { "" } else { $" -- ($current_tag)" }

    mut step = "Init"
    mut cmd = "task upgrade:start:merge -- <tag>"
    
    if $merge_status == "idle" or $merge_status == "released" or $merge_status == "aborted" {
        $step = "Init"
        $cmd = $"task upgrade:start:merge($t)"
    } else if $merge_status == "merge_attempted" or $merge_status == "analysis_only" {
        if $agent_state == null {
            $step = "Analyze"
            $cmd = $"task upgrade:agent:mode:analyze($t)"
        } else {
            let analysis_status = ($agent_state | get --optional status.analysis | default "pending")
            let grouping_status = ($agent_state | get --optional status.grouping | default "pending")
            let pending_groups = ($agent_state | get --optional resolution.pending_groups | default [])
            let pending_human = ($agent_state | get --optional resolution.pending_human_groups | default [])
            
            if $analysis_status != "completed" {
                $step = "Analyze"
                $cmd = $"task upgrade:agent:mode:analyze($t)"
            } else if $grouping_status != "completed" {
                $step = "Plan"
                $cmd = $"task upgrade:agent:mode:plan-groups($t)"
            } else if (($pending_groups | length) > 0) {
                $step = "Resolve Auto"
                $cmd = $"task upgrade:agent:mode:analyze-next-group($t)\nThen: task upgrade:agent:mode:resolve-next($t)"
            } else if (($pending_human | length) > 0) {
                $step = "Resolve Human"
                $cmd = $"task upgrade:agent:mode:resolve-next-human($t)"
            } else if (($status | get --optional merge.has_conflicts | default false) == true) {
                $step = "Plan"
                $cmd = $"task upgrade:agent:mode:plan-groups($t)\nThen: \(Agent state shows 0 pending groups, but git conflicts remain\)"
            } else {
                $step = "Validate"
                $cmd = $"cargo fmt --all --check && cargo clippy --workspace && cargo nextest run --workspace\nThen: task upgrade:status:transition -- local-tested"
            }
        }
    } else if $merge_status == "local_testing_passed" {
        $step = "PR"
        $cmd = "Open PR to main -> review -> task upgrade:status:transition -- ci-passed"
    } else if $merge_status == "ci_passed" {
        $step = "PR"
        $cmd = "Merge PR to main -> task upgrade:status:transition -- merged-main"
    } else if $merge_status == "merged_to_main" {
        $step = "Release"
        $cmd = $"task upgrade:release:next-tag($t)\nThen tag it and: task upgrade:status:transition -- released"
    } else {
        $step = "Init"
        $cmd = $"task upgrade:start:merge($t)"
    }

    let steps = [
        "Init",
        "Analyze",
        "Plan",
        "Resolve Auto",
        "Resolve Human",
        "Validate",
        "PR",
        "Release"
    ]

    let current_step = $step
    let styled_steps = ($steps | each { |s|
        if $s == $current_step {
            $"(ansi yellow_bold)[*] ($s)(ansi reset)"
        } else {
            $"(ansi dark_gray)[ ] ($s)(ansi reset)"
        }
    })

    let flow = ($styled_steps | str join " ➔ ")
    
    print ""
    print $flow
    print ""
    print $"(ansi light_cyan_bold)💡 Next recommended step:(ansi reset)"
    print $"(ansi white)($cmd)(ansi reset)"
    print ""
}
