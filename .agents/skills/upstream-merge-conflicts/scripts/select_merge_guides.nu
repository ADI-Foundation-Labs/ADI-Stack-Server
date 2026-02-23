#!/usr/bin/env nu
# Select file-type merge guides for unresolved upstream merge conflicts.
#
# Usage examples:
#   nu .agents/skills/upstream-merge-conflicts/scripts/select_merge_guides.nu --worktree ~/.local/git/wortrees/ADI-Stack-Server/merge-upstream-v0.14.2
#   nu .agents/skills/upstream-merge-conflicts/scripts/select_merge_guides.nu --conflicts-file .ops/.tmp/upstream-radar/upstream_v0.14.2__merge_upstream-v0.14.2/lists/conflicts.txt --guides-only

def normalize-path-list [raw: list<string>] {
  $raw
  | each { |line| $line | str trim | str replace -a '\' '/' }
  | where { |line| $line != "" }
  | uniq
  | sort
}

def escape-regex [text: string] {
  $text
  | str replace -a '\' '\\\\'
  | str replace -a '.' '\.'
  | str replace -a '+' '\+'
  | str replace -a '(' '\('
  | str replace -a ')' '\)'
  | str replace -a '[' '\['
  | str replace -a ']' '\]'
  | str replace -a '{' '\{'
  | str replace -a '}' '\}'
  | str replace -a '^' '\^'
  | str replace -a '$' '\$'
  | str replace -a '|' '\|'
}

def glob-to-regex [pattern: string] {
  let star_token = "__CODEx_STAR__"
  let q_token = "__CODEx_Q__"
  let p0 = (
    $pattern
    | str replace -a '*' $star_token
    | str replace -a '?' $q_token
  )
  let escaped = (escape-regex $p0)
  let with_wildcards = (
    $escaped
    | str replace -a $star_token '.*'
    | str replace -a $q_token '.'
  )
  $"^($with_wildcards)$"
}

def matches-pattern [path: string, pattern: string] {
  let regex = (glob-to-regex $pattern)
  ($path =~ $regex)
}

def file-rule-match [path: string, patterns: list<string>] {
  $patterns
  | any { |pattern| matches-pattern $path $pattern }
}

def load-rules [rules_path: string] {
  if not ($rules_path | path exists) {
    error make {
      msg: $"rules file not found: ($rules_path)"
    }
  }

  let data = (open --raw $rules_path | from json)
  let rules = ($data | get --optional rules)
  let defaults = ($data | get --optional default_guides)

  if ($rules == null or (($rules | describe) !~ '^(list|table)')) {
    error make { msg: "rules JSON must include list field `rules`" }
  }
  if ($defaults == null or (($defaults | describe) !~ '^(list|table)')) {
    error make { msg: "rules JSON must include list field `default_guides`" }
  }

  {
    rules: $rules
    default_guides: ($defaults | each { |x| $x | into string })
  }
}

def read-conflicts-from-file [conflicts_file: string] {
  if not ($conflicts_file | path exists) {
    error make { msg: $"conflicts file not found: ($conflicts_file)" }
  }
  normalize-path-list (open --raw $conflicts_file | lines)
}

def read-conflicts-from-worktree [worktree: string] {
  let run = (^git -C $worktree diff --name-only --diff-filter=U | complete)
  if $run.exit_code != 0 {
    let err = ($run.stderr | str trim)
    let out = ($run.stdout | str trim)
    let msg = if $err != "" { $err } else if $out != "" { $out } else { "git diff failed" }
    error make { msg: $msg }
  }
  normalize-path-list ($run.stdout | lines)
}

def merge-unique [base: list<string>, extra: list<string>] {
  ($base ++ $extra) | uniq
}

def build-routing [conflict_files: list<string>, rules: list<any>, default_guides: list<string>] {
  mut required_guides = []
  mut routing = []
  mut counts = {}

  for file in $conflict_files {
    mut matched_rule_ids = []
    mut matched_guides = []

    for rule in $rules {
      let rid = ($rule | get --optional id)
      let patterns = ($rule | get --optional patterns | default [])
      let guides = ($rule | get --optional guides | default [])

      if ($rid == null) {
        error make { msg: "each rule must include string field `id`" }
      }
      if (($patterns | describe) !~ '^list') {
        error make { msg: $"rule `($rid)` must include list field `patterns`" }
      }
      if (($guides | describe) !~ '^list') {
        error make { msg: $"rule `($rid)` must include list field `guides`" }
      }

      if (file-rule-match $file ($patterns | each { |p| $p | into string })) {
        $matched_rule_ids = ($matched_rule_ids ++ [($rid | into string)])
        $matched_guides = (merge-unique $matched_guides ($guides | each { |g| $g | into string }))
      }
    }

    let primary_rule = if (($matched_rule_ids | length) == 0) {
      $matched_rule_ids = [ "fallback" ]
      $matched_guides = $default_guides
      "fallback"
    } else {
      $matched_rule_ids | first
    }

    $required_guides = (merge-unique $required_guides $matched_guides)

    let previous_count = ($counts | get --optional $primary_rule | default 0)
    $counts = ($counts | upsert $primary_rule ($previous_count + 1))

    $routing = (
      $routing
      ++ [
        {
          file: $file
          primary_rule: $primary_rule
          rule_ids: $matched_rule_ids
          guides: $matched_guides
        }
      ]
    )
  }

  {
    conflict_files: $conflict_files
    required_guides: $required_guides
    routing: $routing
    counts_by_primary_rule: $counts
  }
}

def main [
  --conflicts-file (-c): string
  --worktree (-w): string
  --rules (-r): string
  --output (-o): string
  --guides-only (-g)
] {
  let repo_root = (^git rev-parse --show-toplevel | str trim)
  let default_rules = $"($repo_root)/.agents/skills/upstream-merge-conflicts/references/file-type-routing.json"
  let rules_path = if $rules == null { $default_rules } else { $rules }
  let loaded = (load-rules $rules_path)
  let default_worktree = "."

  let conflict_files = if $conflicts_file != null {
    read-conflicts-from-file $conflicts_file
  } else {
    let wt = if $worktree == null { $default_worktree } else { $worktree }
    read-conflicts-from-worktree $wt
  }

  let result = (build-routing $conflict_files ($loaded | get rules) ($loaded | get default_guides))
  let result_json = ($result | to json --indent 2) + (char nl)

  if $output != null {
    let out_dir = ($output | path dirname)
    ^mkdir -p $out_dir
    $result_json | save -f $output
  }

  if $guides_only {
    if (($result.required_guides | length) > 0) {
      (($result.required_guides | str join (char nl)) + (char nl)) | print -n
    }
  } else {
    $result_json | print -n
  }
}
