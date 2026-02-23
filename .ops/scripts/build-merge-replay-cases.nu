# Build replay test cases from release lineage mapping.
#
# Default behavior:
# - include only ADI-style merge releases (`vX.Y.Z-b*`)
# - for each case:
#   - start from `pre_tag_commit`
#   - merge mapped upstream commit
#   - compare resulting tree with expected tagged commit tree
#
# Usage:
#   nu .ops/scripts/build-merge-replay-cases.nu
#   nu .ops/scripts/build-merge-replay-cases.nu --include-exact
#   nu .ops/scripts/build-merge-replay-cases.nu --lineage .ops/release-lineage.yaml --output .ops/merge-replay-cases.yaml

def is-b-tag [tag: string] {
  (($tag | parse -r '^v[0-9]+\.[0-9]+\.[0-9]+-b[0-9]*$') | length) > 0
}

def main [
  --lineage (-l): string = ".ops/release-lineage.yaml"
  --output (-o): string = ".ops/merge-replay-cases.yaml"
  --include-exact (-e)
] {
  if not ($lineage | path exists) {
    error make { msg: $"Lineage file not found: ($lineage). Run: task upgrade:history:build-release-lineage" }
  }

  let lineage_data = (open $lineage)
  let versions = ($lineage_data | get versions)

  let cases = (
    $versions
    | each { |v|
      let version = ($v | get version)
      let releases = ($v | get origin_releases)
      let upstream_info = ($v | get upstream)
      let exact_release = ($releases | where { |r| ($r | get tag) == $version } | get --optional 0)
      let upstream_commit_raw = ($upstream_info | get --optional commit)
      let upstream_authoritative = ($upstream_info | get --optional authoritative | default false)

      # Prefer authoritative upstream mapping; otherwise fallback to exact origin tag for deterministic historical replay.
      let upstream_commit = if ($upstream_authoritative and $upstream_commit_raw != null) {
        $upstream_commit_raw
      } else if $exact_release != null {
        $exact_release | get commit
      } else if $upstream_commit_raw != null {
        $upstream_commit_raw
      } else {
        null
      }
      let upstream_source = if ($upstream_authoritative and $upstream_commit_raw != null) {
        $upstream_info | get source
      } else if $exact_release != null {
        "origin_exact_tag_fallback"
      } else if $upstream_commit_raw != null {
        ($upstream_info | get source)
      } else {
        "unavailable"
      }

      let selected_releases = if $include_exact {
        $releases
      } else {
        $releases | where { |r| is-b-tag ($r | get tag) }
      }

      (
        $selected_releases
        | each { |r|
          let release_tag = ($r | get tag)
          let expected_commit = ($r | get commit)
          let pre_merge_commit = ($r | get pre_tag_commit)

          if ($upstream_commit == null or $pre_merge_commit == null) {
            null
          } else {
            let expected_tree_ref = ($expected_commit + "^{tree}")
            let expected_tree = (^git rev-parse $expected_tree_ref | str trim)
            {
              case_id: $release_tag
              version: $version
              release_tag: $release_tag
              pre_merge_commit: $pre_merge_commit
              upstream: {
                tag: $version
                commit: $upstream_commit
                source: $upstream_source
              }
              expected: {
                commit: $expected_commit
                tree: $expected_tree
              }
              assertions: {
                mode: "exact_tree"
                allowed_paths: []
                ai_assertion: null
              }
            }
          }
        }
        | where { |x| $x != null }
      )
    }
    | flatten
  )

  let out = {
    schema_version: 1
    generated_at: (date now | format date "%Y-%m-%dT%H:%M:%S%:z")
    lineage_file: $lineage
    defaults: {
      include_exact_tags: $include_exact
      assertion_mode: "exact_tree"
    }
    cases: $cases
  }

  let out_dir = ($output | path dirname)
  ^mkdir -p $out_dir
  ($out | to yaml) | save -f $output

  {
    output: $output
    cases: ($cases | length)
  } | print
}
