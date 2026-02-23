# Build release lineage mapping between origin release tags and upstream base tags.
#
# Output format (YAML):
# - schema_version
# - generated_at
# - source
# - versions[]:
#   - version
#   - upstream { tag, commit, found_on_upstream }
#   - origin_releases[]:
#     - tag
#     - tag_object
#     - tag_object_type
#     - commit
#     - commit_date
#     - subject
#     - parent_count
#     - parents
#     - pre_tag_commit
#     - inferred_merge_start_commit
#     - is_merge_commit
#
# Usage:
#   nu .ops/scripts/build-release-lineage.nu
#   nu .ops/scripts/build-release-lineage.nu --output .ops/release-lineage.yaml

use lib/git/upstream.nu *

def is-release-like-tag [tag: string] {
  (($tag | parse -r '^v[0-9]+\.[0-9]+\.[0-9]+(-b[0-9]*)?$') | length) > 0
}

def base-version-tag [tag: string] {
  let parsed = ($tag | parse -r '^(?P<base>v[0-9]+\.[0-9]+\.[0-9]+)(?:-b[0-9]*)?$')
  if (($parsed | length) > 0) {
    $parsed | get 0.base
  } else {
    null
  }
}

def upstream-tag-commit-map [] {
  let run = (^git ls-remote --tags upstream | complete)
  if $run.exit_code != 0 {
    return {}
  }

  let refs = (
    $run.stdout
    | lines
    | where { |line| ($line | str trim) != "" }
    | each { |line|
      let parts = ($line | split row "\t")
      {
        hash: ($parts | get 0)
        ref: ($parts | get 1)
      }
    }
  )

  mut m = {}

  # Start with direct refs (lightweight tags or annotated tag objects).
  for r in ($refs | where { |x| not ($x.ref | str ends-with '^{}') }) {
    let tag = ($r.ref | str replace 'refs/tags/' '')
    $m = ($m | upsert $tag $r.hash)
  }

  # Prefer dereferenced commit hashes for annotated tags.
  for r in ($refs | where { |x| $x.ref | str ends-with '^{}' }) {
    let tag = (
      $r.ref
      | str replace 'refs/tags/' ''
      | str replace '^{}' ''
    )
    $m = ($m | upsert $tag $r.hash)
  }

  $m
}

def resolve-upstream-commit [base: string, upstream_map: record] {
  let remote_commit = ($upstream_map | get --optional $base)
  if $remote_commit != null {
    {
      commit: $remote_commit
      source: "upstream_remote_tag"
      found_on_upstream: true
      authoritative: true
    }
  } else if (git-ref-exists $"upstream/($base)") {
    {
      commit: (^git rev-parse $"upstream/($base)" | str trim)
      source: "local_upstream_branch"
      found_on_upstream: false
      authoritative: false
    }
  } else if (git-ref-exists $"origin/upstream/($base)") {
    {
      commit: (^git rev-parse $"origin/upstream/($base)" | str trim)
      source: "origin_upstream_branch"
      found_on_upstream: false
      authoritative: false
    }
  } else {
    {
      commit: null
      source: "unavailable"
      found_on_upstream: false
      authoritative: false
    }
  }
}

def release-record [tag: string] {
  let tag_object = (^git rev-parse $tag | str trim)
  let tag_object_type = (^git cat-file -t $tag_object | str trim)
  let commit = (^git rev-list -n 1 $tag | str trim)
  let date = (^git show -s --format=%cI $commit | str trim)
  let subject = (^git show -s --format=%s $commit | str trim)
  let parents_line = (^git show -s --format=%P $commit | str trim)
  let parents = if $parents_line == "" {
    []
  } else {
    $parents_line | split row " " | where { |p| $p != "" }
  }
  let parent_count = ($parents | length)
  let pre_tag = if $parent_count > 0 { $parents | get 0 } else { null }
  let is_merge = $parent_count > 1

  {
    tag: $tag
    tag_object: $tag_object
    tag_object_type: $tag_object_type
    commit: $commit
    commit_date: $date
    subject: $subject
    parent_count: $parent_count
    parents: $parents
    pre_tag_commit: $pre_tag
    inferred_merge_start_commit: $pre_tag
    is_merge_commit: $is_merge
  }
}

def main [
  --output (-o): string = ".ops/release-lineage.yaml"
] {
  let origin_tags = (
    ^git tag --sort=version:refname
    | lines
    | where { |tag| is-release-like-tag $tag }
  )

  let upstream_map = (upstream-tag-commit-map)
  let base_versions = (
    $origin_tags
    | each { |tag| base-version-tag $tag }
    | where { |v| $v != null }
    | uniq
  )

  let versions = (
    $base_versions
    | each { |base|
      let releases = (
        $origin_tags
        | where { |tag| (base-version-tag $tag) == $base }
        | each { |tag| release-record $tag }
      )
      let exact_release = ($releases | where { |r| ($r | get tag) == $base } | get --optional 0)
      let assumed_commit = if $exact_release == null { null } else { $exact_release | get commit }

      let upstream = (resolve-upstream-commit $base $upstream_map)
      {
        version: $base
        upstream: {
          tag: $base
          commit: ($upstream | get commit)
          source: ($upstream | get source)
          found_on_upstream: ($upstream | get found_on_upstream)
          authoritative: ($upstream | get authoritative)
          assumed_commit_from_origin_exact_tag: (if (($upstream | get commit) == null) { $assumed_commit } else { null })
        }
        origin_releases: $releases
      }
    }
  )

  let out = {
    schema_version: 1
    generated_at: (now-iso)
    source: {
      repository: (git-project-name)
      origin_remote: "origin"
      upstream_remote: "upstream"
      release_tag_pattern: "^vX.Y.Z(-bN)?$"
      note: "pre_tag_commit/inferred_merge_start_commit are inferred from first parent of tagged commit."
    }
    versions: $versions
  }

  let out_dir = ($output | path dirname)
  ^mkdir -p $out_dir
  ($out | to yaml) | save -f $output

  {
    output: $output
    versions: ($versions | length)
    origin_release_tags: ($origin_tags | length)
  } | print
}
