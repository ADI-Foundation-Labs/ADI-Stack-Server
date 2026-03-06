# CI And Automation Config Merge Guide

Apply this guide for workflow and automation files (`.github/workflows/*`, `.ops/**/*.nu`, `Taskfile.yml`, `*.nu`).

## Resolution Goals

- Keep CI and merge automation executable after conflict resolution.
- Preserve ADI upstream-release lifecycle commands and status transitions.
- Inherit upstream security and reliability workflow improvements where compatible.

## Deterministic Policy

1. Keep required ADI merge flow tasks under `upgrade:*` intact.
2. Keep workflow trigger blocks explicit (`on:` events, branches, tags).
3. Preserve secret names and permission scopes from both sides unless one side is obsolete.
4. For Nushell scripts, keep argument names and output schema stable when callers depend on them.

## Procedure Per File

1. Merge interface first: command names, function signatures, CLI flags.
2. Merge behavior second: status transitions, push behavior, validation gates.
3. Merge formatting and comments last.
4. Stage after confirming the file has no conflict markers.

## Validation Hints

- Run at least one `task upgrade:*` command that exercises modified script/task plumbing.
- If GitHub workflow files changed, verify job names and referenced actions still exist.
