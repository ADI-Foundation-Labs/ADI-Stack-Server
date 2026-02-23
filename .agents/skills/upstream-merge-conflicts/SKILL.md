---
name: upstream-merge-conflicts
description: "Mode-based upstream merge-conflict workflow for ADI-Stack-Server. Use when upstream merge conflicts must be handled step-by-step with a persisted merge-agent state file: (1) analyze and classify conflicts, (2) plan low-risk commit groups, (3) resolve one group per commit, and (4) process human-input-required groups separately."
---

# Upstream Merge Conflicts

## Overview

Run merge resolution in distinct modes, each loaded as a separate file.
Do not run one large "resolve all conflicts" prompt.

## Mode Files

Load exactly one mode file when the mode is invoked:
- `references/mode-initial-analyze.md`
- `references/mode-plan-groups.md`
- `references/mode-resolve-group.md`
- `references/mode-resolve-human-group.md`

## Merge Agent State

Use `.ops/scripts/merge-agent-state.nu` to maintain state.
Default state file path:
- `.ops/.tmp/upstream-radar/<base_slug>__<target_slug>/merge-agent-state.yaml`

## Upstream Radar Data

Upstream radar data is generated under `.ops/.tmp/upstream-radar/<base_slug>__<target_slug>/` before conflict resolution begins. This data includes a complexity report (`report.md`) and lists of changed files (ADI-only, upstream-only, overlap) that provide crucial context for the merge.

The specific path to the active radar output directory is available in the merge agent state under the `radar_output_dir` key.

State tracks:
- unresolved file list
- analysis categories (`easy`, `medium`, `hard`, `human_input_needed`)
- planned groups with commit intent and status
- pending non-human groups vs pending human groups
- last completed group and commit hash

## Workflow

1. Start merge and keep `.ops/merge-status.yaml` current:

```bash
task upgrade:start:merge -- vX.Y.Z
task upgrade:status:show
```

2. Run mode `initial-analyze`:

```bash
task upgrade:agent:mode:analyze -- vX.Y.Z [agent]
```

3. Run mode `plan-groups`:

```bash
task upgrade:agent:mode:plan-groups -- vX.Y.Z [agent]
```

4. Resolve non-human groups one by one, one commit per group:

```bash
task upgrade:agent:mode:resolve-next -- vX.Y.Z [agent]
```

5. Process human-input groups separately:

```bash
task upgrade:agent:mode:resolve-next-human -- vX.Y.Z [agent]
```

6. Optionally resolve explicit group names:

```bash
task upgrade:agent:mode:resolve-group -- vX.Y.Z [agent] --group "<group-name>"
task upgrade:agent:mode:resolve-human-group -- vX.Y.Z [agent] --group "<group-name>"
```

7. Validate and transition status after all required groups are complete.

## Resolution Policy

Apply these invariants in every mode:
- Prefer upstream behavior for security, consensus correctness, and protocol compatibility unless it breaks explicit ADI requirements.
- Re-apply ADI-specific deltas as minimal, explicit patches.
- Keep conflict decisions deterministic and explainable; avoid "both-sides paste" merges.
- Never edit `.ops/merge-status.yaml` directly; use `task upgrade:status:*` commands.
- Enable and keep `git rerere` active to reuse previously accepted resolutions.
- Require one logical commit per resolved group.
- Keep human-required decisions isolated and explicit.

## Exit Criteria

Consider conflict resolution complete only when:
- `git -C "<worktree_path>" diff --name-only --diff-filter=U` returns no files.
- All non-human groups are completed.
- Human groups are either completed or explicitly blocked with decision notes.
- Validation commands pass locally.
- Merge status is transitioned through the expected lifecycle.
