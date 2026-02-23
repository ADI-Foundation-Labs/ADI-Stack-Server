# Mode: Plan Groups

## Goal

Create conflict-resolution groups from analyzed files so each group can be resolved and committed independently.

## Inputs

- Merge agent state file path (provided in prompt).
- Analysis categories already written in state (`easy`, `medium`, `hard`, `human_input_needed`).

## Required Output

Update state in place:
- `status.grouping`: set to `completed`
- `groups`: list of records with fields:
  - `name`: short stable id
  - `purpose`: one-sentence intent
  - `description`: what this group resolves
  - `files`: list of file paths
  - `difficulty`: `easy|medium|hard|human_input_needed`
  - `human_input_needed`: bool
  - `status`: `pending`
  - `commit_message`: concise proposed commit message
  - `order`: execution order integer

## Grouping Constraints

- Ensure each group has clear meaning and commit intent.
- Keep group difficulty bounded; do not combine multiple hard files in one group.
- Ensure each non-human group is committable as a standalone step.
- Split `human_input_needed` work into smaller groups with explicit decision descriptions.
- Cover every unresolved file exactly once across all groups.

## Rules

- Do not resolve conflicts or commit in this mode.
- Keep group names short and deterministic.
- Prefer smaller groups when uncertain.
