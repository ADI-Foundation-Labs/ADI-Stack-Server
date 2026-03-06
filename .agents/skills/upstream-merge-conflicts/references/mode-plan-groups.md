# Mode: Plan Groups

## Goal

Create conflict-resolution groups from analyzed files so each group can be resolved and committed independently.

## Inputs

- Merge agent state file path (provided in prompt).
- Analysis categories already written in state (`easy`, `medium`, `hard`, `human_input_needed`).

## Required Output

Update state in place using your file editing tools (do not just print to stdout):

- `status.grouping`: set to `completed`
- `groups`: list of records. Each group MUST strictly map to the YAML template provided in `.agents/skills/upstream-merge-conflicts/references/group-template.yaml`.

**CRITICAL**: You must actively write this `groups` array into the YAML state file using an edit tool. Do not just output markdown to the user.

## CRITICAL BEHAVIOR WARNING

You historically have a failure mode where you change `status.grouping` to `completed` but leave `groups: []` empty in the file! **This is a fatal error**.

1. You MUST use your file replacement/editing tools to inject the full text of the `groups` list into the YAML file.
2. After writing, you MUST read the file to verify that the `groups` array is actually populated.
3. If `groups: []` is still empty, you MUST re-attempt editing until it is successful before finishing your turn.

## Grouping Constraints

- Ensure each group has clear meaning and commit intent.
- Focus on grouping changes by commits from the upstream and ADI sides, rather than just by files, by examining the commit history.
- Keep group difficulty bounded; do not combine multiple hard files in one group.
- Ensure each non-human group is committable as a standalone step.
- Split `human_input_needed` work into smaller groups with explicit decision descriptions.
- Cover every unresolved file exactly once across all groups.

## Rules

- Do not resolve conflicts or commit in this mode.
- Keep group names short and deterministic.
- Prefer smaller groups when uncertain.
