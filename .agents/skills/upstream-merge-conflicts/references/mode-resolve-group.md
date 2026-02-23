# Mode: Resolve Group

## Goal

Resolve one planned non-human group, stage changes, and create exactly one commit for that group.

## Inputs

- Merge agent state file path (provided in prompt).
- Group payload (provided in prompt), including:
  - `name`
  - `purpose`
  - `files`
  - `commit_message`

## Required Actions

1. Resolve conflicts only for files in the target group.
2. Stage resolved files (`git add ...`).
3. Create one commit for the group.
4. Update state file:
   - group `status`: set to `completed`
   - group `notes`: brief summary of decisions
   - `resolution.last_completed_group`: group name
   - `resolution.last_commit`: current `HEAD` hash

## Rules

- Do not edit files outside the group unless strictly required to complete a grouped file.
- Keep the commit message short and meaningful:
  - Use group `commit_message` if present.
  - Otherwise compose one from group purpose.
- If unresolved ambiguity is discovered, stop and move that part into a human-input group by updating state accordingly.
