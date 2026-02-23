# Mode: Resolve Human Group

## Goal

Handle one group marked `human_input_needed` and either:
- resolve and commit it, or
- mark it blocked with explicit decision requests.

## Inputs

- Merge agent state file path (provided in prompt).
- Human group payload (provided in prompt), including decision context.

## Required Actions

1. Attempt resolution for the target human group conservatively.
2. If decisions are still ambiguous:
   - Do not guess.
   - Set group `status` to `blocked`.
   - Add concrete decision questions to group `notes`.
3. If resolved safely:
   - Stage files.
   - Create one commit for the group.
   - Set group `status` to `completed`.
   - Update `resolution.last_completed_group` and `resolution.last_commit`.

## Rules

- Keep human groups small and focused.
- Preserve evidence in notes for every blocked item:
  - conflicting options
  - risk/tradeoff
  - exact file(s) affected
- Never silently downgrade a human-required decision into an automatic merge.
