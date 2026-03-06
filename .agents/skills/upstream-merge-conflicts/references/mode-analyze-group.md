# Mode: Analyze Group

## Goal

Analyze a single group by examining the commit history from both the ADI and upstream sides, including other files changed in related commits, and propose resolution strategies.

## Inputs

- Merge agent state file path (provided in prompt).
- Target group details (provided in prompt).

## Required Output

Update the target group's state in place within the `groups` list:
- `resolve_options`: A list of strings, where each string is a distinct suggestion with reasoning on how to resolve the group.
- `status`:
  - If exactly one option is provided: set to `analyzed`.
  - If multiple options are provided: set to `blocked`.
- `human_input_needed`: If multiple options are provided (status `blocked`), set this to `true`.

## Rules

- Examine the git history from both the upstream branch and ADI branch for the files in the group. Use `git log` and `git show` to understand why the changes were made, and note the author of the commits to better understand the source and intent of the changes.
- Propose one or more complete resolution strategies based on ADI and upstream intent.
- Do not resolve conflicts or stage files in this mode.
- If more than one option is viable, do not guess; output all options, set status to `blocked`, and require a human to select a single option before resolution can proceed.
