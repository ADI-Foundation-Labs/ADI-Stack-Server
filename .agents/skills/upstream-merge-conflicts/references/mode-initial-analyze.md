# Mode: Initial Analyze

## Goal

Classify current unresolved conflict files into:

- `easy`
- `medium`
- `hard`
- `human_input_needed`

Do not resolve conflicts in this mode.

## Inputs

- Active merge worktree (current directory).
- Merge agent state file path (provided in prompt).
- Upstream radar data directory (path found in `radar_output_dir` in state file), specifically:
  - `report.md`: Contains merge complexity, commit counts, and risk level.
  - `lists/`: Contains lists of overlap files, ADI-only changes, and upstream-only changes.
- Current unresolved files from:

```bash
git diff --name-only --diff-filter=U
```

## Required Output

Update the merge agent state file in place:

- `status.analysis`: set to `completed`
- `analysis.generated_at`: set to current timestamp string
- `analysis.easy`: list of file paths
- `analysis.medium`: list of file paths
- `analysis.hard`: list of file paths
- `analysis.human_input_needed`: list of records:
  - `file`
  - `reason`
  - `suggestion` (optional)
- `analysis.notes`: short rationale for difficult areas

## Rules

- First, review the upstream radar `report.md` and file `lists/` to understand the context of the merge (e.g. which files were heavily modified upstream vs ADI) before classifying files.
- Classify every unresolved file exactly once.
- Keep categorization deterministic and explainable.
- Put uncertain decisions into `human_input_needed` instead of guessing.
- Do not stage files, resolve conflicts, or create commits in this mode.
