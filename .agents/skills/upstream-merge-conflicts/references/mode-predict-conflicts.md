# Mode: Predict Conflicts

## Goal

Compare local unmerged ADI changes against recent upstream commits to predict if the local modifications will cause painful conflicts during the next merge, and suggest alternative approaches.

## Inputs

- Overlap diff output containing recent upstream changes vs ADI local changes for specific files (provided in prompt).

## Required Output

Produce a Markdown-formatted report that:

- Identifies which specific files and functions are high-risk for severe merge conflicts.
- Explains why the conflict is likely to occur based on the overlapping changes.
- Suggests concrete alternative approaches for the local ADI implementation to minimize or avoid the predicted future conflict.

## Rules

- Do not modify code; only generate the analytical report.
- Focus on areas where ADI logic and upstream logic are tightly coupled or modifying the exact same structural blocks.
- If no severe conflicts are predicted, state clearly that the risk is low.
