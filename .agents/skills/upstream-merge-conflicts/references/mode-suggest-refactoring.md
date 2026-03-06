# Mode: Suggest Refactoring

## Goal

Analyze the resolved conflicts from the current upstream merge and suggest architectural or structural refactorings on the ADI side to prevent similar conflicts in future merges.

## Inputs

- List of conflicted and resolved files (provided in prompt).
- Radar summary context (provided in prompt).

## Required Output

Produce a Markdown-formatted report with actionable refactoring suggestions. The report should:

- Group related conflicts that share a common root cause.
- Suggest specific techniques to decouple ADI logic from upstream flow (e.g., introducing hooks, traits, dependency injection, isolating custom features in separate files, or using event dispatchers).
- Evaluate the estimated effort vs. future merge friction saved for each suggestion.

## Rules

- Base suggestions on the actual files that conflicted during this merge.
- Do not modify or resolve any code; only produce the Markdown report.
- Focus strictly on structural and architectural improvements rather than micro-optimizations.
- Prefer patterns that allow ADI to consume upstream updates without resolving the same logic blocks repeatedly.
