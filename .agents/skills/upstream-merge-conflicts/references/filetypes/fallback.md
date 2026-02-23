# Fallback Merge Guide

Apply this guide when a conflicted file does not match a known file-type rule.

## Resolution Goals

- Produce a minimal, explicit final file state.
- Preserve upstream compatibility and ADI-specific intent.
- Keep every decision explainable in review.

## Procedure Per File

1. Inspect the conflict hunk and identify intent of both sides.
2. Prefer upstream behavior for compatibility/safety-sensitive logic.
3. Re-introduce required ADI behavior as a small, isolated patch.
4. Remove all conflict markers.
5. Stage file immediately.

## Validation Hints

- If file semantics are unclear, defer with a reviewer note rather than guessing.
- Run targeted tests for subsystems touched by fallback-resolved files.

