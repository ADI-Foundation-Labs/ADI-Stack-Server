# Docs And Markdown Merge Guide

Apply this guide for documentation and content files (`docs/**`, `*.md`, `*.rst`, text assets).

## Resolution Goals

- Keep release and upgrade documentation aligned with the merged code.
- Preserve ADI process details while incorporating upstream documentation improvements.
- Keep changelog entries accurate and non-duplicated.

## Deterministic Policy

1. For release notes and changelog files, keep both upstream facts and ADI-specific release context.
2. For user-facing guides, preserve current ADI runbook commands unless upstream command changes are required by code changes.
3. For generated or static assets (for example images), prefer upstream version unless ADI branding override is intentional.

## Procedure Per File

1. Merge headings and section structure first.
2. Merge command snippets second and verify command names match actual task targets.
3. Merge narrative text last, removing repeated paragraphs.
4. Stage each resolved doc file after conflict markers are removed.

## Validation Hints

- Spot-check that referenced task names and file paths exist.
- Ensure docs do not mention deprecated branch/tag conventions after merge.
