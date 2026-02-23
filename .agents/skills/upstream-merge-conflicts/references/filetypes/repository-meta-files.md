# Repository Meta Files Merge Guide

Apply this guide for governance and repository policy files (for example `CODEOWNERS`, `LICENSE*`, `SECURITY.md`, `CONTRIBUTING.md`, `README.md`).

## Resolution Goals

- Keep legal and ownership metadata correct.
- Preserve ADI project governance requirements.
- Avoid dropping upstream security guidance unintentionally.

## Deterministic Policy

1. For `LICENSE*`, keep the repository's intended licensing model; do not merge conflicting legal text blindly.
2. For `CODEOWNERS`, preserve required ADI maintainers and add upstream ownership only when still relevant.
3. For `SECURITY.md` and `CONTRIBUTING.md`, keep current valid contact/process details and integrate upstream clarifications.
4. For `README.md`, keep canonical ADI quickstart paths and include upstream capability updates where accurate.

## Procedure Per File

1. Merge identity and policy sections first.
2. Merge examples and command snippets second.
3. Remove stale references to deprecated branches, tags, or contacts.
4. Stage after a final human-readable pass.

## Validation Hints

- Confirm all referenced links and contacts are still valid in this repository context.
- Re-check release/tag naming (`vX.Y.Z-bN`) in docs after merge.

