# Cargo Manifests And Lockfiles Merge Guide

Apply this guide for `Cargo.toml` and `Cargo.lock` conflicts.

## Resolution Goals

- Keep dependency graph changes intentional and reproducible.
- Preserve ADI-specific crate wiring while inheriting upstream fixes and version constraints.
- Never hand-edit lockfile conflict markers.

## `Cargo.toml` Policy

1. Resolve `[dependencies]`, `[dev-dependencies]`, and `[build-dependencies]` with explicit intent.
2. Preserve upstream version/security updates unless they break confirmed ADI requirements.
3. Keep ADI path/git overrides only when still required.
4. Ensure features remain explicit; do not silently drop feature flags from either side.

## `Cargo.lock` Policy

1. Resolve all `Cargo.toml` conflicts first.
2. Pick either side for `Cargo.lock` only as a temporary conflict clear step.
3. Regenerate lockfiles from manifests:

```bash
cargo generate-lockfile
```

4. If nested workspaces have their own lockfiles, regenerate with manifest path:

```bash
cargo generate-lockfile --manifest-path loadbase/Cargo.toml
```

5. Stage regenerated lockfiles and avoid manual line-level edits.

## Validation Hints

- Run `cargo check --workspace` after manifest and lockfile resolution.
- If resolver behavior changes, confirm no unintended crate removals in lockfile diff.
