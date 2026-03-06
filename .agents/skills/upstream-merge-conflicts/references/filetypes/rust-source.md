# Rust Source Merge Guide

Apply this guide for `.rs` conflict files.

## Resolution Goals

- Keep upstream correctness, consensus, and security behavior.
- Re-apply ADI-specific behavior as minimal deltas on top of upstream changes.
- Avoid changing public interfaces unless required by both branches.

## Deterministic Resolution Order

1. Resolve module and import structure first.
2. Resolve type and trait definitions second.
3. Resolve function signatures third.
4. Resolve function body logic last.
5. Resolve tests or assertions in the same file before staging.

## Procedure Per File

1. Compare ADI and upstream patch intent (use generated patch artifacts when available).
2. Build a single final implementation that preserves upstream invariants and explicit ADI requirements.
3. Remove all conflict markers.
4. Stage the file immediately.

## Validation Hints

- Prefer compiling the affected crate after resolving a cluster of Rust files.
- If method signatures changed upstream, propagate updates to all callers before running full workspace checks.
