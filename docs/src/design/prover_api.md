# Prover API

```
        .route("/prover-jobs/v1/status", get(status))
        .route("/prover-jobs/v1/FRI/pick", post(pick_fri_job))
        .route("/prover-jobs/v1/FRI/submit", post(submit_fri_proof))
        .route("/prover-jobs/v1/SNARK/pick", post(pick_snark_job))
        .route("/prover-jobs/v1/SNARK/submit", post(submit_snark_proof))
```

## Proof submission

- **FRI submit** fully verifies the proof: it runs the recursion verifier and compares the final registers
  with the batch's public input (`fri_job_manager.rs`, `fri_proof_verifier.rs`).
- **SNARK submit** checks only the shape (`snark_proof_shape/`). A rejection is a `400`, and nothing reaches
  `l1_sender`. A shape rejection also logs `rejected malformed SNARK proof` and increments the
  `prover_api_malformed_snark_proofs` counter.
  - `batch_from <= batch_to`. An empty range completes no jobs and panics the job map.
  - Exactly 44 words of 32 bytes. Words 0–21 and 40–43 are 13 BN254 G1 points `(x, y)`: each coordinate is
    below `q`, and each point is on `y² = x³ + 3`. Words 22–39 are 18 scalars below `r`. This is the input
    that L1 `loadProof` accepts.

## Decisions

- SNARK submit checks the shape and does not verify the proof. The shape check stops malformed input with
  no new dependency. Full verification was rejected: it needs ~29 new crates (incl. `boojum`), an embedded
  VK per proving version, and a toolchain the server does not build with.
- Out-of-range words are rejected, although L1 reduces them `mod q` / `mod r`. An honest prover never
  emits them.
- The curve math uses `U256` (`snark_proof_shape/bn254.rs`), not `ark-bn254`. Release lines lock
  different `ark` versions, so a direct dependency would make the lockfile diverge between them.

## Known gaps

- A well-formed but wrong SNARK proof passes the check, reverts on L1, and panics `l1_sender`.
- The 44-word length assumes every proving version uses the non-recursive Airbender PLONK wrapper. A
  recursive VK (48 words) or another SNARK type needs a change to `snark_proof_shape/`.
