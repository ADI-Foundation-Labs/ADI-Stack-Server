# ADI Stack Server

## What is ADI Stack Server?

ADI Stack Server is the sequencer implementation for the [ADI Stack OS](https://github.com/ADI-Foundation-Labs/ADI-Stack-zkOS),
the new operating system of the ADI Stack.<br>
The ADI Stack Server design optimizes for throughput, low latency, and a seamless development experience.

ADI Stack utilizing the [MatterLabs zkOS stack](https://github.com/matter-labs/zksync-os-server)

## Design principles

* Minimal, async persistence
  * to meet throughput and latency requirements, we avoid synchronous persistence at the critical path. Additionally,
    we aim at storing only the data that is strictly needed - minimizing the potential for state inconsistency
* Easy to replay arbitrary blocks
  * Sequencer: components are idempotent
  * Batcher: `batcher` component skips all blocks until the first uncommitted batch.
    Thus, downstream components only receive batches that they need to act upon 
* State - strong separation between
  * Actual state - data needed to execute VM: key-value storage and preimages map
  * Receipts repositories - data only needed in API
  * Data related to Proofs and L1 - not needed by sequencer / JSON RPC - only introduced downstream from `batcher`

## Quickstart

To run server locally with in-memory L1 node and dummy proofs, run the following commands:
```bash
# Launch zksync-os-server on the default port 3050
# This also starts in-memory L1 node as a background process.
# By default, fake (dummy) proofs are used both for FRI and SNARK proofs.
./run_local.sh ./local-chains/v31.0/default

# Use default rich account for testing
RICH_ACCOUNT=0x36615Cf349d7F6344891B1e7CA7C72883F5dc049
PRIVATE_KEY=0x7726827caac94a7f9e1b160f7ea819f172f7b6f9d2a97f992c38edeab82d4110

# Send test transaction
TO=0x5A67EE02274D9Ec050d412b96fE810Be4D71e7A0
cast send --private-key ${PRIVATE_KEY} --rpc-url http://localhost:3050 ${TO} --value 100
```

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for contribution guidelines.

## Policies

- [Contribution policy](CONTRIBUTING.md)

## License

ADI Stack repositories are distributed under the terms of either

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/blog/license/mit/>)

at your option.

## Official Links

- [Website](https://adi.foundation)
- [Docs](https://docs.adi.foundation/)
- [Github](https://github.com/ADI-Foundation-Labs/)
- [X](https://x.com/adi_foundation)
- [X for ADI Chain announcements](https://x.com/ADIChain_)
- [LinkedIn](https://www.linkedin.com/company/adifoundation/)
- [Discord](http://discord.gg/adi-foundation)