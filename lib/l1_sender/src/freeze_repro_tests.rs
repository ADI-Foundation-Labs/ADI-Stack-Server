//! Reproduction test for the L1 sender "freeze after sending batches to L1" bug.
//!
//! Symptom observed in production: the log line
//!   `sending L1 transactions`            (lib.rs, before the send loop)
//! is emitted, but the following line
//!   `sent to L1, waiting for inclusion`  (lib.rs, after the send loop)
//! is never emitted. The component makes no further progress and never crashes.
//!
//! Root cause: every provider RPC issued between those two log lines runs with
//! **no timeout**:
//!   * `estimate_eip1559_fees()` -> `eth_feeHistory`
//!   * `provider.fill(..)`       -> `eth_getTransactionCount`, `eth_chainId`, `eth_estimateGas`
//!   * `provider.get_block(..)`  -> `eth_getBlockByNumber`
//!   * `provider.send_raw_transaction(..)` -> `eth_sendRawTransaction`
//!
//! The only timeout in the whole flow is `TRANSACTION_TIMEOUT`, and it is attached
//! to `get_receipt()` (the *inclusion* wait, awaited AFTER the second log line).
//! It does NOT cover the sending phase. Note also that the production retry layer
//! does not help here: a request that simply never returns is not an *error*, so
//! it is never retried — the future just stays pending forever.
//!
//! This test drives `run_l1_sender` with a transport whose first send-path RPC
//! (`eth_feeHistory`) never returns, and asserts that `run_l1_sender` freezes:
//! it neither produces output downstream nor returns.

use crate::batcher_model::{
    BatchEnvelope, BatchMetadata, BatchSignatureData, FriProof, SignedBatchEnvelope,
};
use crate::commands::L1SenderCommand;
use crate::commands::commit::CommitCommand;
use crate::config::L1SenderConfig;
use crate::run_l1_sender;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256};
use alloy::providers::ProviderBuilder;
use alloy::rpc::client::RpcClient;
use alloy::rpc::json_rpc::{RequestPacket, Response, ResponsePacket, ResponsePayload};
use alloy::signers::local::PrivateKeySigner;
use alloy::transports::{TransportError, TransportFut};
use secrecy::SecretString;
use serde_json::value::RawValue;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tower::Service;
use zksync_os_batch_types::BatchInfo;
use zksync_os_contract_interface::models::{CommitBatchInfo, DACommitmentScheme, StoredBatchInfo};
use zksync_os_pipeline::PeekableReceiver;
use zksync_os_types::{ProtocolSemanticVersion, PubdataMode};

/// A test transport that mimics an L1 node which goes unresponsive mid-flight.
///
/// It answers the handful of benign read RPCs needed to enter the send loop
/// (`eth_getBalance` for the operator balance check, plus `eth_chainId` /
/// `eth_blockNumber` as a safety net), and then **hangs forever** on the first
/// real send-path RPC. The hung method name is recorded and `hang_signal` is
/// fired so the test can synchronize precisely on "we are now stuck between the
/// two log lines".
#[derive(Clone)]
struct HangingL1Transport {
    hang_signal: Arc<Notify>,
    hung_on_method: Arc<Mutex<Option<String>>>,
}

impl HangingL1Transport {
    /// JSON-RPC results for the benign read calls. Values are arbitrary but valid.
    fn canned_result(method: &str) -> Option<&'static str> {
        match method {
            // 2 ETH — must be non-zero so `register_operator` does not bail.
            "eth_getBalance" => Some("\"0x1bc16d674ec80000\""),
            "eth_chainId" => Some("\"0x10f\""),
            "eth_blockNumber" => Some("\"0x1\""),
            _ => None,
        }
    }
}

impl Service<RequestPacket> for HangingL1Transport {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let hang_signal = self.hang_signal.clone();
        let hung_on_method = self.hung_on_method.clone();

        // Decode what we need synchronously so the returned future does not hold `req`.
        let single = match req {
            RequestPacket::Single(req) => Some((req.method().to_owned(), req.id().clone())),
            RequestPacket::Batch(_) => None,
        };

        Box::pin(async move {
            match single {
                Some((method, id)) if Self::canned_result(&method).is_some() => {
                    let raw =
                        RawValue::from_string(Self::canned_result(&method).unwrap().to_string())
                            .unwrap();
                    Ok(ResponsePacket::Single(Response {
                        id,
                        payload: ResponsePayload::Success(raw),
                    }))
                }
                Some((method, _)) => {
                    // THE BUG: `run_l1_sender` issued this send-path RPC with no
                    // timeout around it. We never answer -> the future stays
                    // pending forever -> the whole component freezes.
                    *hung_on_method.lock().unwrap() = Some(method);
                    hang_signal.notify_one();
                    std::future::pending::<()>().await;
                    unreachable!("hanging transport never resolves")
                }
                None => {
                    hang_signal.notify_one();
                    std::future::pending::<()>().await;
                    unreachable!("hanging transport never resolves")
                }
            }
        })
    }
}

/// Build a minimal but valid `SignedBatchEnvelope<FriProof>` so a `CommitCommand`
/// can be constructed. Mirrors the `dummy_batch_metadata` helper in
/// `zksync_os_batch_verification` (which we cannot depend on from here).
fn signed_envelope(batch_number: u64) -> SignedBatchEnvelope<FriProof> {
    let metadata = BatchMetadata {
        previous_stored_batch_info: StoredBatchInfo {
            batch_number: batch_number - 1,
            state_commitment: B256::ZERO,
            number_of_layer1_txs: 0,
            priority_operations_hash: B256::ZERO,
            dependency_roots_rolling_hash: B256::ZERO,
            l2_to_l1_logs_root_hash: B256::ZERO,
            commitment: B256::ZERO,
            last_block_timestamp: 0,
        },
        batch_info: BatchInfo {
            commit_info: CommitBatchInfo {
                batch_number,
                new_state_commitment: B256::ZERO,
                number_of_layer1_txs: 0,
                priority_operations_hash: B256::ZERO,
                dependency_roots_rolling_hash: B256::ZERO,
                l2_to_l1_logs_root_hash: B256::ZERO,
                l2_da_commitment_scheme: DACommitmentScheme::BlobsAndPubdataKeccak256,
                da_commitment: B256::ZERO,
                first_block_timestamp: 0,
                first_block_number: Some(batch_number),
                last_block_timestamp: 0,
                last_block_number: Some(batch_number),
                chain_id: 270,
                operator_da_input: Vec::new(),
            },
            chain_address: Address::ZERO,
            upgrade_tx_hash: None,
            blob_sidecar: None,
        },
        first_block_number: batch_number,
        last_block_number: batch_number,
        pubdata_mode: PubdataMode::Calldata,
        tx_count: 0,
        execution_version: 1,
        protocol_version: ProtocolSemanticVersion::legacy_genesis_version(),
    };
    BatchEnvelope::new(metadata, FriProof::Fake).with_signatures(BatchSignatureData::NotNeeded)
}

/// Everything needed to drive and observe a `run_l1_sender` whose L1 node hangs.
struct Harness {
    handle: JoinHandle<anyhow::Result<()>>,
    outbound_rx: mpsc::Receiver<SignedBatchEnvelope<FriProof>>,
    /// Fired once `run_l1_sender` issues (and gets stuck on) a send-path RPC.
    hang_signal: Arc<Notify>,
    /// The RPC method `run_l1_sender` froze on.
    hung_on_method: Arc<Mutex<Option<String>>>,
    /// Kept alive so the inbound channel does not close.
    _inbound_tx: mpsc::Sender<L1SenderCommand<CommitCommand>>,
}

/// Spawn `run_l1_sender` wired to a single commit command and a `HangingL1Transport`.
async fn spawn_sender_with_hanging_l1() -> Harness {
    let hang_signal = Arc::new(Notify::new());
    let hung_on_method = Arc::new(Mutex::new(None));
    let transport = HangingL1Transport {
        hang_signal: hang_signal.clone(),
        hung_on_method: hung_on_method.clone(),
    };

    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(PrivateKeySigner::random()))
        .connect_client(RpcClient::new(transport, true));

    let (inbound_tx, inbound_rx) = mpsc::channel::<L1SenderCommand<CommitCommand>>(8);
    let inbound = PeekableReceiver::new(inbound_rx);
    let (outbound_tx, outbound_rx) = mpsc::channel::<SignedBatchEnvelope<FriProof>>(8);

    // One batch to commit. `inbound_tx` is kept alive so the channel never closes.
    inbound_tx
        .send(L1SenderCommand::SendToL1(CommitCommand::new(
            signed_envelope(42),
        )))
        .await
        .unwrap();

    let config = L1SenderConfig::<CommitCommand> {
        // anvil's well-known dev key #0 — only used to derive the operator address.
        operator_pk: SecretString::from(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        ),
        max_fee_per_gas_wei: 100_000_000_000,
        max_priority_fee_per_gas_wei: 1_000_000_000,
        max_fee_per_blob_gas_wei: 1_000_000_000,
        command_limit: 8,
        poll_interval: Duration::from_millis(50),
        fusaka_upgrade_timestamp: u64::MAX,
        phantom_data: PhantomData,
    };

    let handle = tokio::spawn(run_l1_sender(
        inbound,
        outbound_tx,
        Address::ZERO,
        provider,
        config,
    ));

    Harness {
        handle,
        outbound_rx,
        hang_signal,
        hung_on_method,
        _inbound_tx: inbound_tx,
    }
}

/// Characterization test for the CURRENT (buggy) behavior: when a send-path L1 RPC
/// never returns, `run_l1_sender` gets stuck between "sending L1 transactions" and
/// "sent to L1, waiting for inclusion" — no output, no error, forever.
///
/// This test PASSES on current code (it documents the bug). Once the timeout fix
/// lands, the freeze no longer happens and this test should be removed in favor of
/// `gives_up_instead_of_freezing_when_l1_rpc_hangs` below.
#[tokio::test]
async fn freezes_between_sending_and_inclusion_when_l1_rpc_hangs() {
    let mut harness = spawn_sender_with_hanging_l1().await;

    // The component must reach the send path and issue an un-timed-out L1 RPC.
    // If this times out, the test setup is wrong (we never got into the send loop).
    tokio::time::timeout(Duration::from_secs(10), harness.hang_signal.notified())
        .await
        .expect("run_l1_sender never issued a send-path L1 RPC");

    // Document exactly where it froze: the first send-path RPC, which sits
    // between the two log lines in `run_l1_sender`.
    assert_eq!(
        harness.hung_on_method.lock().unwrap().as_deref(),
        Some("eth_feeHistory"),
        "expected the freeze to happen on the first send-path RPC (gas fee estimation)"
    );

    // THE FREEZE: with that RPC hanging and no timeout around it, `run_l1_sender`
    // makes no further progress. It does not emit a batch downstream...
    let progress = tokio::time::timeout(Duration::from_secs(2), harness.outbound_rx.recv()).await;
    assert!(
        progress.is_err(),
        "expected a freeze (no downstream output), but the sender made progress: {progress:?}"
    );
    // ...and it does not return (no crash, no completion) — it is stuck forever.
    assert!(
        !harness.handle.is_finished(),
        "expected run_l1_sender to be frozen, but the task finished"
    );

    harness.handle.abort();
}

/// RED test (currently failing): pins the desired behavior that the suggested fix
/// must deliver.
///
/// Desired behavior: when a send-path L1 RPC hangs, `run_l1_sender` must NOT freeze
/// forever — it must give up after a bounded time and return an `Err`, so the
/// supervisor can crash-and-restart it (matching the component's existing recovery
/// model). The fix is to wrap the send-path RPCs inside `run_l1_sender` with
/// `tokio::time::timeout(SEND_TIMEOUT, ..)` and turn an elapsed timeout into an error.
///
/// This test uses a **paused virtual clock** (`start_paused = true`): the tokio
/// runtime auto-advances time to the next pending timer whenever it is otherwise
/// idle. That makes the test independent of the actual `SEND_TIMEOUT` magnitude and
/// keeps it instant in both states:
///   * CURRENT code: the send-path future is parked with no timer of its own, so the
///     only timer is this test's deadline -> it elapses -> the test FAILS (red),
///     fast, without hanging the suite.
///   * AFTER the fix: `run_l1_sender`'s own timeout fires first (auto-advanced),
///     `run_l1_sender` returns `Err`, and the test PASSES (green).
///
/// Note: this pins the in-`run_l1_sender` timeout fix specifically. A transport-level
/// timeout (e.g. a `reqwest` request timeout in `build_node_l1_provider`) lives below
/// this mock transport and would need a separate integration test against a real
/// hung HTTP server.
#[tokio::test(start_paused = true)]
async fn gives_up_instead_of_freezing_when_l1_rpc_hangs() {
    let harness = spawn_sender_with_hanging_l1().await;

    // Make sure we actually reached the send path and got stuck on a send-path RPC.
    tokio::time::timeout(Duration::from_secs(10), harness.hang_signal.notified())
        .await
        .expect("run_l1_sender never issued a send-path L1 RPC");
    assert_eq!(
        harness.hung_on_method.lock().unwrap().as_deref(),
        Some("eth_feeHistory"),
        "expected to be stuck on the first send-path RPC (gas fee estimation)"
    );

    // Generous virtual-time budget — far larger than any sane `SEND_TIMEOUT`. With the
    // paused clock this resolves instantly: either `run_l1_sender`'s own timeout fires
    // (after the fix) or this deadline elapses (current code).
    const FIX_DEADLINE: Duration = Duration::from_secs(24 * 60 * 60);

    match tokio::time::timeout(FIX_DEADLINE, harness.handle).await {
        Err(_elapsed) => panic!(
            "run_l1_sender froze: it did not return within {FIX_DEADLINE:?} (virtual time) after \
             an L1 RPC hung. Fix: wrap the send-path RPCs in `tokio::time::timeout` so a hung \
             request becomes an error (crash-and-restart) instead of an infinite freeze."
        ),
        Ok(Err(join_err)) => panic!("run_l1_sender task panicked: {join_err}"),
        Ok(Ok(Ok(()))) => {
            panic!("run_l1_sender returned Ok, but a hung send should surface as an error")
        }
        // Expected once the fix lands: it gave up with an error instead of freezing.
        Ok(Ok(Err(_err))) => {}
    }
}
