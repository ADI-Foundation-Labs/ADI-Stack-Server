use crate::batcher_model::{BatchEnvelope, FriProof};
use crate::commands::L1SenderCommand;
use crate::config::L1SenderConfig;
use crate::metrics::L1SenderState;
use crate::tx_request_with_gas_fields;
use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::Address;
use alloy::providers::{Provider, WalletProvider};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use secrecy::ExposeSecret;
use std::str::FromStr;
use tokio::sync::mpsc::Sender;
use zksync_os_observability::ComponentStateReporter;
use zksync_os_pipeline::PeekableReceiver;

/// Dry-run version of L1 sender that simulates transactions using eth_call.
/// Logs the simulation result and passes batches downstream as if they succeeded.
/// Processes one batch at a time (no parallel processing).
pub async fn run_l1_sender_dry_run<Input: L1SenderCommand>(
    // == plumbing ==
    mut inbound: PeekableReceiver<Input>,
    outbound: Sender<BatchEnvelope<FriProof>>,

    // == command-specific settings ==
    to_address: Address,

    // == config ==
    provider: impl Provider + WalletProvider<Wallet = EthereumWallet> + 'static,
    config: L1SenderConfig<Input>,
) -> anyhow::Result<()> {
    let latency_tracker =
        ComponentStateReporter::global().handle_for(Input::NAME, L1SenderState::WaitingRecv);

    // Get operator address either from config or derive from private key
    let operator_address = if let Some(addr) = config.dry_run_operator_address {
        tracing::info!(
            command_name = Input::NAME,
            %addr,
            "dry-run mode using configured operator address (no private key required)"
        );
        addr
    } else {
        let signer = PrivateKeySigner::from_str(config.operator_pk.expose_secret())
            .context("failed to parse operator private key")?;
        let addr = signer.address();
        tracing::info!(
            command_name = Input::NAME,
            %addr,
            "dry-run mode using operator address derived from private key"
        );
        addr
    };

    loop {
        latency_tracker.enter_state(L1SenderState::WaitingRecv);
        let Some(mut cmd) = inbound.recv().await else {
            anyhow::bail!("inbound channel closed");
        };

        latency_tracker.enter_state(L1SenderState::SendingToL1);
        let command_name = Input::NAME;

        tracing::info!(
            command_name,
            %cmd,
            "simulating L1 transaction (dry-run mode)"
        );

        let tx_request = tx_request_with_gas_fields(
            &provider,
            operator_address,
            config.max_fee_per_gas(),
            config.max_priority_fee_per_gas(),
        )
        .await?
        .with_to(to_address)
        .with_call(&cmd.solidity_call());

        tracing::debug!(
            command_name,
            from = ?tx_request.from,
            to = ?tx_request.to,
            "dry-run eth_call transaction request"
        );

        // Use eth_call to simulate the transaction
        match provider.call(tx_request).await {
            Ok(result) => {
                tracing::info!(
                    command_name,
                    %cmd,
                    result_bytes = result.len(),
                    "dry-run eth_call succeeded"
                );
            }
            Err(e) => {
                tracing::warn!(
                    command_name,
                    %cmd,
                    error = %e,
                    "dry-run eth_call failed - sending batch downstream regardless"
                );
            }
        }

        // Mark as sent and mined even though we didn't actually send
        cmd.as_mut().iter_mut().for_each(|envelope| {
            envelope.set_stage(Input::SENT_STAGE);
            envelope.set_stage(Input::MINED_STAGE);
        });

        tracing::info!(
            command_name,
            %cmd,
            "dry-run simulation completed, sending downstream",
        );
        latency_tracker.enter_state(L1SenderState::WaitingSend);
        for output_envelope in cmd.into() {
            outbound.send(output_envelope).await?;
        }
    }
}
