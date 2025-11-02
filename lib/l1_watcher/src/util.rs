use alloy::primitives::BlockNumber;
use alloy::providers::{DynProvider, Provider};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use zksync_os_contract_interface::ZkChain;

pub async fn find_l1_block_by_predicate<Fut: Future<Output = anyhow::Result<bool>>>(
    zk_chain: Arc<ZkChain<DynProvider>>,
    predicate: impl Fn(Arc<ZkChain<DynProvider>>, u64) -> Fut,
) -> anyhow::Result<BlockNumber> {
    let latest = zk_chain.provider().get_block_number().await?;

    let guarded_predicate =
        async |zk: Arc<ZkChain<DynProvider>>, block: u64| -> anyhow::Result<bool> {
            if !zk.code_exists_at_block(block.into()).await? {
                // return early if contract is not deployed yet - otherwise `predicate` might fail
                return Ok(false);
            }
            predicate(zk, block).await
        };

    // Ensure the predicate is true by the upper bound, or bail early.
    if !guarded_predicate(zk_chain.clone(), latest).await? {
        anyhow::bail!(
            "Condition not satisfied up to latest block: contract not deployed yet \
             or target not reached.",
        );
    }

    // Binary search on [0, latest] for the first block where predicate is true.
    let (mut lo, mut hi) = (0u64, latest);
    while lo < hi {
        let mid = (lo + hi) / 2;
        if guarded_predicate(zk_chain.clone(), mid).await? {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }

    Ok(lo)
}

/// Retry a storage lookup with a grace period, logging warnings along the way.
pub async fn retry_with_grace_period<T, E, F, Fut>(
    operation: F,
    grace_period: Duration,
    retry_interval: Duration,
    context: &str,
) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<Option<T>, E>>,
    E: std::fmt::Debug,
{
    let start = std::time::Instant::now();
    let mut attempt = 0;

    loop {
        attempt += 1;
        match operation().await {
            Ok(Some(value)) => {
                if attempt > 1 {
                    tracing::info!(
                        context,
                        attempt,
                        elapsed_ms = start.elapsed().as_millis(),
                        "Successfully retrieved data after retrying"
                    );
                }
                return Ok(value);
            }
            Ok(None) => {
                let elapsed = start.elapsed();
                if elapsed >= grace_period {
                    tracing::error!(
                        context,
                        attempt,
                        grace_period_sec = grace_period.as_secs(),
                        "Grace period expired, data not found in storage"
                    );
                    panic!("{} is not present in storage after {} seconds grace period", context, grace_period.as_secs());
                }

                let remaining = grace_period - elapsed;
                tracing::warn!(
                    context,
                    attempt,
                    elapsed_sec = elapsed.as_secs(),
                    remaining_sec = remaining.as_secs(),
                    "Data not found in storage, will retry (this is expected for External Nodes with delayed proof sync)"
                );
                tokio::time::sleep(retry_interval).await;
            }
            Err(e) => {
                tracing::error!(context, ?e, "Error accessing storage");
                return Err(e);
            }
        }
    }
}
