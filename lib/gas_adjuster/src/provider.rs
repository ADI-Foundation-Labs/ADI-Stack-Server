use alloy::providers::{DynProvider, Provider};

/// Information about the base fees provided by the L1 client.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BaseFees {
    pub base_fee_per_gas: u128,
    pub base_fee_per_blob_gas: u128,
}

#[async_trait::async_trait]
pub trait EthFeeProvider: Provider + std::fmt::Debug {
    /// Collects the base fee history for the specified block range.
    ///
    /// Returns 1 value for each block in range, assuming that these blocks exist.
    /// Will return an error if the `upto_block` is beyond the head block.
    async fn base_fee_history(
        &self,
        upto_block: u64,
        block_count: u64,
    ) -> anyhow::Result<Vec<BaseFees>>;
}

const FEE_HISTORY_MAX_REQUEST_CHUNK: usize = 1023;

#[async_trait::async_trait]
impl EthFeeProvider for DynProvider {
    async fn base_fee_history(
        &self,
        upto_block: u64,
        block_count: u64,
    ) -> anyhow::Result<Vec<BaseFees>> {
        let mut history = Vec::with_capacity(block_count as usize);
        let from_block = upto_block.saturating_sub(block_count - 1);

        // Here we are requesting `fee_history` from blocks
        // `[from_block; upto_block]` in chunks of size `FEE_HISTORY_MAX_REQUEST_CHUNK`
        // starting from the oldest block.
        for chunk_start in (from_block..=upto_block).step_by(FEE_HISTORY_MAX_REQUEST_CHUNK) {
            let chunk_end = (chunk_start + FEE_HISTORY_MAX_REQUEST_CHUNK as u64).min(upto_block);
            let chunk_size = chunk_end - chunk_start + 1;

            let fee_history = self
                .get_fee_history(chunk_size, chunk_end.into(), &[])
                .await?;

            if fee_history.oldest_block != chunk_start {
                anyhow::bail!(
                    "unexpected `oldest_block`, expected: {chunk_start}, got {}",
                    fee_history.oldest_block
                );
            }

            // // The following commented checks are useful but they don't work with anvil
            // // (anvil returns inconsistent response if data for some block is missing).
            // if fee_history.base_fee_per_gas.len() != chunk_size as usize + 1 {
            //     anyhow::bail!(
            //         "unexpected `base_fee_per_gas.len()`, expected: {}, got {}",
            //         chunk_size + 1,
            //         fee_history.base_fee_per_gas.len()
            //     );
            // }
            //
            // // Per specification, the values should always be provided, and must be 0 for blocks
            // // prior to EIP-4844.
            // // https://ethereum.github.io/execution-apis/api-documentation/
            // if fee_history.base_fee_per_blob_gas.len() != chunk_size as usize + 1 {
            //     anyhow::bail!(
            //         "unexpected `base_fee_per_blob_gas.len()`, expected: {}, got {}",
            //         chunk_size + 1,
            //         fee_history.base_fee_per_blob_gas.len()
            //     );
            // }

            // We take `chunk_size` entries and drop data for the block after `chunk_end`.
            for (base_fee_per_gas, base_fee_per_blob_gas) in fee_history
                .base_fee_per_gas
                .into_iter()
                .zip(fee_history.base_fee_per_blob_gas)
                .take(chunk_size as usize)
            {
                let fees = BaseFees {
                    base_fee_per_gas,
                    base_fee_per_blob_gas,
                };
                history.push(fees)
            }
        }

        Ok(history)
    }
}
