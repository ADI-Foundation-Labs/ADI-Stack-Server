//! Runtime fee overrides shared between the block producer and the JSON-RPC layer.
//!
//! `ConfigOverrides` carries operator-settable replacements for fee-related fields
//! (`base_fee`, `pubdata_price`, `native_price`) and for the L1 sender's gas fee caps
//! (`l1_sender_max_*`). The type is consumed by:
//!
//! - `zksync_os_rpc_private` -- exposes it via the `config.setOverrides` private RPC.
//! - `zksync_os_sequencer` -- applies it when producing a new block.
//! - `zksync_os_l1_sender` -- applies the `l1_sender_max_*` caps when sending L1 txs.
//! - `zksync_os_rpc` -- applies it when answering fee-exposing endpoints
//!   (`eth_gasPrice`, `eth_feeHistory`, `eth_estimateGas`, `eth_call`, tracers) so
//!   clients see the new values immediately, without waiting for the next block.
//!
//! Keeping a single helper here guarantees the producer and the RPC read path merge
//! overrides in lockstep -- adding a new override field only requires touching
//! [`ConfigOverrides::apply_to_fee_params`].

use alloy::primitives::U256;
use zksync_os_storage_api::BlockContext;
use zksync_os_types::FeeParams;

/// Runtime overrides for fee-related fields of a produced block.
///
/// A `None` field means "don't override"; a `Some(_)` field replaces the corresponding
/// value in the target `BlockContext`.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ConfigOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_fee: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pubdata_price: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_price: Option<U256>,
    /// Max fee per gas for L1 sender transactions (in wei). Values are saturated down to
    /// u128 at the alloy tx boundary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_sender_max_fee_per_gas_wei: Option<U256>,
    /// Max priority fee per gas for L1 sender transactions (in wei).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_sender_max_priority_fee_per_gas_wei: Option<U256>,
    /// Max fee per blob gas for L1 sender transactions (in wei).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_sender_max_fee_per_blob_gas_wei: Option<U256>,
}

/// The bootloader requires `basefee / native_price == NATIVE_PER_GAS`.
/// This must match the `native_per_gas` config value (default 100).
const NATIVE_PER_GAS: u64 = 100;

impl ConfigOverrides {
    /// Applies any present overrides on top of [`FeeParams`].
    ///
    /// When `base_fee` is overridden but `native_price` is not, `native_price`
    /// is automatically derived as `base_fee / NATIVE_PER_GAS` to maintain the
    /// invariant the bootloader expects (`basefee = native_price × native_per_gas`).
    ///
    /// `pubdata_price` defaults to 0 unless explicitly overridden because the
    /// base fee already includes all extra costs (L1 pubdata price,
    /// infrastructure costs, etc.).
    pub fn apply_to_fee_params(&self, fee_params: &mut FeeParams) {
        if let Some(base_fee) = self.base_fee {
            fee_params.eip1559_basefee = base_fee;
            if self.native_price.is_none() {
                fee_params.native_price = base_fee / U256::from(NATIVE_PER_GAS);
            }
        }
        fee_params.pubdata_price = self.pubdata_price.unwrap_or(U256::ZERO);
        if let Some(native_price) = self.native_price {
            fee_params.native_price = native_price;
        }
    }

    /// Applies any present overrides on top of an existing [`BlockContext`].
    pub fn apply_to(&self, ctx: &mut BlockContext) {
        let mut fee_params = FeeParams {
            eip1559_basefee: ctx.eip1559_basefee,
            native_price: ctx.native_price,
            pubdata_price: ctx.pubdata_price,
        };
        self.apply_to_fee_params(&mut fee_params);
        ctx.eip1559_basefee = fee_params.eip1559_basefee;
        ctx.native_price = fee_params.native_price;
        ctx.pubdata_price = fee_params.pubdata_price;
    }

    /// Merge with another `ConfigOverrides`, preferring values from `self`.
    pub fn merge_with(self, fallback: Self) -> Self {
        Self {
            base_fee: self.base_fee.or(fallback.base_fee),
            pubdata_price: self.pubdata_price.or(fallback.pubdata_price),
            native_price: self.native_price.or(fallback.native_price),
            l1_sender_max_fee_per_gas_wei: self
                .l1_sender_max_fee_per_gas_wei
                .or(fallback.l1_sender_max_fee_per_gas_wei),
            l1_sender_max_priority_fee_per_gas_wei: self
                .l1_sender_max_priority_fee_per_gas_wei
                .or(fallback.l1_sender_max_priority_fee_per_gas_wei),
            l1_sender_max_fee_per_blob_gas_wei: self
                .l1_sender_max_fee_per_blob_gas_wei
                .or(fallback.l1_sender_max_fee_per_blob_gas_wei),
        }
    }

    /// Remove overrides by field name. Unknown names are logged and ignored.
    pub fn remove_fields(mut self, fields: Vec<String>) -> Self {
        for field in fields {
            match field.as_str() {
                "base_fee" => self.base_fee = None,
                "pubdata_price" => self.pubdata_price = None,
                "native_price" => self.native_price = None,
                "l1_sender_max_fee_per_gas_wei" => self.l1_sender_max_fee_per_gas_wei = None,
                "l1_sender_max_priority_fee_per_gas_wei" => {
                    self.l1_sender_max_priority_fee_per_gas_wei = None
                }
                "l1_sender_max_fee_per_blob_gas_wei" => {
                    self.l1_sender_max_fee_per_blob_gas_wei = None
                }
                _ => tracing::warn!("Unknown field to remove: {}", field),
            }
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fee_params(base_fee: u64, native_price: u64, pubdata_price: u64) -> FeeParams {
        FeeParams {
            eip1559_basefee: U256::from(base_fee),
            native_price: U256::from(native_price),
            pubdata_price: U256::from(pubdata_price),
        }
    }

    #[test]
    fn empty_overrides_keep_prices_but_zero_pubdata() {
        let mut params = fee_params(552, 5, 77);
        ConfigOverrides::default().apply_to_fee_params(&mut params);
        assert_eq!(params, fee_params(552, 5, 0));
    }

    #[test]
    fn base_fee_override_derives_native_price() {
        let mut params = fee_params(552, 5, 77);
        let overrides = ConfigOverrides {
            base_fee: Some(U256::from(1_000)),
            ..Default::default()
        };
        overrides.apply_to_fee_params(&mut params);
        assert_eq!(params, fee_params(1_000, 1_000 / NATIVE_PER_GAS, 0));
    }

    #[test]
    fn explicit_native_price_wins_over_derivation() {
        let mut params = fee_params(552, 5, 77);
        let overrides = ConfigOverrides {
            base_fee: Some(U256::from(1_000)),
            native_price: Some(U256::from(3)),
            pubdata_price: Some(U256::from(9)),
            ..Default::default()
        };
        overrides.apply_to_fee_params(&mut params);
        assert_eq!(params, fee_params(1_000, 3, 9));
    }

    /// The block and the mempool are fed from the two entry points, so they must not drift.
    #[test]
    fn apply_to_matches_apply_to_fee_params() {
        let overrides = ConfigOverrides {
            base_fee: Some(U256::from(1_000)),
            ..Default::default()
        };
        let mut params = fee_params(552, 5, 77);
        overrides.apply_to_fee_params(&mut params);

        let mut ctx = BlockContext {
            eip1559_basefee: U256::from(552),
            native_price: U256::from(5),
            pubdata_price: U256::from(77),
            ..Default::default()
        };
        overrides.apply_to(&mut ctx);

        assert_eq!(ctx.eip1559_basefee, params.eip1559_basefee);
        assert_eq!(ctx.native_price, params.native_price);
        assert_eq!(ctx.pubdata_price, params.pubdata_price);
    }
}
