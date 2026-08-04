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
//! [`ConfigOverrides::apply_to`].

use alloy::primitives::U256;
use zksync_os_storage_api::BlockContext;

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
    /// Apply any present overrides on top of an existing [`BlockContext`].
    ///
    /// When `base_fee` is overridden but `native_price` is not, `native_price`
    /// is automatically derived as `base_fee / NATIVE_PER_GAS` to maintain the
    /// invariant the bootloader expects (`basefee = native_price × native_per_gas`).
    ///
    /// `pubdata_price` defaults to 0 unless explicitly overridden because the
    /// base fee already includes all extra costs (L1 pubdata price,
    /// infrastructure costs, etc.).
    pub fn apply_to(&self, ctx: &mut BlockContext) {
        if let Some(base_fee) = self.base_fee {
            ctx.eip1559_basefee = base_fee;
            if self.native_price.is_none() {
                ctx.native_price = base_fee / U256::from(NATIVE_PER_GAS);
            }
        }
        ctx.pubdata_price = self.pubdata_price.unwrap_or(U256::ZERO);
        if let Some(native_price) = self.native_price {
            ctx.native_price = native_price;
        }
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
