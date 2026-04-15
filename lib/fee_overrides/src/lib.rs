//! Runtime fee overrides shared between the block producer and the JSON-RPC layer.
//!
//! `ConfigOverrides` carries operator-settable replacements for fee-related fields
//! (`base_fee`, `pubdata_price`, `native_price`). The type is consumed by:
//!
//! - `zksync_os_rpc_private` -- exposes it via the `config.setOverrides` private RPC.
//! - `zksync_os_sequencer` -- applies it when producing a new block.
//! - `zksync_os_rpc` -- applies it when answering fee-exposing endpoints
//!   (`eth_gasPrice`, `eth_feeHistory`, `eth_estimateGas`, `eth_call`, tracers) so
//!   clients see the new values immediately, without waiting for the next block.
//!
//! Keeping a single helper here guarantees the producer and the RPC read path merge
//! overrides in lockstep -- adding a new override field only requires touching
//! [`ConfigOverrides::apply_to`].

use alloy::primitives::U256;
use zksync_os_interface::types::BlockContext;

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
}

impl ConfigOverrides {
    /// Apply any present overrides on top of an existing [`BlockContext`].
    pub fn apply_to(&self, ctx: &mut BlockContext) {
        if let Some(base_fee) = self.base_fee {
            ctx.eip1559_basefee = base_fee;
        }
        if let Some(pubdata_price) = self.pubdata_price {
            ctx.pubdata_price = pubdata_price;
        }
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
        }
    }

    /// Remove overrides by field name. Unknown names are logged and ignored.
    pub fn remove_fields(mut self, fields: Vec<String>) -> Self {
        for field in fields {
            match field.as_str() {
                "base_fee" => self.base_fee = None,
                "pubdata_price" => self.pubdata_price = None,
                "native_price" => self.native_price = None,
                _ => tracing::warn!("Unknown field to remove: {}", field),
            }
        }
        self
    }
}
