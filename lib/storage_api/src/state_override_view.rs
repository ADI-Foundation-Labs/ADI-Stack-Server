use std::collections::HashMap;

use crate::ViewState;
use alloy::primitives::B256;
use zksync_os_interface::traits::{PreimageSource, ReadStorage};

/// A `ViewState` wrapper that overrides specific storage slots, delegating all other
/// reads/preimage lookups to the inner state view.
#[derive(Debug, Clone)]
pub struct OverriddenStateView<V: ViewState> {
    inner: V,
    overrides: HashMap<B256, B256>,
}

impl<V: ViewState> OverriddenStateView<V> {
    pub fn new(inner: V, overrides: HashMap<B256, B256>) -> Self {
        Self { inner, overrides }
    }
}

impl<V: ViewState> ReadStorage for OverriddenStateView<V> {
    fn read(&mut self, key: B256) -> Option<B256> {
        if let Some(val) = self.overrides.get(&key) {
            return Some(*val);
        }
        self.inner.read(key)
    }
}

impl<V: ViewState> PreimageSource for OverriddenStateView<V> {
    fn get_preimage(&mut self, hash: B256) -> Option<Vec<u8>> {
        self.inner.get_preimage(hash)
    }
}
