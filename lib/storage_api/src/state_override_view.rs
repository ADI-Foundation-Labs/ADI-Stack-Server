use std::collections::HashMap;

use crate::ViewState;
use alloy::primitives::{Address, B256, U256, ruint::aliases::B160};
use zk_ee::common_structs::derive_flat_storage_key;
use zk_os_api::helpers::{set_properties_balance, set_properties_code, set_properties_nonce};
use zk_os_basic_system::system_implementation::flat_storage_model::{
    ACCOUNT_PROPERTIES_STORAGE_ADDRESS, AccountProperties, address_into_special_storage_key,
};
use zksync_os_interface::traits::{PreimageSource, ReadStorage};

#[derive(Debug, Clone, Default)]
pub struct AccountViewOverride {
    pub balance: Option<U256>,
    pub nonce: Option<u64>,
    pub code: Option<Vec<u8>>,
}

/// A `ViewState` wrapper that overrides specific storage slots and/or account
/// properties (balance/nonce/code). All other reads/preimage lookups delegate to the inner state.
#[derive(Debug, Clone)]
pub struct OverriddenStateView<V: ViewState> {
    inner: V,
    // direct storage slot overrides (flat keys)
    overrides: HashMap<B256, B256>,
    // override for the account properties key -> resulting account properties hash
    account_key_to_hash: HashMap<B256, B256>,
    // preimage overrides: hash -> preimage bytes (for account props and bytecode)
    preimage_overrides: HashMap<B256, Vec<u8>>,
}

impl<V: ViewState> OverriddenStateView<V> {
    pub fn new(
        inner: V,
        overrides: HashMap<B256, B256>,
        account_overrides: HashMap<Address, AccountViewOverride>,
    ) -> Self {
        let mut account_key_to_hash = HashMap::new();
        let mut preimage_overrides: HashMap<B256, Vec<u8>> = HashMap::new();

        // Precompute account property hashes and preimages for all overridden accounts.
        for (address, acc_override) in account_overrides.into_iter() {
            if acc_override.balance.is_none()
                && acc_override.nonce.is_none()
                && acc_override.code.is_none()
            {
                continue;
            }

            let mut base: AccountProperties =
                inner.clone().get_account(address).unwrap_or_default();

            if let Some(nonce) = acc_override.nonce {
                set_properties_nonce(&mut base, nonce);
            }

            if let Some(balance) = acc_override.balance {
                set_properties_balance(&mut base, balance);
            }

            if let Some(code) = acc_override.code {
                let bytecode_preimage = set_properties_code(&mut base, &code);
                // Map bytecode hash -> preimage
                let bytecode_hash_b256: B256 = base.bytecode_hash.as_u8_array().into();
                preimage_overrides.insert(bytecode_hash_b256, bytecode_preimage);
            }

            // Compute and store account properties preimage and its hash
            let acc_hash = base.compute_hash();
            let acc_hash_b256: B256 = acc_hash.as_u8_array().into();
            preimage_overrides.insert(acc_hash_b256, base.encoding().to_vec());

            // Compute flat storage key for account properties of this address
            let key = derive_flat_storage_key(
                &ACCOUNT_PROPERTIES_STORAGE_ADDRESS,
                &address_into_special_storage_key(&B160::from_be_bytes(address.into_array())),
            );
            account_key_to_hash.insert(B256::from(key.as_u8_array()), acc_hash_b256);
        }

        Self {
            inner,
            overrides,
            account_key_to_hash,
            preimage_overrides,
        }
    }
}

impl<V: ViewState> ReadStorage for OverriddenStateView<V> {
    fn read(&mut self, key: B256) -> Option<B256> {
        if let Some(val) = self.overrides.get(&key) {
            return Some(*val);
        }

        if let Some(val) = self.account_key_to_hash.get(&key) {
            return Some(*val);
        }

        self.inner.read(key)
    }
}

impl<V: ViewState> PreimageSource for OverriddenStateView<V> {
    fn get_preimage(&mut self, hash: B256) -> Option<Vec<u8>> {
        if let Some(bytes) = self.preimage_overrides.get(&hash) {
            return Some(bytes.clone());
        }

        self.inner.get_preimage(hash)
    }
}
