use alloy::consensus::constants::GWEI_TO_WEI;
use alloy::primitives::Address;
use secrecy::SecretString;
use std::marker::PhantomData;
use std::time::Duration;

/// Configuration of L1 sender.
#[derive(Clone, Debug)]
pub struct L1SenderConfig<Input> {
    /// Private key to operate from.
    /// Depending on the mode, this can be a commit/prove/execute operator.
    pub operator_pk: SecretString,

    /// Max fee per gas we are willing to spend (in gwei).
    pub max_fee_per_gas_gwei: u64,

    /// Max priority fee per gas we are willing to spend (in gwei).
    pub max_priority_fee_per_gas_gwei: u64,

    /// Max number of commands (to commit/prove/execute one batch) to be processed at a time.
    pub command_limit: usize,

    /// How often to poll L1 for new blocks.
    pub poll_interval: Duration,

    /// If true, use eth_call to simulate the transaction instead of sending it onchain.
    /// The result will be logged and the batch will be passed downstream regardless of the simulation result.
    pub dry_run: bool,

    /// Optional operator address to use in dry-run mode.
    /// If set, this address will be used for eth_call simulation instead of deriving from operator_pk.
    /// This allows running dry-run mode without access to private keys.
    pub dry_run_operator_address: Option<Address>,

    pub phantom_data: PhantomData<Input>,
}

impl<T> L1SenderConfig<T> {
    /// Max fee per gas we are willing to spend (in wei).
    pub fn max_fee_per_gas(&self) -> u128 {
        self.max_fee_per_gas_gwei as u128 * (GWEI_TO_WEI as u128)
    }

    /// Max priority fee per gas we are willing to spend (in wei).
    pub fn max_priority_fee_per_gas(&self) -> u128 {
        self.max_priority_fee_per_gas_gwei as u128 * (GWEI_TO_WEI as u128)
    }
}
