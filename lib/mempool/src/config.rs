/// Default transaction fee cap: 30 ETH in wei
pub const DEFAULT_TX_FEE_CAP: u128 = 30_000_000_000_000_000_000;

pub struct TxValidatorConfig {
    /// Max input size of a transaction to be accepted by mempool
    pub max_input_bytes: usize,
    /// Maximum transaction fee cap in wei.
    /// Transactions with fees exceeding this will be rejected by the mempool.
    /// Set to 0 to disable the cap.
    pub tx_fee_cap: u128,
}
