use crate::GasAdjuster;

pub trait PubdataPriceProvider: Send + Sync + 'static {
    fn pubdata_price(&self) -> u128;
}

impl PubdataPriceProvider for GasAdjuster {
    fn pubdata_price(&self) -> u128 {
        self.pubdata_price_inner()
    }
}

#[derive(Debug)]
pub struct PanickingPubdataPriceProvider;

impl PubdataPriceProvider for PanickingPubdataPriceProvider {
    fn pubdata_price(&self) -> u128 {
        panic!("PanickingPubdataPriceProvider should not be called");
    }
}
