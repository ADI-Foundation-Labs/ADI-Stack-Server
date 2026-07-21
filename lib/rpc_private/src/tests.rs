#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use alloy::primitives::U256;

fn full_overrides() -> ConfigOverrides {
    ConfigOverrides {
        base_fee: Some(U256::from(1)),
        pubdata_price: Some(U256::from(2)),
        native_price: Some(U256::from(3)),
        l1_sender_max_fee_per_gas_wei: Some(U256::from(20_000_000_000u64)),
        l1_sender_max_priority_fee_per_gas_wei: Some(U256::from(10_000_000_000u64)),
        l1_sender_max_fee_per_blob_gas_wei: Some(U256::from(5_000_000_000u64)),
    }
}

#[test]
fn merge_prefers_self_over_fallback() {
    let this = ConfigOverrides {
        l1_sender_max_fee_per_gas_wei: Some(U256::from(42)),
        ..Default::default()
    };
    let merged = this.merge_with(full_overrides());
    assert_eq!(merged.l1_sender_max_fee_per_gas_wei, Some(U256::from(42)));
    assert_eq!(
        merged.l1_sender_max_priority_fee_per_gas_wei,
        Some(U256::from(10_000_000_000u64))
    );
    assert_eq!(
        merged.l1_sender_max_fee_per_blob_gas_wei,
        Some(U256::from(5_000_000_000u64))
    );
    assert_eq!(merged.base_fee, Some(U256::from(1)));
}

#[test]
fn remove_fields_clears_l1_sender_overrides() {
    let cleared = full_overrides().remove_fields(vec![
        "l1_sender_max_fee_per_gas_wei".to_string(),
        "l1_sender_max_priority_fee_per_gas_wei".to_string(),
        "l1_sender_max_fee_per_blob_gas_wei".to_string(),
    ]);
    assert_eq!(cleared.l1_sender_max_fee_per_gas_wei, None);
    assert_eq!(cleared.l1_sender_max_priority_fee_per_gas_wei, None);
    assert_eq!(cleared.l1_sender_max_fee_per_blob_gas_wei, None);
    assert_eq!(cleared.base_fee, Some(U256::from(1)));
}

#[test]
fn save_and_load_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config_overrides.json");
    let overrides = full_overrides();
    save_to_file(&overrides, &path).unwrap();
    let loaded = load_from_file(&path).unwrap();
    assert_eq!(
        loaded.l1_sender_max_fee_per_gas_wei,
        overrides.l1_sender_max_fee_per_gas_wei
    );
    assert_eq!(
        loaded.l1_sender_max_priority_fee_per_gas_wei,
        overrides.l1_sender_max_priority_fee_per_gas_wei
    );
    assert_eq!(
        loaded.l1_sender_max_fee_per_blob_gas_wei,
        overrides.l1_sender_max_fee_per_blob_gas_wei
    );
    assert_eq!(loaded.base_fee, overrides.base_fee);
    // atomic write must not leave a temp file behind
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(leftovers, vec!["config_overrides.json"]);
}

#[test]
fn none_fields_are_omitted_from_json() {
    let overrides = ConfigOverrides {
        l1_sender_max_fee_per_gas_wei: Some(U256::from(7)),
        ..Default::default()
    };
    let json = serde_json::to_string(&overrides).unwrap();
    assert_eq!(json, r#"{"l1_sender_max_fee_per_gas_wei":"0x7"}"#);
}

#[test]
fn unknown_and_missing_fields_are_tolerated_on_load() {
    let json = r#"{"base_fee":"0x1","some_future_field":true}"#;
    let overrides: ConfigOverrides = serde_json::from_str(json).unwrap();
    assert_eq!(overrides.base_fee, Some(U256::from(1)));
    assert_eq!(overrides.l1_sender_max_fee_per_gas_wei, None);
}
