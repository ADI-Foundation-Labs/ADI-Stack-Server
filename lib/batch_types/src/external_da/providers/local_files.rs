use alloy::primitives::{B256, keccak256};
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------- Rust data types ----------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalFilesDaData {
    pub state_diff_hash: B256,
    pub pubdata_hash: B256,
    #[serde(default)]
    pub file_path: Option<String>,
}

#[derive(Debug, Error)]
pub enum LocalFilesDaError {
    #[error("invalid operator_da_input length, expected 64 bytes, got {0}")]
    InvalidOperatorInputLength(usize),
    #[error(
        "invalid local files pubdata hash: expected keccak(pubdata)={expected:?}, got {actual:?}"
    )]
    PubdataHashMismatch { expected: B256, actual: B256 },
}

// ---------- Encode / decode / validate ----------

#[must_use]
pub fn build_local_files_da_commitment(state_diff_hash: B256, pubdata_hash: B256) -> B256 {
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(state_diff_hash.as_slice());
    bytes.extend_from_slice(pubdata_hash.as_slice());
    keccak256(bytes)
}

#[must_use]
pub fn encode_local_files_operator_da_input(state_diff_hash: B256, pubdata_hash: B256) -> Vec<u8> {
    let mut operator_da_input = Vec::with_capacity(64);
    operator_da_input.extend_from_slice(state_diff_hash.as_slice());
    operator_da_input.extend_from_slice(pubdata_hash.as_slice());
    operator_da_input
}

pub fn decode_local_files_operator_da_input(
    operator_da_input: &[u8],
) -> Result<LocalFilesDaData, LocalFilesDaError> {
    if operator_da_input.len() != 64 {
        return Err(LocalFilesDaError::InvalidOperatorInputLength(
            operator_da_input.len(),
        ));
    }

    let state_diff_hash = B256::from_slice(&operator_da_input[..32]);
    let pubdata_hash = B256::from_slice(&operator_da_input[32..64]);
    Ok(LocalFilesDaData {
        state_diff_hash,
        pubdata_hash,
        file_path: None,
    })
}

pub fn validate_pubdata_hash_matches_pubdata(
    pubdata: &[u8],
    pubdata_hash: B256,
) -> Result<(), LocalFilesDaError> {
    let expected = keccak256(pubdata);
    if expected != pubdata_hash {
        return Err(LocalFilesDaError::PubdataHashMismatch {
            expected,
            actual: pubdata_hash,
        });
    }
    Ok(())
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::U256;

    #[test]
    fn encode_decode_operator_da_input_roundtrip() {
        let state_diff_hash = B256::from(U256::from(1u64));
        let pubdata_hash = B256::from(U256::from(2u64));
        let encoded = encode_local_files_operator_da_input(state_diff_hash, pubdata_hash);
        let decoded = decode_local_files_operator_da_input(&encoded).unwrap();
        assert_eq!(decoded.state_diff_hash, state_diff_hash);
        assert_eq!(decoded.pubdata_hash, pubdata_hash);
        assert_eq!(decoded.file_path, None);
    }

    #[test]
    fn commitment_is_state_diff_hash_and_pubdata_hash_keccak() {
        let state_diff_hash = B256::from(U256::from(123u64));
        let pubdata_hash = B256::from(U256::from(456u64));
        let mut expected = Vec::new();
        expected.extend_from_slice(state_diff_hash.as_slice());
        expected.extend_from_slice(pubdata_hash.as_slice());

        assert_eq!(
            build_local_files_da_commitment(state_diff_hash, pubdata_hash),
            keccak256(expected)
        );
    }
}
