use alloy::primitives::{B256, U256, keccak256};
use alloy::sol;
use alloy::sol_types::SolValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------- Solidity ABI type for on-chain encoding ----------

sol! {
    struct IAvailBridgeMerkleProofInput {
        bytes32[] dataRootProof;
        bytes32[] leafProof;
        bytes32 rangeHash;
        uint256 dataRootIndex;
        bytes32 blobRoot;
        bytes32 bridgeRoot;
        bytes32 leaf;
        uint256 leafIndex;
    }
}

// ---------- Rust data types ----------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AvailMerkleProofInput {
    pub data_root_proof: Vec<B256>,
    pub leaf_proof: Vec<B256>,
    pub range_hash: B256,
    pub data_root_index: U256,
    pub blob_root: B256,
    pub bridge_root: B256,
    pub leaf: B256,
    pub leaf_index: U256,
}

impl From<AvailMerkleProofInput> for IAvailBridgeMerkleProofInput {
    fn from(value: AvailMerkleProofInput) -> Self {
        Self {
            dataRootProof: value.data_root_proof,
            leafProof: value.leaf_proof,
            rangeHash: value.range_hash,
            dataRootIndex: value.data_root_index,
            blobRoot: value.blob_root,
            bridgeRoot: value.bridge_root,
            leaf: value.leaf,
            leafIndex: value.leaf_index,
        }
    }
}

impl From<&AvailMerkleProofInput> for IAvailBridgeMerkleProofInput {
    fn from(value: &AvailMerkleProofInput) -> Self {
        Self {
            dataRootProof: value.data_root_proof.clone(),
            leafProof: value.leaf_proof.clone(),
            rangeHash: value.range_hash,
            dataRootIndex: value.data_root_index,
            blobRoot: value.blob_root,
            bridgeRoot: value.bridge_root,
            leaf: value.leaf,
            leafIndex: value.leaf_index,
        }
    }
}

impl From<IAvailBridgeMerkleProofInput> for AvailMerkleProofInput {
    fn from(value: IAvailBridgeMerkleProofInput) -> Self {
        Self {
            data_root_proof: value.dataRootProof,
            leaf_proof: value.leafProof,
            range_hash: value.rangeHash,
            data_root_index: value.dataRootIndex,
            blob_root: value.blobRoot,
            bridge_root: value.bridgeRoot,
            leaf: value.leaf,
            leaf_index: value.leafIndex,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AvailDaData {
    pub state_diff_hash: B256,
    pub merkle_proof_input: AvailMerkleProofInput,
}

#[derive(Debug, Error)]
pub enum AvailDaError {
    #[error("invalid operator_da_input length, expected at least 32 bytes, got {0}")]
    OperatorInputTooShort(usize),
    #[error("failed to decode Avail merkle proof input: {0}")]
    DecodeFailed(String),
    #[error("invalid Avail leaf: expected keccak(pubdata)={expected:?}, got {actual:?}")]
    LeafMismatch { expected: B256, actual: B256 },
}

// ---------- Encode / decode / validate ----------

#[must_use]
pub fn build_avail_da_commitment(state_diff_hash: B256, leaf: B256) -> B256 {
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(state_diff_hash.as_slice());
    bytes.extend_from_slice(leaf.as_slice());
    keccak256(bytes)
}

#[must_use]
pub fn encode_avail_operator_da_input(
    state_diff_hash: B256,
    merkle_proof_input: &AvailMerkleProofInput,
) -> Vec<u8> {
    let mut operator_da_input = Vec::with_capacity(32);
    operator_da_input.extend_from_slice(state_diff_hash.as_slice());
    operator_da_input.extend(IAvailBridgeMerkleProofInput::from(merkle_proof_input).abi_encode());
    operator_da_input
}

pub fn decode_avail_operator_da_input(
    operator_da_input: &[u8],
) -> Result<AvailDaData, AvailDaError> {
    if operator_da_input.len() < 32 {
        return Err(AvailDaError::OperatorInputTooShort(operator_da_input.len()));
    }

    let state_diff_hash = B256::from_slice(&operator_da_input[..32]);
    let merkle_proof_input = IAvailBridgeMerkleProofInput::abi_decode(&operator_da_input[32..])
        .map_err(|err| AvailDaError::DecodeFailed(err.to_string()))?
        .into();
    Ok(AvailDaData {
        state_diff_hash,
        merkle_proof_input,
    })
}

pub fn validate_leaf_matches_pubdata(pubdata: &[u8], leaf: B256) -> Result<(), AvailDaError> {
    let expected_leaf = keccak256(pubdata);
    if expected_leaf != leaf {
        return Err(AvailDaError::LeafMismatch {
            expected: expected_leaf,
            actual: leaf,
        });
    }
    Ok(())
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_merkle_proof_input(leaf: B256) -> AvailMerkleProofInput {
        AvailMerkleProofInput {
            data_root_proof: vec![B256::ZERO],
            leaf_proof: vec![B256::from(U256::from(11u64))],
            range_hash: B256::from(U256::from(22u64)),
            data_root_index: U256::from(33u64),
            blob_root: B256::from(U256::from(44u64)),
            bridge_root: B256::from(U256::from(55u64)),
            leaf,
            leaf_index: U256::from(66u64),
        }
    }

    #[test]
    fn encode_decode_operator_da_input_roundtrip() {
        let state_diff_hash = B256::from(U256::from(1u64));
        let leaf = B256::from(U256::from(2u64));
        let input = sample_merkle_proof_input(leaf);
        let encoded = encode_avail_operator_da_input(state_diff_hash, &input);
        let decoded = decode_avail_operator_da_input(&encoded).unwrap();

        assert_eq!(decoded.state_diff_hash, state_diff_hash);
        assert_eq!(decoded.merkle_proof_input, input);
    }

    #[test]
    fn da_commitment_is_state_diff_hash_and_leaf_keccak() {
        let state_diff_hash = B256::from(U256::from(123u64));
        let leaf = B256::from(U256::from(456u64));
        let mut expected = Vec::new();
        expected.extend_from_slice(state_diff_hash.as_slice());
        expected.extend_from_slice(leaf.as_slice());

        assert_eq!(
            build_avail_da_commitment(state_diff_hash, leaf),
            keccak256(expected)
        );
    }

    #[test]
    fn leaf_validation_checks_pubdata_hash() {
        let pubdata = b"hello-avail";
        let correct_leaf = keccak256(pubdata);
        validate_leaf_matches_pubdata(pubdata, correct_leaf).unwrap();

        let err = validate_leaf_matches_pubdata(pubdata, B256::ZERO).unwrap_err();
        assert!(matches!(err, AvailDaError::LeafMismatch { .. }));
    }
}
