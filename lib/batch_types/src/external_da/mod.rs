use alloy::primitives::B256;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zksync_os_types::PubdataMode;

pub(crate) mod providers;

pub use providers::avail::{
    AvailDaData, AvailDaError, AvailMerkleProofInput, build_avail_da_commitment,
    decode_avail_operator_da_input, encode_avail_operator_da_input, validate_leaf_matches_pubdata,
};
pub use providers::local_files::{
    LocalFilesDaData, LocalFilesDaError, build_local_files_da_commitment,
    decode_local_files_operator_da_input, encode_local_files_operator_da_input,
    validate_pubdata_hash_matches_pubdata,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExternalDaProvider {
    Avail,
    LocalFiles,
}

impl ExternalDaProvider {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Avail => "avail",
            Self::LocalFiles => "local_files",
        }
    }

    pub fn parse(provider: &str) -> Result<Self, ExternalDaError> {
        if provider.eq_ignore_ascii_case("avail") {
            return Ok(Self::Avail);
        }
        if provider.eq_ignore_ascii_case("local_files") {
            return Ok(Self::LocalFiles);
        }
        Err(ExternalDaError::UnsupportedProvider(provider.to_owned()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExternalDaData {
    Avail(AvailDaData),
    LocalFiles(LocalFilesDaData),
}

impl ExternalDaData {
    #[must_use]
    pub fn provider(&self) -> ExternalDaProvider {
        match self {
            Self::Avail(_) => ExternalDaProvider::Avail,
            Self::LocalFiles(_) => ExternalDaProvider::LocalFiles,
        }
    }
}

#[derive(Debug, Error)]
pub enum ExternalDaError {
    #[error("missing external DA payload for pubdata mode {0:?}")]
    MissingData(PubdataMode),
    #[error("external DA payload is not allowed for pubdata mode {0:?}")]
    UnexpectedData(PubdataMode),
    #[error("unsupported external DA provider `{0}`")]
    UnsupportedProvider(String),
    #[error("external DA commitment mismatch: expected {expected:?}, got {actual:?}")]
    CommitmentMismatch { expected: B256, actual: B256 },
    #[error("no registered external DA codec could decode the payload: {0}")]
    NoMatchingCodec(String),
    #[error("invalid Avail payload: {0}")]
    Avail(#[from] AvailDaError),
    #[error("invalid Local Files payload: {0}")]
    LocalFiles(#[from] LocalFilesDaError),
}

// ---------- Public API ----------

pub fn decode_external_da_data_for_mode(
    pubdata_mode: PubdataMode,
    operator_da_input: &[u8],
) -> Result<Option<ExternalDaData>, ExternalDaError> {
    when_external_da(pubdata_mode, || try_decode(operator_da_input))
}

pub fn decode_external_da_data_with_commitment_for_mode(
    pubdata_mode: PubdataMode,
    da_commitment: B256,
    operator_da_input: &[u8],
) -> Result<Option<ExternalDaData>, ExternalDaError> {
    when_external_da(pubdata_mode, || {
        try_validate_commitment(da_commitment, operator_da_input)?;
        try_decode(operator_da_input)
    })
}

pub fn compute_external_da_fields_for_mode(
    pubdata: &[u8],
    pubdata_mode: PubdataMode,
    external_da_data: Option<ExternalDaData>,
) -> Result<Option<(B256, Vec<u8>)>, ExternalDaError> {
    if !pubdata_mode.uses_external_da() {
        if external_da_data.is_some() {
            return Err(ExternalDaError::UnexpectedData(pubdata_mode));
        }
        return Ok(None);
    }

    let data = external_da_data.ok_or(ExternalDaError::MissingData(pubdata_mode))?;
    let (da_commitment, operator_da_input) = compute_fields(pubdata, data)?;
    Ok(Some((da_commitment, operator_da_input)))
}

pub fn validate_external_da_commitment_for_mode(
    pubdata_mode: PubdataMode,
    da_commitment: B256,
    operator_da_input: &[u8],
) -> Result<(), ExternalDaError> {
    if !pubdata_mode.uses_external_da() {
        return Ok(());
    }
    try_validate_commitment(da_commitment, operator_da_input)
}

// ---------- Internal dispatch ----------

/// Runs `f` only when `pubdata_mode` uses external DA, returning `Ok(None)` otherwise.
fn when_external_da<T>(
    pubdata_mode: PubdataMode,
    f: impl FnOnce() -> Result<T, ExternalDaError>,
) -> Result<Option<T>, ExternalDaError> {
    if !pubdata_mode.uses_external_da() {
        return Ok(None);
    }
    f().map(Some)
}

fn compute_fields(
    pubdata: &[u8],
    data: ExternalDaData,
) -> Result<(B256, Vec<u8>), ExternalDaError> {
    match data {
        ExternalDaData::Avail(avail_data) => {
            let state_diff_hash = avail_data.state_diff_hash;
            let leaf = avail_data.merkle_proof_input.leaf;
            validate_leaf_matches_pubdata(pubdata, leaf)?;
            let da_commitment = build_avail_da_commitment(state_diff_hash, leaf);
            let operator_da_input =
                encode_avail_operator_da_input(state_diff_hash, &avail_data.merkle_proof_input);
            Ok((da_commitment, operator_da_input))
        }
        ExternalDaData::LocalFiles(local_files_data) => {
            validate_pubdata_hash_matches_pubdata(pubdata, local_files_data.pubdata_hash)?;
            let da_commitment = build_local_files_da_commitment(
                local_files_data.state_diff_hash,
                local_files_data.pubdata_hash,
            );
            let operator_da_input = encode_local_files_operator_da_input(
                local_files_data.state_diff_hash,
                local_files_data.pubdata_hash,
            );
            Ok((da_commitment, operator_da_input))
        }
    }
}

/// Try decoding `operator_da_input` with each provider, collecting all errors on failure.
fn try_decode(operator_da_input: &[u8]) -> Result<ExternalDaData, ExternalDaError> {
    if let Ok(data) = decode_avail_operator_da_input(operator_da_input) {
        return Ok(ExternalDaData::Avail(data));
    }
    if let Ok(data) = decode_local_files_operator_da_input(operator_da_input) {
        return Ok(ExternalDaData::LocalFiles(data));
    }
    // Both failed — rebuild errors for diagnostics.
    let avail_err = decode_avail_operator_da_input(operator_da_input).unwrap_err();
    let local_err = decode_local_files_operator_da_input(operator_da_input).unwrap_err();
    Err(ExternalDaError::NoMatchingCodec(format!(
        "avail: {avail_err}; local_files: {local_err}"
    )))
}

/// Try validating the commitment with each provider, collecting all errors on failure.
fn try_validate_commitment(
    da_commitment: B256,
    operator_da_input: &[u8],
) -> Result<(), ExternalDaError> {
    if let Ok(avail_data) = decode_avail_operator_da_input(operator_da_input) {
        let expected = build_avail_da_commitment(
            avail_data.state_diff_hash,
            avail_data.merkle_proof_input.leaf,
        );
        return ensure_commitment_matches(expected, da_commitment);
    }
    if let Ok(local_data) = decode_local_files_operator_da_input(operator_da_input) {
        let expected =
            build_local_files_da_commitment(local_data.state_diff_hash, local_data.pubdata_hash);
        return ensure_commitment_matches(expected, da_commitment);
    }
    let avail_err = decode_avail_operator_da_input(operator_da_input).unwrap_err();
    let local_err = decode_local_files_operator_da_input(operator_da_input).unwrap_err();
    Err(ExternalDaError::NoMatchingCodec(format!(
        "avail: {avail_err}; local_files: {local_err}"
    )))
}

fn ensure_commitment_matches(expected: B256, actual: B256) -> Result<(), ExternalDaError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ExternalDaError::CommitmentMismatch { expected, actual })
    }
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_parse_is_case_insensitive() {
        assert_eq!(
            ExternalDaProvider::parse("AvAiL").unwrap(),
            ExternalDaProvider::Avail
        );
        assert_eq!(
            ExternalDaProvider::parse("local_files").unwrap(),
            ExternalDaProvider::LocalFiles
        );
    }

    #[test]
    fn decode_invalid_payload_reports_all_codec_errors() {
        let err = decode_external_da_data_for_mode(PubdataMode::External, &[0u8; 3]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("avail"), "error should mention avail: {msg}");
        assert!(
            msg.contains("local_files"),
            "error should mention local_files: {msg}"
        );
    }
}
