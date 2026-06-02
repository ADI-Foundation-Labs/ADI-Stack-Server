use crate::config::ExternalDaConfig;
use alloy::primitives::{B256, U256, keccak256};
use anyhow::Context;
use base64::Engine;
use reqwest::header::HeaderValue;
use secrecy::ExposeSecret;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::time::{Duration, Instant};
use vise::{Buckets, Counter, EncodeLabelValue, Histogram, LabeledFamily, Metrics, Unit};
use zksync_os_batch_types::{
    AvailDaData, AvailMerkleProofInput, ExternalDaData, ExternalDaProvider, LocalFilesDaData,
};
use zksync_os_types::PubdataMode;

#[derive(Debug, Clone)]
pub struct ExternalDaRequest<'a> {
    pub chain_id: u64,
    pub sl_chain_id: u64,
    pub batch_number: u64,
    pub pubdata: &'a [u8],
}

// ---------- Metrics ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EncodeLabelValue)]
#[metrics(label = "operation", rename_all = "snake_case")]
enum ExternalDaOperation {
    Publish,
    FetchProof,
}

#[derive(Debug, Metrics)]
#[metrics(prefix = "external_da")]
struct ExternalDaMetrics {
    #[metrics(labels = ["operation"], unit = Unit::Seconds, buckets = Buckets::LATENCIES)]
    request_latency: LabeledFamily<ExternalDaOperation, Histogram<Duration>>,

    #[metrics(labels = ["operation"])]
    retries: LabeledFamily<ExternalDaOperation, Counter>,

    #[metrics(labels = ["operation"])]
    request_failures: LabeledFamily<ExternalDaOperation, Counter>,

    validation_failures: Counter,

    successful_batches: Counter,
}

#[vise::register]
static EXTERNAL_DA_METRICS: vise::Global<ExternalDaMetrics> = vise::Global::new();

// ---------- HTTP Client ----------

#[derive(Debug)]
pub struct ExternalDaHttpClient {
    client: reqwest::Client,
    config: ExternalDaConfig,
    provider: ExternalDaProvider,
}

impl ExternalDaHttpClient {
    pub async fn fetch_external_da_data(
        &self,
        request: &ExternalDaRequest<'_>,
    ) -> anyhow::Result<ExternalDaData> {
        let pubdata_hash = keccak256(request.pubdata);
        let batch_key = format!(
            "{}:{}:{}:{}",
            request.chain_id, request.sl_chain_id, request.batch_number, pubdata_hash
        );

        let publish_result = if let Some(url) = self.config.publish_url.as_deref() {
            let publish_payload = build_publish_payload(request, &batch_key, pubdata_hash);
            let publish_response = self
                .post_with_retry(
                    url,
                    &publish_payload,
                    request,
                    &batch_key,
                    ExternalDaOperation::Publish,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to publish batch {} pubdata to external DA",
                        request.batch_number
                    )
                })?;
            parse_da_data_from_response(self.provider, publish_response)
        } else {
            None
        };

        let da_data = if let Some(data) = publish_result {
            data
        } else {
            let proof_url = self
                .config
                .proof_url
                .as_deref()
                .context("external_da.proof_url must be set when publish response does not include proof payload")?;
            let proof_payload = build_proof_payload(request, &batch_key, pubdata_hash);
            let proof_response = self
                .post_with_retry(
                    proof_url,
                    &proof_payload,
                    request,
                    &batch_key,
                    ExternalDaOperation::FetchProof,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to fetch proof for external DA batch {}",
                        request.batch_number
                    )
                })?;
            parse_da_data_from_response(self.provider, proof_response).with_context(|| {
                format!(
                    "proof endpoint response does not contain `{}` external DA payload (expected direct payload or {{data}}/{{result}} wrapper)",
                    self.provider.as_str()
                )
            })?
        };

        if let Err(err) = validate_da_data(&self.config, self.provider, pubdata_hash, &da_data) {
            EXTERNAL_DA_METRICS.validation_failures.inc();
            return Err(err);
        }

        EXTERNAL_DA_METRICS.successful_batches.inc();
        Ok(da_data)
    }

    async fn post_with_retry<Payload: Serialize>(
        &self,
        url: &str,
        payload: &Payload,
        request: &ExternalDaRequest<'_>,
        batch_key: &str,
        operation: ExternalDaOperation,
    ) -> anyhow::Result<Value> {
        let max_attempts = self.config.max_retries.saturating_add(1);
        let mut attempt = 1u32;
        loop {
            let start = Instant::now();
            let result = self.post_json(url, payload, batch_key).await;
            EXTERNAL_DA_METRICS.request_latency[&operation].observe(start.elapsed());

            match result {
                Ok(value) => {
                    tracing::debug!(
                        chain_id = request.chain_id,
                        sl_chain_id = request.sl_chain_id,
                        batch_number = request.batch_number,
                        provider = self.provider.as_str(),
                        operation = ?operation,
                        attempt,
                        "external DA request succeeded"
                    );
                    return Ok(value);
                }
                Err(err) => {
                    EXTERNAL_DA_METRICS.request_failures[&operation].inc();
                    if attempt >= max_attempts {
                        return Err(err).with_context(|| {
                            format!(
                                "external DA request failed after {attempt} attempt(s), provider={}, url={url}, operation={operation:?}, batch={}",
                                self.provider.as_str(),
                                request.batch_number
                            )
                        });
                    }

                    EXTERNAL_DA_METRICS.retries[&operation].inc();
                    let backoff = Duration::from_millis(
                        self.config
                            .retry_backoff_ms
                            .saturating_mul(u64::from(attempt)),
                    );
                    tracing::warn!(
                        chain_id = request.chain_id,
                        sl_chain_id = request.sl_chain_id,
                        batch_number = request.batch_number,
                        provider = self.provider.as_str(),
                        operation = ?operation,
                        attempt,
                        max_attempts,
                        ?backoff,
                        error = %err,
                        "external DA request failed, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                }
            }
        }
    }

    async fn post_json<Payload: Serialize>(
        &self,
        url: &str,
        payload: &Payload,
        batch_key: &str,
    ) -> anyhow::Result<Value> {
        let response = self
            .client
            .post(url)
            .header("x-idempotency-key", batch_key)
            .header("x-zksync-da-batch-key", batch_key)
            .json(payload)
            .send()
            .await
            .with_context(|| format!("external DA POST failed (url={url})"))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .with_context(|| format!("failed to read external DA response body (url={url})"))?;

        if !status.is_success() {
            anyhow::bail!(
                "external DA request returned non-success status {status}; body={}",
                truncate_for_log(&body)
            );
        }

        serde_json::from_str(&body).with_context(|| {
            format!(
                "failed to deserialize external DA JSON response (url={url}); body={} ",
                truncate_for_log(&body)
            )
        })
    }
}

// ---------- Constructor ----------

pub fn build_external_da_client(
    config: &ExternalDaConfig,
    pubdata_mode: PubdataMode,
) -> anyhow::Result<Option<ExternalDaHttpClient>> {
    if !pubdata_mode.uses_external_da() {
        return Ok(None);
    }

    anyhow::ensure!(
        config.enabled,
        "external_da.enabled must be true when using external DA mode"
    );
    let provider = ExternalDaProvider::parse(&config.provider)?;

    anyhow::ensure!(
        config.publish_url.is_some() || config.proof_url.is_some(),
        "external DA requires at least one endpoint: set external_da.publish_url and/or external_da.proof_url"
    );

    let mut default_headers = reqwest::header::HeaderMap::new();
    if let Some(api_key) = &config.api_key {
        let mut api_key_value = HeaderValue::from_str(api_key.expose_secret())
            .context("external_da.api_key contains invalid header value")?;
        api_key_value.set_sensitive(true);
        default_headers.insert("x-api-key", api_key_value);
    }

    let client = reqwest::Client::builder()
        .default_headers(default_headers)
        .timeout(config.request_timeout)
        .build()
        .context("failed to build external DA HTTP client")?;

    Ok(Some(ExternalDaHttpClient {
        client,
        config: config.clone(),
        provider,
    }))
}

// ---------- Payload builders ----------

fn build_publish_payload(
    request: &ExternalDaRequest<'_>,
    batch_key: &str,
    pubdata_hash: B256,
) -> Value {
    serde_json::json!({
        "chainId": request.chain_id,
        "slChainId": request.sl_chain_id,
        "batchNumber": request.batch_number,
        "batchKey": batch_key,
        "pubdataHash": pubdata_hash,
        "pubdataBase64": base64::engine::general_purpose::STANDARD.encode(request.pubdata),
    })
}

fn build_proof_payload(
    request: &ExternalDaRequest<'_>,
    batch_key: &str,
    pubdata_hash: B256,
) -> Value {
    serde_json::json!({
        "chainId": request.chain_id,
        "slChainId": request.sl_chain_id,
        "batchNumber": request.batch_number,
        "batchKey": batch_key,
        "pubdataHash": pubdata_hash,
    })
}

// ---------- Response parsing ----------

fn parse_da_data_from_response(
    provider: ExternalDaProvider,
    response: Value,
) -> Option<ExternalDaData> {
    match provider {
        ExternalDaProvider::Avail => parse_response_with_wrappers::<AvailApiDaData>(&response)
            .map(|api| ExternalDaData::Avail(api.into())),
        ExternalDaProvider::LocalFiles => {
            parse_response_with_wrappers::<LocalFilesApiDaData>(&response)
                .map(|api| ExternalDaData::LocalFiles(api.into()))
        }
    }
}

fn parse_response_with_wrappers<T: serde::de::DeserializeOwned>(response: &Value) -> Option<T> {
    serde_json::from_value(response.clone())
        .ok()
        .or_else(|| {
            response
                .get("data")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
        })
        .or_else(|| {
            response
                .get("result")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
        })
}

// ---------- Validation ----------

fn validate_da_data(
    config: &ExternalDaConfig,
    provider: ExternalDaProvider,
    pubdata_hash: B256,
    data: &ExternalDaData,
) -> anyhow::Result<()> {
    if !config.validate_pubdata_integrity {
        return Ok(());
    }
    match (provider, data) {
        (ExternalDaProvider::Avail, ExternalDaData::Avail(avail_data)) => {
            let actual_leaf = avail_data.merkle_proof_input.leaf;
            if actual_leaf != pubdata_hash {
                anyhow::bail!(
                    "external DA payload validation failed: expected leaf {pubdata_hash}, got {actual_leaf}",
                );
            }
        }
        (ExternalDaProvider::LocalFiles, ExternalDaData::LocalFiles(local_files_data)) => {
            if local_files_data.pubdata_hash != pubdata_hash {
                anyhow::bail!(
                    "external DA payload validation failed: expected pubdata_hash {}, got {}",
                    pubdata_hash,
                    local_files_data.pubdata_hash
                );
            }
        }
        _ => unreachable!("provider/data mismatch"),
    }
    Ok(())
}

// ---------- API deserialization types ----------
// These exist because the external DA adapter returns camelCase JSON,
// while our internal types use snake_case serialization (stored on disk).

#[derive(Debug, Deserialize)]
struct AvailApiDaData {
    #[serde(default, alias = "stateDiffHash")]
    state_diff_hash: Option<B256>,
    #[serde(alias = "merkleProofInput")]
    merkle_proof_input: AvailApiMerkleProofInput,
}

#[derive(Debug, Deserialize)]
struct AvailApiMerkleProofInput {
    #[serde(alias = "dataRootProof")]
    data_root_proof: Vec<B256>,
    #[serde(alias = "leafProof")]
    leaf_proof: Vec<B256>,
    #[serde(alias = "rangeHash")]
    range_hash: B256,
    #[serde(alias = "dataRootIndex")]
    data_root_index: U256,
    #[serde(alias = "blobRoot")]
    blob_root: B256,
    #[serde(alias = "bridgeRoot")]
    bridge_root: B256,
    leaf: B256,
    #[serde(alias = "leafIndex")]
    leaf_index: U256,
}

impl From<AvailApiDaData> for AvailDaData {
    fn from(value: AvailApiDaData) -> Self {
        Self {
            state_diff_hash: value.state_diff_hash.unwrap_or(B256::ZERO),
            merkle_proof_input: AvailMerkleProofInput {
                data_root_proof: value.merkle_proof_input.data_root_proof,
                leaf_proof: value.merkle_proof_input.leaf_proof,
                range_hash: value.merkle_proof_input.range_hash,
                data_root_index: value.merkle_proof_input.data_root_index,
                blob_root: value.merkle_proof_input.blob_root,
                bridge_root: value.merkle_proof_input.bridge_root,
                leaf: value.merkle_proof_input.leaf,
                leaf_index: value.merkle_proof_input.leaf_index,
            },
        }
    }
}

#[derive(Debug, Deserialize)]
struct LocalFilesApiDaData {
    #[serde(default, alias = "stateDiffHash")]
    state_diff_hash: Option<B256>,
    #[serde(alias = "pubdataHash")]
    pubdata_hash: B256,
    #[serde(default, alias = "filePath")]
    file_path: Option<String>,
}

impl From<LocalFilesApiDaData> for LocalFilesDaData {
    fn from(value: LocalFilesApiDaData) -> Self {
        Self {
            state_diff_hash: value.state_diff_hash.unwrap_or(B256::ZERO),
            pubdata_hash: value.pubdata_hash,
            file_path: value.file_path,
        }
    }
}

// ---------- Helpers ----------

fn truncate_for_log(body: &str) -> &str {
    const MAX_LOG_BODY_LEN: usize = 512;
    if body.len() <= MAX_LOG_BODY_LEN {
        body
    } else {
        let mut cut = MAX_LOG_BODY_LEN;
        while cut > 0 && !body.is_char_boundary(cut) {
            cut -= 1;
        }
        &body[..cut]
    }
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn truncate_for_log_limits_length() {
        let long = "x".repeat(700);
        let truncated = truncate_for_log(&long);
        assert_eq!(truncated.len(), 512);
    }

    #[test]
    fn parse_avail_payload_from_wrapped_response() {
        let response = json!({
            "data": {
                "stateDiffHash": B256::ZERO,
                "merkleProofInput": {
                    "dataRootProof": [],
                    "leafProof": [],
                    "rangeHash": B256::ZERO,
                    "dataRootIndex": U256::ZERO,
                    "blobRoot": B256::ZERO,
                    "bridgeRoot": B256::ZERO,
                    "leaf": B256::ZERO,
                    "leafIndex": U256::ZERO
                }
            }
        });
        let parsed = parse_da_data_from_response(ExternalDaProvider::Avail, response);
        assert!(parsed.is_some());
    }

    #[test]
    fn parse_avail_payload_from_result_wrapper() {
        let response = json!({
            "result": {
                "stateDiffHash": B256::ZERO,
                "merkleProofInput": {
                    "dataRootProof": [],
                    "leafProof": [],
                    "rangeHash": B256::ZERO,
                    "dataRootIndex": U256::ZERO,
                    "blobRoot": B256::ZERO,
                    "bridgeRoot": B256::ZERO,
                    "leaf": B256::ZERO,
                    "leafIndex": U256::ZERO
                }
            }
        });
        let parsed = parse_da_data_from_response(ExternalDaProvider::Avail, response);
        assert!(parsed.is_some());
    }

    #[test]
    fn parse_avail_payload_returns_none_for_ack_only() {
        let response = json!({ "accepted": true, "requestId": "abc" });
        assert!(parse_da_data_from_response(ExternalDaProvider::Avail, response).is_none());
    }

    #[test]
    fn parse_local_files_payload_from_wrapped_response() {
        let response = json!({
            "data": {
                "stateDiffHash": B256::ZERO,
                "pubdataHash": B256::ZERO,
                "filePath": "blobs/sample.bin"
            }
        });
        let parsed = parse_da_data_from_response(ExternalDaProvider::LocalFiles, response);
        assert!(parsed.is_some());
    }

    #[test]
    fn parse_local_files_payload_returns_none_for_ack_only() {
        let response = json!({ "accepted": true, "requestId": "abc" });
        assert!(parse_da_data_from_response(ExternalDaProvider::LocalFiles, response).is_none());
    }
}
