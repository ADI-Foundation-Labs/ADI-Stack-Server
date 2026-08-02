use cargo_metadata::{MetadataCommand, PackageId};
use reqwest::StatusCode;
use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use std::path::{Path, PathBuf};
use url::Url;

const APP_VARIANTS: [&str; 3] = [
    "multiblock_batch",
    "singleblock_batch",
    "singleblock_batch_logging_enabled",
];

/// Explicit override for branch / local development workflows. When set, it provides the V6
/// proving binaries directly (used for Alt DA, where V6 binaries must be rebuilt from a custom
/// zksync-os branch because the in-VM DA commitment generator changed).
const V6_SOURCE_PATH_ENV: &str = "ZKSYNC_OS_V6_SOURCE_PATH";

const DOWNLOAD_MAX_ATTEMPTS: usize = 5;
const DOWNLOAD_TIMEOUT_SECS: u64 = 60;
const DOWNLOAD_BASE_BACKOFF_MS: u64 = 500;

#[derive(Debug, Clone)]
enum GitRef {
    Tag(String),
    Branch(String),
    Rev(String),
}

fn parse_git_ref(package_id: &PackageId) -> anyhow::Result<GitRef> {
    let url = Url::parse(&package_id.to_string())?;
    if let Some((_, tag)) = url.query_pairs().find(|(key, _)| key == "tag") {
        return Ok(GitRef::Tag(tag.to_string()));
    }
    if let Some((_, branch)) = url.query_pairs().find(|(key, _)| key == "branch") {
        return Ok(GitRef::Branch(branch.to_string()));
    }
    if let Some((_, rev)) = url.query_pairs().find(|(key, _)| key == "rev") {
        return Ok(GitRef::Rev(rev.to_string()));
    }
    anyhow::bail!("missing tag/branch/rev in git url `{url}`");
}

fn proving_version_from_tag(tag: &str) -> Option<&'static str> {
    match tag {
        "v0.2.9-interface-v0.0.14-b" => Some("V6"),
        "dev-20260402-b" => Some("V7"),
        _ => None,
    }
}

fn maybe_remap_v6_release_tag(tag: &str) -> &str {
    // TEMPORARY HACK for V6!!!
    // We've updated interface and rust toolchain for corresponding zksync-os version and it caused a change in binaries.
    // We need to use original V6 binaries from zksync-os v0.2.5.
    // Should be removed as soon as we can get rid of proving V6.
    if proving_version_from_tag(tag) == Some("V6") {
        "v0.2.5-b"
    } else {
        tag
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
}

fn new_http_client() -> anyhow::Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static("zksync-os-build-script/1.0"),
    );

    if let Ok(token) = std::env::var("GITHUB_TOKEN") {
        let bearer = format!("Bearer {}", token.trim());
        match HeaderValue::from_str(&bearer) {
            Ok(value) => {
                headers.insert(AUTHORIZATION, value);
            }
            Err(err) => {
                println!("cargo:warning=Ignoring invalid GITHUB_TOKEN format: {err}");
            }
        }
    }

    Ok(Client::builder()
        .default_headers(headers)
        .timeout(std::time::Duration::from_secs(DOWNLOAD_TIMEOUT_SECS))
        .build()?)
}

fn download_with_retry(client: &Client, url: &str, path: &Path) -> anyhow::Result<()> {
    for attempt in 1..=DOWNLOAD_MAX_ATTEMPTS {
        let response = client.get(url).send();
        match response {
            Ok(response) => {
                let status = response.status();
                if status.is_success() {
                    let body = response.bytes()?;
                    std::fs::write(path, body.as_ref())?;
                    return Ok(());
                }

                if is_retryable_status(status) && attempt < DOWNLOAD_MAX_ATTEMPTS {
                    let delay_ms = DOWNLOAD_BASE_BACKOFF_MS * attempt as u64;
                    println!(
                        "cargo:warning=download attempt {attempt}/{DOWNLOAD_MAX_ATTEMPTS} failed with status {status} for {url}; retrying in {delay_ms}ms"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                    continue;
                }

                anyhow::bail!("download failed with status {status} for {url}");
            }
            Err(err) => {
                if attempt < DOWNLOAD_MAX_ATTEMPTS {
                    let delay_ms = DOWNLOAD_BASE_BACKOFF_MS * attempt as u64;
                    println!(
                        "cargo:warning=download attempt {attempt}/{DOWNLOAD_MAX_ATTEMPTS} failed for {url}: {err}; retrying in {delay_ms}ms"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                    continue;
                }

                anyhow::bail!("download request failed for {url}: {err}");
            }
        }
    }
    unreachable!("loop always returns on success or final attempt");
}

fn ensure_local_binaries_exist(path: &Path) -> anyhow::Result<()> {
    for variant in APP_VARIANTS {
        let file = path.join(format!("{variant}.bin"));
        if !file.exists() {
            anyhow::bail!("missing file `{}`", file.display());
        }
    }
    Ok(())
}

fn sanitize_ref_for_path(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => c,
            _ => '_',
        })
        .collect()
}

fn set_source_path(proving_version: &str, path: &Path) {
    println!(
        "cargo:rustc-env=ZKSYNC_OS_{proving_version}_SOURCE_PATH={}",
        path.to_string_lossy()
    );
}

/// Tag-based (release) workflow: download the proving binaries from GitHub releases.
fn configure_from_release_tag(
    client: &Client,
    manifest_dir: &Path,
    proving_version: &str,
    tag: &str,
) -> anyhow::Result<()> {
    let release_tag = maybe_remap_v6_release_tag(tag);
    let dir = manifest_dir.join("apps").join(release_tag);
    std::fs::create_dir_all(&dir)?;
    for variant in APP_VARIANTS {
        let path = dir.join(format!("{variant}.bin"));
        if path.exists() {
            continue;
        }
        let url = format!(
            "https://github.com/ADI-Foundation-Labs/ADI-Stack-zkOS/releases/download/{release_tag}/{variant}.bin"
        );
        download_with_retry(client, &url, &path)?;
    }
    set_source_path(proving_version, &dir);
    Ok(())
}

/// Branch / revision (local development) workflow: there are no release binaries for a branch,
/// so the binaries must be provided locally. Used for Alt DA, where the V6 binaries are rebuilt
/// from a custom zksync-os branch. Resolution order:
///   1. `ZKSYNC_OS_V6_SOURCE_PATH` env override;
///   2. `apps/<ref>` or `apps/<sanitized-ref>` in this crate.
fn configure_v6_from_local_ref(
    manifest_dir: &Path,
    git_ref: &str,
    ref_kind: &str,
) -> anyhow::Result<()> {
    if let Ok(path) = std::env::var(V6_SOURCE_PATH_ENV) {
        let path = PathBuf::from(path);
        ensure_local_binaries_exist(&path).map_err(|err| {
            anyhow::anyhow!(
                "{V6_SOURCE_PATH_ENV} is set but invalid at `{}`: {err}",
                path.display()
            )
        })?;
        println!(
            "cargo:warning=Using explicit {V6_SOURCE_PATH_ENV} override at `{}`",
            path.display()
        );
        set_source_path("V6", &path);
        return Ok(());
    }

    let candidates = [
        manifest_dir.join("apps").join(git_ref),
        manifest_dir
            .join("apps")
            .join(sanitize_ref_for_path(git_ref)),
    ];
    for candidate in candidates {
        if ensure_local_binaries_exist(&candidate).is_ok() {
            println!(
                "cargo:warning=Using local zksync-os V6 binaries for {ref_kind} `{git_ref}` from `{}`",
                candidate.display()
            );
            set_source_path("V6", &candidate);
            return Ok(());
        }
    }

    let preferred = manifest_dir
        .join("apps")
        .join(sanitize_ref_for_path(git_ref));
    anyhow::bail!(
        "no local zksync-os app binaries found for {ref_kind} `{git_ref}`. \
Build them from the zksync-os branch and place `{}` files in `{}`, or set {V6_SOURCE_PATH_ENV} to a directory containing them",
        APP_VARIANTS
            .iter()
            .map(|v| format!("{v}.bin"))
            .collect::<Vec<_>>()
            .join("`, `"),
        preferred.display()
    )
}

fn main() {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let metadata = MetadataCommand::new().exec().unwrap();
    let client = new_http_client().expect("failed to create HTTP client");

    // Expose the directory containing `app*.bin` files for every proving version present in the
    // dependency graph (V6 and V7). A `forward_system` pinned to a tag downloads release binaries;
    // one pinned to a branch/rev (Alt DA V6) uses locally-built binaries.
    for package in &metadata.packages {
        if package.name.as_str() != "forward_system" {
            continue;
        }
        let git_ref = match parse_git_ref(&package.id) {
            Ok(git_ref) => git_ref,
            Err(err) => {
                println!("cargo::error=failed to parse forward_system's git reference: {err}");
                return;
            }
        };

        let result = match git_ref {
            GitRef::Tag(tag) => {
                let Some(proving_version) = proving_version_from_tag(&tag) else {
                    // Historical / unused forward_system versions don't ship proving binaries.
                    continue;
                };
                configure_from_release_tag(&client, &manifest_dir, proving_version, &tag)
            }
            // A branch/rev `forward_system` is the Alt DA V6 dependency (only V6 is pinned to a
            // branch; V7 stays tag-based), so it maps to the V6 proving binaries.
            GitRef::Branch(branch) => configure_v6_from_local_ref(&manifest_dir, &branch, "branch"),
            GitRef::Rev(rev) => configure_v6_from_local_ref(&manifest_dir, &rev, "revision"),
        };
        if let Err(err) = result {
            println!("cargo::error={err}");
            return;
        }
    }
}
