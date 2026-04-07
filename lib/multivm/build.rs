use cargo_metadata::{Metadata, MetadataCommand, PackageId};
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
    let mut query_pairs = url.query_pairs();
    if let Some((_, tag)) = query_pairs.find(|(key, _)| key == "tag") {
        return Ok(GitRef::Tag(tag.to_string()));
    }

    let mut query_pairs = url.query_pairs();
    if let Some((_, branch)) = query_pairs.find(|(key, _)| key == "branch") {
        return Ok(GitRef::Branch(branch.to_string()));
    }

    let mut query_pairs = url.query_pairs();
    if let Some((_, rev)) = query_pairs.find(|(key, _)| key == "rev") {
        return Ok(GitRef::Rev(rev.to_string()));
    }

    anyhow::bail!("missing tag/branch/rev in git url `{url}`");
}

fn proving_version_from_tag(tag: &str) -> Option<&'static str> {
    match tag {
        "v0.2.8-interface-v0.0.14-b"
        | "v0.2.8-simulation-only-interface-v0.0.14-b"
        | "dev-20260311" => Some("V6"),
        _ => None,
    }
}

fn maybe_remap_v6_release_tag(tag: &str) -> &str {
    // TEMPORARY HACK for V6!!!
    // We've updated interface and rust toolchain for corresponding zksync-os version and it caused a change in binaries.
    // We need to use original V6 binaries from zksync-os v0.2.5.
    // Should be removed as soon as we can get rid of proving V6.
    if tag == "v0.2.8-interface-v0.0.14-b" {
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

fn set_v6_source_path(path: &Path) {
    println!(
        "cargo:rustc-env={V6_SOURCE_PATH_ENV}={}",
        path.to_string_lossy()
    );
}

fn resolve_current_forward_system_id(
    metadata: &Metadata,
    manifest_dir: &Path,
) -> anyhow::Result<PackageId> {
    let manifest_path = manifest_dir.join("Cargo.toml");
    let this_package = metadata
        .packages
        .iter()
        .find(|pkg| {
            pkg.name.as_str() == "zksync_os_multivm"
                && pkg.manifest_path.as_std_path() == manifest_path
        })
        .ok_or_else(|| anyhow::anyhow!("failed to locate zksync_os_multivm package in metadata"))?;

    let resolve = metadata
        .resolve
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("cargo metadata missing resolve graph"))?;
    let node = resolve
        .nodes
        .iter()
        .find(|node| node.id == this_package.id)
        .ok_or_else(|| {
            anyhow::anyhow!("failed to locate zksync_os_multivm node in resolve graph")
        })?;
    let dep = node
        .deps
        .iter()
        .find(|dep| dep.name.to_string() == "zk_os_forward_system")
        .ok_or_else(|| {
            anyhow::anyhow!(
                "failed to locate `zk_os_forward_system` dependency for zksync_os_multivm"
            )
        })?;

    Ok(dep.pkg.clone())
}

fn configure_from_release_tag(manifest_dir: &Path, tag: &str) -> anyhow::Result<()> {
    let proving_version = proving_version_from_tag(tag)
        .ok_or_else(|| anyhow::anyhow!("unsupported zksync-os tag `{tag}` for proving binaries"))?;
    let release_tag = maybe_remap_v6_release_tag(tag);

    let dir = manifest_dir.join("apps").join(release_tag);
    std::fs::create_dir_all(&dir)?;
    if ensure_local_binaries_exist(&dir).is_err() {
        let client = new_http_client()?;
        for variant in APP_VARIANTS {
            let url = format!(
                "https://github.com/ADI-Foundation-Labs/ADI-Stack-zkOS/releases/download/{release_tag}/{variant}.bin"
            );
            let path = dir.join(format!("{variant}.bin"));
            if path.exists() {
                continue;
            }
            download_with_retry(&client, &url, &path)?;
        }
    }

    println!(
        "cargo:warning=Using zksync-os release binaries from `{}` for proving {proving_version}",
        dir.display()
    );
    set_v6_source_path(&dir);
    Ok(())
}

fn configure_from_local_ref(
    manifest_dir: &Path,
    git_ref: &str,
    ref_kind: &str,
) -> anyhow::Result<()> {
    let candidates = [
        manifest_dir.join("apps").join(git_ref),
        manifest_dir
            .join("apps")
            .join(sanitize_ref_for_path(git_ref)),
    ];
    for candidate in candidates {
        if ensure_local_binaries_exist(&candidate).is_ok() {
            println!(
                "cargo:warning=Using local zksync-os binaries for {ref_kind} `{git_ref}` from `{}`",
                candidate.display()
            );
            set_v6_source_path(&candidate);
            return Ok(());
        }
    }

    let preferred = manifest_dir
        .join("apps")
        .join(sanitize_ref_for_path(git_ref));
    anyhow::bail!(
        "no local zksync-os app binaries found for {ref_kind} `{git_ref}`. \
Place `{}` files in `{}` or set {V6_SOURCE_PATH_ENV} to a directory containing them",
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

    // Explicit override for branch / local development workflows.
    if let Ok(path) = std::env::var(V6_SOURCE_PATH_ENV) {
        let path = PathBuf::from(path);
        if let Err(err) = ensure_local_binaries_exist(&path) {
            println!(
                "cargo::error={V6_SOURCE_PATH_ENV} is set but invalid at `{}`: {err}",
                path.display()
            );
            return;
        }
        println!(
            "cargo:warning=Using explicit {V6_SOURCE_PATH_ENV} override at `{}`",
            path.display()
        );
        set_v6_source_path(&path);
        return;
    }

    let metadata = MetadataCommand::new().exec().unwrap();
    let forward_system_id = match resolve_current_forward_system_id(&metadata, &manifest_dir) {
        Ok(id) => id,
        Err(err) => {
            println!("cargo::error=failed to resolve current zksync-os dependency: {err}");
            return;
        }
    };

    let git_ref = match parse_git_ref(&forward_system_id) {
        Ok(git_ref) => git_ref,
        Err(err) => {
            println!(
                "cargo::error=failed to parse current forward_system git reference `{forward_system_id}`: {err}"
            );
            return;
        }
    };

    let result = match git_ref {
        GitRef::Tag(tag) => configure_from_release_tag(&manifest_dir, &tag),
        GitRef::Branch(branch) => configure_from_local_ref(&manifest_dir, &branch, "branch"),
        GitRef::Rev(rev) => configure_from_local_ref(&manifest_dir, &rev, "revision"),
    };
    if let Err(err) = result {
        println!("cargo::error={err}");
    }
}
