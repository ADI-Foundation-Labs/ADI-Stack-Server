use alloy::{
    hex::ToHexExt as _,
    primitives::{Address, B256},
};

use anyhow::{Result, anyhow};

pub(super) fn ensure_len(bytes: &[u8], expected: usize, what: &str) -> Result<()> {
    if bytes.len() != expected {
        Err(anyhow!(
            "Invalid {what} length: expected {expected}, got {}",
            bytes.len()
        ))
    } else {
        Ok(())
    }
}

pub(super) fn decode_u64(value: &[u8]) -> Result<u64> {
    ensure_len(value, 8, "u64")?;
    let mut arr = [0u8; 8];
    arr.copy_from_slice(value);
    Ok(u64::from_be_bytes(arr))
}

pub(super) fn decode_u64_pair(bytes: &[u8]) -> Result<(u64, u64)> {
    ensure_len(bytes, 16, "u64 pair")?;
    let (left, right) = bytes.split_at(8);
    Ok((decode_u64(left)?, decode_u64(right)?))
}

pub(super) fn decode_b256(bytes: &[u8], what: &str) -> Result<B256> {
    ensure_len(bytes, 32, what)?;
    Ok(B256::from_slice(bytes))
}

pub(super) fn format_b256(value: B256, truncate_to: usize) -> String {
    let encoded = value.encode_hex();
    if truncate_to > 0 && encoded.len() > truncate_to * 2 {
        format!("0x{}…", &encoded[..truncate_to * 2])
    } else {
        format!("0x{encoded}")
    }
}

pub(super) fn format_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

pub(super) fn truncate_hex(bytes: &[u8], limit: usize) -> String {
    if bytes.len() <= limit {
        format_hex(bytes)
    } else {
        let truncated = &bytes[..limit];
        let mut hex_repr = format_hex(truncated);
        hex_repr.push('…');
        format!("{hex_repr} (total {} bytes)", bytes.len())
    }
}

pub(super) fn ascii_preview(bytes: &[u8], limit: usize) -> String {
    let preview: String = bytes
        .iter()
        .take(limit)
        .map(|b| {
            let ch = *b as char;
            if ch.is_ascii_graphic() || ch == ' ' {
                ch
            } else {
                '.'
            }
        })
        .collect();
    if bytes.len() > limit {
        format!("{preview}…")
    } else {
        preview
    }
}

pub(super) fn short_hex(bytes: &[u8], max: usize) -> Result<String> {
    if bytes.is_empty() {
        return Ok("0x".into());
    }
    let hex = hex::encode(bytes);
    if max == 0 || hex.len() <= max * 2 {
        Ok(format!("0x{hex}"))
    } else {
        Ok(format!("0x{}…", &hex[..max * 2]))
    }
}

pub(super) fn format_address(address: Address) -> String {
    format!("0x{}", hex::encode(address.as_slice()))
}

pub(super) fn format_optional_address(address: Option<Address>) -> String {
    address.map(format_address).unwrap_or_else(|| "none".into())
}
