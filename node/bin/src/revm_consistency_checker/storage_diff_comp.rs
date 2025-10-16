use alloy::primitives::{Address, B256, U256, address};
use reth_revm::DatabaseRef;
use reth_revm::db::CacheDB;
use std::collections::HashMap;
use zksync_os_interface::types::{AccountDiff, StorageWrite};

use crate::revm_consistency_checker::bytecode_hash::{
    EMPTY_BYTE_CODE_HASH, calculate_bytecode_hash,
};

const ACCOUNT_PROPERTIES_STORAGE_ADDRESS: Address =
    address!("0000000000000000000000000000000000008003");

/// Fold a sequence of per-transaction state diffs into a single per-block diff.
/// `tx_diffs` must be ordered from earliest to latest tx in the block.
pub fn accumulate_revm_state_diffs<DB>(
    cache_db: &mut CacheDB<DB>,
    zksync_account_diff: &[AccountDiff],
) -> Vec<AccountDiff>
where
    DB: DatabaseRef,
{
    let mut to_add = vec![];
    for (address, _) in cache_db.cache.accounts.iter() {
        let mut cont = false;
        for x in zksync_account_diff.iter() {
            if x.address == *address {
                cont = true;
                break;
            }
        }
        if !cont {
            to_add.push(address);
        }
    }
    let mut final_zksync_account_diff = Vec::from(zksync_account_diff);

    for addr in to_add {
        if let Ok(account) = cache_db.basic_ref(*addr) {
            let account = account.unwrap_or_default();
            final_zksync_account_diff.push(AccountDiff {
                address: *addr,
                balance: account.balance,
                nonce: account.nonce,
                bytecode_hash: calculate_bytecode_hash(&account.code.unwrap_or_default()),
            });
        }
    }

    final_zksync_account_diff
}

pub fn compare_state_diffs<DB>(
    cache_db: &mut CacheDB<DB>,
    // revm_state_diffs: &HashMap<Address, Account, RandomState>,
    zksync_storage_writes: &Vec<StorageWrite>,
    zksync_account_diffs: &Vec<AccountDiff>,
) where
    DB: DatabaseRef,
{
    // 1) Build REVM map: (account, slot_key) -> value
    let mut revm_map: HashMap<(Address, B256), B256> = HashMap::new();

    for (addr, account) in &cache_db.cache.accounts {
        // TODO: Account Properties stores the hash of nonce, balance and etc
        // It is limitation for now
        if *addr == ACCOUNT_PROPERTIES_STORAGE_ADDRESS {
            continue;
        }
        for (slot_key, slot) in &account.storage {
            if cache_db.db.storage_ref(*addr, *slot_key).unwrap() != *slot {
                let k = B256::from(*slot_key);
                let v = B256::from(*slot);
                revm_map.insert((*addr, k), v);
            }
        }
    }

    // 2) Build ZK map (latest write wins)
    let mut zk_map: HashMap<(Address, B256), B256> = HashMap::new();
    for w in zksync_storage_writes {
        // TODO: Account Properties stores the hash of nonce, balance and etc
        // It is limitation for now
        if w.account == ACCOUNT_PROPERTIES_STORAGE_ADDRESS {
            continue;
        }
        // As per your note: ignore `key`, use `(account, account_key)`.
        zk_map.insert((w.account, w.account_key), w.value);
    }

    // 3) Compare
    let mut missing_in_revm: Vec<((Address, B256), B256)> = Vec::new();
    let mut missing_in_zksync: Vec<(Address, B256)> = Vec::new();
    let mut value_mismatches: Vec<((Address, B256), B256, B256)> = Vec::new();

    for (k, zk_v) in &zk_map {
        match revm_map.get(k) {
            None => missing_in_revm.push((*k, *zk_v)),
            Some(revm_v) if revm_v != zk_v => value_mismatches.push((*k, *revm_v, *zk_v)),
            _ => {}
        }
    }
    for k in revm_map.keys() {
        if !zk_map.contains_key(k) {
            missing_in_zksync.push(*k);
        }
    }

    #[derive(Clone, Copy)]
    struct Snap {
        nonce: u64,
        balance: U256,
        bytecode_hash: B256,
    }

    // REVM changed accounts → post-state snapshot
    let mut revm_accounts: HashMap<Address, Snap> = HashMap::new();
    for (addr, acc) in &cache_db.cache.accounts {
        let code = acc.info.code.as_ref();
        let bytecode_hash = if let Some(bytecode) = code
            && !bytecode.is_empty()
        {
            calculate_bytecode_hash(bytecode)
        } else {
            Default::default()
        };

        revm_accounts.insert(
            *addr,
            Snap {
                nonce: acc.info.nonce,
                balance: acc.info.balance,
                bytecode_hash,
            },
        );
    }

    // ZKsync account diffs (latest wins if duplicates)
    let mut zk_accounts: HashMap<Address, Snap> = HashMap::new();
    for d in zksync_account_diffs {
        zk_accounts.insert(
            d.address,
            Snap {
                nonce: d.nonce,
                balance: d.balance,
                bytecode_hash: d.bytecode_hash,
            },
        );
    }

    // Accounts present in ZK but not in REVM (shouldn't happen if REVM list is "changed accounts")
    let mut acc_missing_in_revm: Vec<Address> = Vec::new();
    // Accounts present in REVM but not in ZK
    let mut acc_missing_in_zksync: Vec<Address> = Vec::new();
    // Field-level mismatches
    struct AccMismatch {
        addr: Address,
        nonce: Option<(u64, u64)>, // (revm, zk)
        balance: Option<(U256, U256)>,
        bytecode_hash: Option<(B256, B256)>,
    }
    let mut acc_value_mismatches: Vec<AccMismatch> = Vec::new();

    for (&addr, r) in &revm_accounts {
        match zk_accounts.get(&addr) {
            None => acc_missing_in_zksync.push(addr),
            Some(z) => {
                let mut mm = AccMismatch {
                    addr,
                    nonce: None,
                    balance: None,
                    bytecode_hash: None,
                };
                if r.nonce != z.nonce {
                    mm.nonce = Some((r.nonce, z.nonce));
                }
                if r.balance != z.balance {
                    mm.balance = Some((r.balance, z.balance));
                }
                if r.bytecode_hash != z.bytecode_hash
                    && !(r.bytecode_hash == EMPTY_BYTE_CODE_HASH && z.bytecode_hash == B256::ZERO)
                    && !(r.bytecode_hash == B256::ZERO && z.bytecode_hash == EMPTY_BYTE_CODE_HASH)
                {
                    mm.bytecode_hash = Some((r.bytecode_hash, z.bytecode_hash));
                }
                if mm.nonce.is_some() || mm.balance.is_some() || mm.bytecode_hash.is_some() {
                    acc_value_mismatches.push(mm);
                }
            }
        }
    }
    for (&addr, _z) in &zk_accounts {
        if !revm_accounts.contains_key(&addr) {
            acc_missing_in_revm.push(addr);
        }
    }

    const MAX_SHOW: usize = 20;
    let storage_ok =
        missing_in_revm.is_empty() && missing_in_zksync.is_empty() && value_mismatches.is_empty();
    let accounts_ok = acc_missing_in_revm.is_empty()
        && acc_missing_in_zksync.is_empty()
        && acc_value_mismatches.is_empty();

    if storage_ok && accounts_ok {
        println!(
            "✅ State diffs match. Compared {} storage keys and {} accounts (incl. synthesized 0x…8003).",
            zk_map.len(),
            zk_accounts.len()
        );
        return;
    }

    println!("❌ State diffs do not match.");

    // Storage section
    println!("=== STORAGE DIFFS ===");
    println!("  missing_in_revm  : {}", missing_in_revm.len());
    for ((addr, key), val) in missing_in_revm.iter().take(MAX_SHOW) {
        println!("    REVM MISSING -> addr: {addr:?}, slot: {key:#x}, expected: {val:#x}");
    }
    if missing_in_revm.len() > MAX_SHOW {
        println!("    ... and {} more", missing_in_revm.len() - MAX_SHOW);
    }

    println!("  missing_in_zksync: {}", missing_in_zksync.len());
    for (addr, key) in missing_in_zksync.iter().take(MAX_SHOW) {
        let v = revm_map.get(&(*addr, *key)).copied().unwrap_or(B256::ZERO);
        println!("    ZK MISSING -> addr: {addr:?}, slot: {key:#x}, revm_value: {v:#x}");
    }
    if missing_in_zksync.len() > MAX_SHOW {
        println!("    ... and {} more", missing_in_zksync.len() - MAX_SHOW);
    }

    println!("  value_mismatches : {}", value_mismatches.len());
    for ((addr, key), revm_v, zk_v) in value_mismatches.iter().take(MAX_SHOW) {
        println!(
            "    VALUE MISMATCH -> addr: {addr:?}, slot: {key:#x}, revm: {revm_v:#x}, zk: {zk_v:#x}"
        );
    }
    if value_mismatches.len() > MAX_SHOW {
        println!("    ... and {} more", value_mismatches.len() - MAX_SHOW);
    }

    // Accounts section
    println!("=== ACCOUNT DIFFS ===");
    println!("  acc_missing_in_revm   : {}", acc_missing_in_revm.len());
    for addr in acc_missing_in_revm.iter().take(MAX_SHOW) {
        println!("    REVM MISSING ACCOUNT -> addr: {addr:?}");
    }
    if acc_missing_in_revm.len() > MAX_SHOW {
        println!("    ... and {} more", acc_missing_in_revm.len() - MAX_SHOW);
    }

    println!("  acc_missing_in_zksync : {}", acc_missing_in_zksync.len());
    for addr in acc_missing_in_zksync.iter().take(MAX_SHOW) {
        println!("    ZK MISSING ACCOUNT -> addr: {addr:?}");
    }
    if acc_missing_in_zksync.len() > MAX_SHOW {
        println!(
            "    ... and {} more",
            acc_missing_in_zksync.len() - MAX_SHOW
        );
    }

    println!("  acc_value_mismatches  : {}", acc_value_mismatches.len());
    for m in acc_value_mismatches.iter().take(MAX_SHOW) {
        if let Some((r, z)) = m.nonce {
            println!(
                "    NONCE MISMATCH -> addr: {:?}, revm: {r}, zk: {z}",
                m.addr
            );
        }
        if let Some((r, z)) = m.balance {
            println!(
                "    BALANCE MISMATCH -> addr: {:?}, revm: {r}, zk: {z}",
                m.addr
            );
        }
        if let Some((r, z)) = m.bytecode_hash {
            println!(
                "    BYTECODE HASH MISMATCH -> addr: {:?}, revm: {r:#x}, zk: {z:#x}",
                m.addr
            );
        }
    }
    if acc_value_mismatches.len() > MAX_SHOW {
        println!("    ... and {} more", acc_value_mismatches.len() - MAX_SHOW);
    }
}
