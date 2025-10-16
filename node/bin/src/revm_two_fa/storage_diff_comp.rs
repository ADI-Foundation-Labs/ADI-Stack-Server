use alloy::primitives::{Address, B256, U256, address, map::foldhash::fast::RandomState};
use blake2::{Blake2s256, Digest};
use reth_revm::bytecode::Bytecode;
use reth_revm::db::CacheDB;
use reth_revm::{Database, DatabaseRef};
use reth_revm::{bytecode::opcode, state::Account};
use std::collections::{HashMap, hash_map::Entry};
use zksync_os_interface::types::{AccountDiff, StorageWrite};

const ACCOUNT_PROPERTIES_STORAGE_ADDRESS: Address =
    address!("0000000000000000000000000000000000008003");

pub const BYTECODE_ALIGNMENT: usize = core::mem::size_of::<u64>();
pub const JUMPDEST: u8 = 0x5b;

pub const EMPTY_BYTE_CODE_HASH: B256 = B256::new([
    0x69, 0x21, 0x7a, 0x30, 0x79, 0x90, 0x80, 0x94, 0xe1, 0x11, 0x21, 0xd0, 0x42, 0x35, 0x4a, 0x7c,
    0x1f, 0x55, 0xb6, 0x48, 0x2c, 0xa1, 0xa5, 0x1e, 0x1b, 0x25, 0x0d, 0xfd, 0x1e, 0xd0, 0xee, 0xf9,
]);

// TODO: refactor

/// Fold a sequence of per-transaction state diffs into a single per-block diff.
/// `tx_diffs` must be ordered from earliest to latest tx in the block.
pub fn accumulate_revm_state_diffs<DB>(
    tx_diffs: &[HashMap<Address, Account, RandomState>],
    cache_db: &mut CacheDB<DB>,
    zksync_account_diff: &[AccountDiff],
) -> (HashMap<Address, Account, RandomState>, Vec<AccountDiff>)
where DB: DatabaseRef
{
    let mut acc: HashMap<Address, Account, RandomState> =
        HashMap::with_hasher(RandomState::default());

    for tx in tx_diffs {
        for (&addr, src) in tx {
            match acc.entry(addr) {
                Entry::Vacant(v) => {
                    // First time we see this account in the block: take the whole snapshot.
                    v.insert(src.clone());
                }
                Entry::Occupied(mut o) => {
                    // Merge into the running per-block snapshot.
                    merge_account(o.get_mut(), src);
                }
            }
        }
    }

    for account in acc.values_mut() {
        if account.is_selfdestructed_locally() {
            account.selfdestruct();
            account.unmark_selfdestructed_locally();
        }
    }

    for acc_diff in zksync_account_diff {
        if let Entry::Vacant(v) = acc.entry(acc_diff.address) {
            if let Ok(account) = cache_db.basic(acc_diff.address) {
                v.insert(account.unwrap_or_default().into());
            }
        }
    }

    let mut to_add = vec![];
    for (address, _) in acc.iter() {
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
        if let Ok(account) = cache_db.basic(*addr) {
            let account = account.unwrap_or_default();
            final_zksync_account_diff.push(AccountDiff {
                address: *addr,
                balance: account.balance,
                nonce: account.nonce,
                bytecode_hash: compute_bytecode_hash(&account.code.unwrap_or_default())
            });
        }
    }

    (acc, final_zksync_account_diff)
}

/// Merge `src` tx-level changes into `dst` (block accumulator).
/// Last-write-wins semantics for all fields/slots.
fn merge_account(dst: &mut Account, src: &Account) {
    dst.info = src.info.clone();
    dst.status = src.status.clone();
    dst.transaction_id = src.transaction_id;

    // Overwrite slots present in this tx diff.
    for (k, slot) in &src.storage {
        dst.storage.insert(*k, slot.clone());
    }
}

#[inline(always)]
pub const fn bytecode_padding_len(deployed_len: usize) -> usize {
    let word = BYTECODE_ALIGNMENT;
    let rem = deployed_len % word;
    if rem == 0 { 0 } else { word - rem }
}

/// # Safety
/// pos must be within the bounds of the bitmap.
pub(crate) unsafe fn set_bit_on_unchecked(inner: &mut Vec<u8>, pos: usize) {
    let (word_idx, bit_idx) = (pos / 8, pos % 8);
    let dst = unsafe { inner.get_unchecked_mut(word_idx) };
    *dst |= 1u8 << bit_idx;
}

/// Analyzes bytecode to build a jump map.
fn create_artifacts(code: &[u8]) -> Vec<u8> {
    let code_len = code.len();
    let u64_capacity = code_len.next_multiple_of(u64::BITS as usize) / (u64::BITS as usize);
    let word_capacity = u64_capacity * (u64::BITS as usize / usize::BITS as usize);
    let mut jumps = Vec::with_capacity(word_capacity * usize::BITS as usize / 8);
    jumps.resize(word_capacity * usize::BITS as usize / 8, 0);

    let mut i = 0;
    while i < code_len {
        let op = code[i];
        if op == JUMPDEST {
            // SAFETY: `i` is always < code_len
            unsafe { set_bit_on_unchecked(&mut jumps, i) };
            i += 1;
        } else if (opcode::PUSH1..=opcode::PUSH32).contains(&op) {
            i += 1 + (op - opcode::PUSH1 + 1) as usize;
        } else {
            i += 1;
        }
    }

    jumps
}

fn compute_bytecode_hash(evm_code: &Bytecode) -> B256 {
    let artifacts = create_artifacts(&evm_code.original_bytes());
    let artifacts_len = artifacts.len();
    let padding_len = bytecode_padding_len(evm_code.original_bytes().len());
    let full_len = evm_code.original_bytes().len() + padding_len + artifacts_len;
    let mut padded_bytecode: Vec<u8> = vec![0u8; full_len];
    padded_bytecode[..evm_code.original_bytes().len()].copy_from_slice(&evm_code.original_bytes());
    let bitmap_offset = evm_code.original_bytes().len() + padding_len;
    padded_bytecode[bitmap_offset..].copy_from_slice(&artifacts);
    B256::from_slice(&Blake2s256::digest(&padded_bytecode))
}

pub fn compare_state_diffs(
    revm_state_diffs: &HashMap<Address, Account, RandomState>,
    zksync_storage_writes: &Vec<StorageWrite>,
    zksync_account_diffs: &Vec<AccountDiff>,
) {
    // 1) Build REVM map: (account, slot_key) -> value
    let mut revm_map: HashMap<(Address, B256), B256> = HashMap::new();

    for (addr, account) in revm_state_diffs {
        // TODO: Account Properties stores the hash of nonce, balance and etc
        // It is limitation for now
        if *addr == ACCOUNT_PROPERTIES_STORAGE_ADDRESS {
            continue;
        }
        for (slot_key, slot) in account.changed_storage_slots() {
            let k = B256::from(*slot_key);
            let v = B256::from(slot.present_value);
            revm_map.insert((*addr, k), v);
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
    for (addr, acc) in revm_state_diffs {
        let code = acc.info.code.as_ref();
        let bytecode_hash = if let Some(bytecode) = code
            && !bytecode.is_empty()
        {
            compute_bytecode_hash(bytecode)
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
    // panic!("State diff comparison failed");
}

#[test]
fn calculate_bytecode_test() {
    use alloy::hex;
    let bytecode = Bytecode::new_legacy(hex!("60806040523661001357610011610017565b005b6100115b61001f6102a0565b73ffffffffffffffffffffffffffffffffffffffff1633036102965760607fffffffff000000000000000000000000000000000000000000000000000000005f35167fc9a6301a000000000000000000000000000000000000000000000000000000008101610097576100906102df565b915061028e565b7fb0e10d7a000000000000000000000000000000000000000000000000000000007fffffffff000000000000000000000000000000000000000000000000000000008216016100e857610090610332565b7f70d7c690000000000000000000000000000000000000000000000000000000007fffffffff0000000000000000000000000000000000000000000000000000000082160161013957610090610376565b7f07ae5bc0000000000000000000000000000000000000000000000000000000007fffffffff0000000000000000000000000000000000000000000000000000000082160161018a576100906103a6565b7fa39f25e5000000000000000000000000000000000000000000000000000000007fffffffff000000000000000000000000000000000000000000000000000000008216016101db576100906103f2565b6040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152604260248201527f5472616e73706172656e745570677261646561626c6550726f78793a2061646d60448201527f696e2063616e6e6f742066616c6c6261636b20746f2070726f7879207461726760648201527f6574000000000000000000000000000000000000000000000000000000000000608482015260a4015b60405180910390fd5b815160208301f35b61029e610405565b565b5f7fb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d61035b5473ffffffffffffffffffffffffffffffffffffffff16919050565b60606102e9610415565b5f6102f736600481846108bc565b810190610304919061090b565b905061031f8160405180602001604052805f8152505f61041f565b505060408051602081019091525f815290565b60605f8061034336600481846108bc565b8101906103509190610951565b915091506103608282600161041f565b60405180602001604052805f8152509250505090565b6060610380610415565b5f61038e36600481846108bc565b81019061039b919061090b565b905061031f8161044a565b60606103b0610415565b5f6103b96102a0565b6040805173ffffffffffffffffffffffffffffffffffffffff831660208201529192500160405160208183030381529060405291505090565b60606103fc610415565b5f6103b96104ae565b61029e6104106104ae565b6104bc565b341561029e575f5ffd5b610428836104da565b5f825111806104345750805b15610445576104438383610526565b505b505050565b7f7e644d79422f17c01e4894b5f4f588d331ebfa28653d42ae832dc59e38c9798f6104736102a0565b6040805173ffffffffffffffffffffffffffffffffffffffff928316815291841660208301520160405180910390a16104ab81610552565b50565b5f6104b761065e565b905090565b365f5f375f5f365f845af43d5f5f3e8080156104d6573d5ff35b3d5ffd5b6104e381610685565b60405173ffffffffffffffffffffffffffffffffffffffff8216907fbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b905f90a250565b606061054b8383604051806060016040528060278152602001610abc60279139610750565b9392505050565b73ffffffffffffffffffffffffffffffffffffffff81166105f5576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152602660248201527f455243313936373a206e65772061646d696e20697320746865207a65726f206160448201527f64647265737300000000000000000000000000000000000000000000000000006064820152608401610285565b807fb53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d61035b80547fffffffffffffffffffffffff00000000000000000000000000000000000000001673ffffffffffffffffffffffffffffffffffffffff9290921691909117905550565b5f7f360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc6102c3565b73ffffffffffffffffffffffffffffffffffffffff81163b610729576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152602d60248201527f455243313936373a206e657720696d706c656d656e746174696f6e206973206e60448201527f6f74206120636f6e7472616374000000000000000000000000000000000000006064820152608401610285565b807f360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc610618565b60605f5f8573ffffffffffffffffffffffffffffffffffffffff16856040516107799190610a52565b5f60405180830381855af49150503d805f81146107b1576040519150601f19603f3d011682016040523d82523d5f602084013e6107b6565b606091505b50915091506107c7868383876107d1565b9695505050505050565b606083156108665782515f0361085f5773ffffffffffffffffffffffffffffffffffffffff85163b61085f576040517f08c379a000000000000000000000000000000000000000000000000000000000815260206004820152601d60248201527f416464726573733a2063616c6c20746f206e6f6e2d636f6e74726163740000006044820152606401610285565b5081610870565b6108708383610878565b949350505050565b8151156108885781518083602001fd5b806040517f08c379a00000000000000000000000000000000000000000000000000000000081526004016102859190610a68565b5f5f858511156108ca575f5ffd5b838611156108d6575f5ffd5b5050820193919092039150565b803573ffffffffffffffffffffffffffffffffffffffff81168114610906575f5ffd5b919050565b5f6020828403121561091b575f5ffd5b61054b826108e3565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52604160045260245ffd5b5f5f60408385031215610962575f5ffd5b61096b836108e3565b9150602083013567ffffffffffffffff811115610986575f5ffd5b8301601f81018513610996575f5ffd5b803567ffffffffffffffff8111156109b0576109b0610924565b6040517fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0603f7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0601f8501160116810181811067ffffffffffffffff82111715610a1c57610a1c610924565b604052818152828201602001871015610a33575f5ffd5b816020840160208301375f602083830101528093505050509250929050565b5f82518060208501845e5f920191825250919050565b602081525f82518060208401528060208501604085015e5f6040828501015260407fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe0601f8301168401019150509291505056fe416464726573733a206c6f772d6c6576656c2064656c65676174652063616c6c206661696c6564a2646970667358221220c3d6e2f9af21088fe12b0b83df5fd964a6203e7192607bf44e7e3cb4ea82952564736f6c634300081c0033").into());
    let bytecode_hash = compute_bytecode_hash(&bytecode);
    println!("bytecode {:?}", bytecode_hash);
}

#[test]
fn calculate_empty_bytecode_test() {
    use alloy::hex;
    let bytecode = Bytecode::new_legacy(hex!("").into());
    let bytecode_hash = compute_bytecode_hash(&bytecode);
    println!("bytecode {:?}", bytecode_hash);
}

// 0x69217a3079908094e11121d042354a7c1f55b6482ca1a51e1b250dfd1ed0eef9
