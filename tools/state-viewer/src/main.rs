use std::{
    cmp::{max, min},
    fmt::Write as _,
    io::stdout,
    path::{Path, PathBuf},
    time::Duration,
};

use alloy::{
    consensus::{Block, Sealed, Transaction},
    eips::Decodable2718,
    primitives::{Address, TxHash, B256, U256},
    rlp::Decodable,
};
use anyhow::{anyhow, Context, Result};
use bincode::config::standard;
use clap::{Parser, ValueEnum};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand as _,
};
use hex::ToHex;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block as tuiBlock, Borders, List, ListItem, ListState, Paragraph, Wrap},
    Frame, Terminal,
};
use zksync_os_interface::types::BlockContext;
use zksync_os_rocksdb::rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, Options, SingleThreaded,
};
use zksync_os_storage_api::{RepositoryBlock, TxMeta};
use zksync_os_types::{ZkEnvelope, ZkReceiptEnvelope, ZkTransaction, ZkTxType};

type RocksDb = DBWithThreadMode<SingleThreaded>;

const DEFAULT_LIMIT: usize = 256;
const DB_ORDER: [DbKind; 5] = [
    DbKind::BlockReplayWal,
    DbKind::Preimages,
    DbKind::Repository,
    DbKind::State,
    DbKind::Tree,
];

#[derive(Parser)]
#[command(author, version, about = "Inspect ZKsync OS RocksDB databases", long_about = None)]
struct Args {
    /// Base directory containing RocksDB databases (e.g. ./db/node1)
    #[arg(long, default_value = "./db/node1")]
    data_dir: PathBuf,
    /// Which database to inspect
    #[arg(long, value_enum, default_value = "repository")]
    db: DbKind,
    /// Maximum number of entries to load per column family
    #[arg(long, default_value_t = DEFAULT_LIMIT)]
    limit: usize,
}

#[derive(Clone, Copy, ValueEnum, Debug, PartialEq, Eq)]
enum DbKind {
    #[value(alias = "block-replay")]
    BlockReplayWal,
    Preimages,
    Repository,
    State,
    Tree,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut app = App::new(args.data_dir, args.limit, args.db)?;

    enable_raw_mode().context("enabling raw mode")?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen).context("switching to alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, &mut app);

    disable_raw_mode().context("disabling raw mode")?;
    terminal
        .backend_mut()
        .execute(LeaveAlternateScreen)
        .context("leaving alternate screen")?;
    terminal.show_cursor()?;
    result
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut App,
) -> Result<()> {
    loop {
        terminal.draw(|frame| app.draw(frame))?;

        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if app.handle_key(key)? {
                    break;
                }
            }
        }
    }
    Ok(())
}

struct App {
    base_path: PathBuf,
    limit: usize,
    kind_index: usize,
    schema: Box<dyn Schema>,
    db: RocksDb,
    cf_names: Vec<String>,
    selected_cf: usize,
    entries: Vec<Entry>,
    selected_entry: usize,
    db_path: PathBuf,
    status: Option<String>,
}

impl App {
    fn new(base_path: PathBuf, limit: usize, initial_kind: DbKind) -> Result<Self> {
        if !base_path.exists() {
            return Err(anyhow!(
                "Base directory `{}` does not exist",
                base_path.display()
            ));
        }

        let kind_index = DB_ORDER
            .iter()
            .position(|&k| k == initial_kind)
            .ok_or_else(|| anyhow!("Unsupported database kind {:?}", initial_kind))?;

        let (schema, db, cf_names, db_path) = open_components(&base_path, initial_kind)?;

        let mut app = Self {
            base_path,
            limit: max(1, limit),
            kind_index,
            schema,
            db,
            cf_names,
            selected_cf: 0,
            entries: Vec::new(),
            selected_entry: 0,
            db_path,
            status: None,
        };
        app.reload_entries()?;
        Ok(app)
    }

    fn draw(&self, frame: &mut Frame<'_>) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(4),
                Constraint::Min(10),
                Constraint::Length(9),
            ])
            .split(frame.size());

        let info_lines = vec![
            Line::from(vec![
                Span::raw("Schema: "),
                Span::styled(
                    self.schema.name(),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw("  DB Path: "),
                Span::raw(self.db_path.display().to_string()),
            ]),
            Line::from(
                "Controls: Tab/Shift-Tab DB • ←/→ CF • ↑/↓ move • PgUp/PgDn page • g/G start/end • r reload • q exit",
            ),
            Line::from(self.status.clone().unwrap_or_default()),
        ];
        let header = Paragraph::new(info_lines)
            .block(tuiBlock::default().title("Info").borders(Borders::ALL));
        frame.render_widget(header, layout[0]);

        let body_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(26), Constraint::Min(20)])
            .split(layout[1]);

        let cf_items: Vec<ListItem> = self
            .cf_names
            .iter()
            .map(|name| ListItem::new(name.clone()))
            .collect();
        let mut cf_state = ListState::default();
        cf_state.select(Some(self.selected_cf));
        let cf_list = List::new(cf_items)
            .block(
                tuiBlock::default()
                    .title("Column Families")
                    .borders(Borders::ALL),
            )
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        frame.render_stateful_widget(cf_list, body_layout[0], &mut cf_state);

        let entry_items: Vec<ListItem> = if self.entries.is_empty() {
            vec![ListItem::new("⟡ No entries loaded")]
        } else {
            self.entries
                .iter()
                .map(|entry| ListItem::new(entry.summary.clone()))
                .collect()
        };
        let mut entry_state = ListState::default();
        if !self.entries.is_empty() {
            entry_state.select(Some(self.selected_entry));
        }
        let entries_title = format!(
            "Entries (showing up to {}, loaded {})",
            self.limit,
            self.entries.len()
        );
        let entry_list = List::new(entry_items)
            .block(
                tuiBlock::default()
                    .title(entries_title)
                    .borders(Borders::ALL),
            )
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        frame.render_stateful_widget(entry_list, body_layout[1], &mut entry_state);

        let detail_text = if self.entries.is_empty() {
            "Select a column family or press r to reload.".to_owned()
        } else {
            self.entries[self.selected_entry].detail.clone()
        };
        let detail = Paragraph::new(detail_text)
            .block(
                tuiBlock::default()
                    .title("Entry Details")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false });
        frame.render_widget(detail, layout[2]);
    }

    fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Ok(true),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(true),
            KeyCode::Tab => self.switch_db(1)?,
            KeyCode::BackTab => self.switch_db(-1)?,
            KeyCode::Char(']') if key.modifiers.is_empty() => self.switch_db(1)?,
            KeyCode::Char('[') if key.modifiers.is_empty() => self.switch_db(-1)?,
            KeyCode::Char('n') if key.modifiers.is_empty() => self.switch_db(1)?,
            KeyCode::Char('p') if key.modifiers.is_empty() => self.switch_db(-1)?,
            KeyCode::Left => self.prev_cf()?,
            KeyCode::Right => self.next_cf()?,
            KeyCode::Up => self.move_selection(-1),
            KeyCode::Down => self.move_selection(1),
            KeyCode::PageUp => self.move_selection(-10),
            KeyCode::PageDown => self.move_selection(10),
            KeyCode::Char('g') if key.modifiers.is_empty() => self.jump_to_start(),
            KeyCode::Char('G') | KeyCode::End => self.jump_to_end(),
            KeyCode::Home => self.jump_to_start(),
            KeyCode::Char('r') => self.reload_entries()?,
            _ => {}
        }
        Ok(false)
    }

    fn prev_cf(&mut self) -> Result<()> {
        if self.cf_names.is_empty() {
            return Ok(());
        }
        self.selected_cf = if self.selected_cf == 0 {
            self.cf_names.len() - 1
        } else {
            self.selected_cf - 1
        };
        self.reload_entries()
    }

    fn next_cf(&mut self) -> Result<()> {
        if self.cf_names.is_empty() {
            return Ok(());
        }
        self.selected_cf = (self.selected_cf + 1) % self.cf_names.len();
        self.reload_entries()
    }

    fn move_selection(&mut self, delta: isize) {
        if self.entries.is_empty() {
            return;
        }
        let len = self.entries.len() as isize;
        let next = min(max(self.selected_entry as isize + delta, 0), len - 1);
        self.selected_entry = next as usize;
    }

    fn jump_to_start(&mut self) {
        if !self.entries.is_empty() {
            self.selected_entry = 0;
        }
    }

    fn jump_to_end(&mut self) {
        if !self.entries.is_empty() {
            self.selected_entry = self.entries.len() - 1;
        }
    }

    fn reload_entries(&mut self) -> Result<()> {
        let cf_name = &self.cf_names[self.selected_cf];
        let cf_handle = self
            .db
            .cf_handle(cf_name)
            .with_context(|| format!("missing column family `{cf_name}`"))?;

        let mut entries = Vec::new();
        for (idx, result) in self
            .db
            .iterator_cf(cf_handle, IteratorMode::Start)
            .enumerate()
        {
            if idx >= self.limit {
                break;
            }
            let (key, value) = result?;
            match self.schema.format_entry(cf_name, &key, &value) {
                Ok(entry) => entries.push(entry),
                Err(err) => entries.push(Entry::from_error(cf_name, &key, err)),
            }
        }

        self.entries = entries;
        self.selected_entry = 0;
        self.status = Some(format!(
            "Loaded {} entries from `{}`",
            self.entries.len(),
            cf_name
        ));
        Ok(())
    }

    fn switch_db(&mut self, delta: isize) -> Result<()> {
        let len = DB_ORDER.len() as isize;
        if len == 0 {
            return Ok(());
        }
        let mut idx = self.kind_index as isize + delta;
        idx = ((idx % len) + len) % len;
        self.set_kind(idx as usize)?;
        self.reload_entries()?;
        self.status = Some(format!(
            "Switched to {} database ({} column families)",
            self.schema.name(),
            self.cf_names.len()
        ));
        Ok(())
    }

    fn set_kind(&mut self, new_index: usize) -> Result<()> {
        let kind = DB_ORDER
            .get(new_index)
            .copied()
            .ok_or_else(|| anyhow!("Invalid database index {new_index}"))?;
        let (schema, db, cf_names, db_path) = open_components(&self.base_path, kind)?;
        self.schema = schema;
        self.db = db;
        self.cf_names = cf_names;
        self.selected_cf = 0;
        self.entries.clear();
        self.selected_entry = 0;
        self.db_path = db_path;
        self.kind_index = new_index;
        Ok(())
    }
}

struct Entry {
    summary: String,
    detail: String,
}

impl Entry {
    fn new(summary: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            detail: detail.into(),
        }
    }

    fn from_error(cf: &str, key: &[u8], err: anyhow::Error) -> Self {
        let summary = format!(
            "[{cf}] key={} (decode error)",
            short_hex(key, 16).unwrap_or_else(|_| "<invalid>".into())
        );
        let detail = format!("Failed to decode entry: {err:?}");
        Self::new(summary, detail)
    }
}

trait Schema {
    fn name(&self) -> &'static str;
    fn db_path(&self, base: &Path) -> PathBuf;
    fn column_families(&self) -> &'static [&'static str];
    fn format_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Entry>;
}

struct BlockReplaySchema;

impl Schema for BlockReplaySchema {
    fn name(&self) -> &'static str {
        "block_replay_wal"
    }

    fn db_path(&self, base: &Path) -> PathBuf {
        base.join("block_replay_wal")
    }

    fn column_families(&self) -> &'static [&'static str] {
        &[
            "context",
            "last_processed_l1_tx_id",
            "txs",
            "node_version",
            "block_output_hash",
            "latest",
        ]
    }

    fn format_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Entry> {
        match cf {
            "context" => {
                let block = decode_u64(key)?;
                let (ctx, _) =
                    bincode::serde::decode_from_slice::<BlockContext, _>(value, standard())?;
                let summary = format!(
                    "block {block}: ts={} basefee={} pubdata_limit={}",
                    ctx.timestamp, ctx.eip1559_basefee, ctx.pubdata_limit
                );
                let detail = format!(
                    "Block #{block}\nTimestamp: {}\nChain ID: {}\nGas limit: {}\nPubdata limit: {}\nBase fee: {}\nFee recipient: {}\nExecution version: {}\nHashes[255]: {}\n",
                    ctx.timestamp,
                    ctx.chain_id,
                    ctx.gas_limit,
                    ctx.pubdata_limit,
                    ctx.eip1559_basefee,
                    format_address(ctx.coinbase),
                    ctx.execution_version,
                    ctx.block_hashes.0[255]
                );
                Ok(Entry::new(summary, detail))
            }
            "last_processed_l1_tx_id" => {
                let block = decode_u64(key)?;
                let (l1_id, _) = bincode::serde::decode_from_slice::<u64, _>(
                    value,
                    bincode::config::standard(),
                )?;
                let summary = format!("block {block}: next L1 priority id {l1_id}");
                let detail = format!("Block #{block}\nLast processed L1 tx id: {l1_id}");
                Ok(Entry::new(summary, detail))
            }
            "txs" => {
                let block = decode_u64(key)?;
                let (txs, _) =
                    bincode::decode_from_slice::<Vec<ZkTransaction>, _>(value, standard())?;
                let counts = tx_counts(&txs);
                let summary = format!(
                    "block {block}: {} txs (L1 {}, L2 {}, upgrade {})",
                    txs.len(),
                    counts.l1,
                    counts.l2,
                    counts.upgrade
                );
                let mut detail = format!("Block #{block} transactions ({} total):\n", txs.len());
                for (idx, tx) in txs.iter().enumerate() {
                    let _ = writeln!(
                        detail,
                        "  #{idx:<3} {} | nonce {} | to {}",
                        tx_summary(tx),
                        tx.nonce(),
                        format_optional_address(tx.to())
                    );
                }
                Ok(Entry::new(summary, detail))
            }
            "node_version" => {
                let block = decode_u64(key)?;
                let version =
                    String::from_utf8(value.to_vec()).context("node_version entry is not UTF-8")?;
                let summary = format!("block {block}: node {version}");
                let detail = format!("Block #{block}\nNode version: {version}");
                Ok(Entry::new(summary, detail))
            }
            "block_output_hash" => {
                let block = decode_u64(key)?;
                let hash = decode_b256(value, "block output hash")?;
                let summary = format!("block {block}: output {}", format_b256(hash, 12));
                let detail = format!(
                    "Block #{block}\nBlock output hash: {}",
                    format_b256(hash, 0)
                );
                Ok(Entry::new(summary, detail))
            }
            "latest" => {
                let key_str = String::from_utf8_lossy(key);
                let block = decode_u64(value)?;
                let summary = format!("{key_str} → {block}");
                let detail =
                    format!("Metadata key `{key_str}`\nValue: {block} (latest block number)");
                Ok(Entry::new(summary, detail))
            }
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}

struct PreimagesSchema;

impl Schema for PreimagesSchema {
    fn name(&self) -> &'static str {
        "preimages_full_diffs"
    }

    fn db_path(&self, base: &Path) -> PathBuf {
        base.join("preimages_full_diffs")
    }

    fn column_families(&self) -> &'static [&'static str] {
        &["storage"]
    }

    fn format_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Entry> {
        match cf {
            "storage" => {
                let hash = decode_b256(key, "preimage key")?;
                let summary = format!("{} → {} bytes", format_b256(hash, 12), value.len());
                let detail = format!(
                    "Hash: {}\nLength: {} bytes\nHex: {}\nASCII preview: {}\n",
                    format_b256(hash, 0),
                    value.len(),
                    truncate_hex(value, 256),
                    ascii_preview(value, 64)
                );
                Ok(Entry::new(summary, detail))
            }
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}

struct StateSchema;

impl Schema for StateSchema {
    fn name(&self) -> &'static str {
        "state_full_diffs"
    }

    fn db_path(&self, base: &Path) -> PathBuf {
        base.join("state_full_diffs")
    }

    fn column_families(&self) -> &'static [&'static str] {
        &["data", "meta"]
    }

    fn format_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Entry> {
        match cf {
            "data" => {
                let key_b256 = decode_b256(&key[0..32], "storage key")?;
                let block = decode_u64(&key[32..40])?;
                let value_b256 = decode_b256(value, "storage value")?;
                let summary = format!(
                    "{} (block #{block}) → {}",
                    format_b256(key_b256, 12),
                    format_b256(value_b256, 12)
                );
                let detail = format!(
                    "Key: {}\nBlock: {}\nValue (B256): {}\nValue (U256): {}\n",
                    format_b256(key_b256, 0),
                    block,
                    format_b256(value_b256, 0),
                    U256::from_be_bytes(value_b256.0)
                );
                Ok(Entry::new(summary, detail))
            }
            "meta" => {
                let key_str = String::from_utf8_lossy(key);
                let base_block = decode_u64(value)?;
                let summary = format!("{key_str} → {base_block}");
                let detail = format!("Metadata key `{key_str}`\nBase block number: {base_block}");
                Ok(Entry::new(summary, detail))
            }
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}

struct RepositorySchema;

impl Schema for RepositorySchema {
    fn name(&self) -> &'static str {
        "repository"
    }

    fn db_path(&self, base: &Path) -> PathBuf {
        base.join("repository")
    }

    fn column_families(&self) -> &'static [&'static str] {
        &[
            "block_data",
            "block_number_to_hash",
            "tx",
            "tx_receipt",
            "tx_meta",
            "initiator_and_nonce_to_hash",
            "meta",
        ]
    }

    fn format_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Entry> {
        match cf {
            "block_data" => {
                let hash = decode_b256(key, "block hash")?;
                let mut slice = value;
                let block: Block<TxHash> = Block::decode(&mut slice)
                    .context("decoding block rlp")
                    .unwrap();
                let sealed = RepositoryBlock::new_unchecked(block, hash);
                let header = &sealed.header;
                let tx_count = sealed.body.transactions.len();
                let summary = format!(
                    "{} → block #{}, {} txs",
                    format_b256(hash, 8),
                    header.number,
                    tx_count
                );
                let detail = format!(
                    "Block hash: {}\nNumber: {}\nTimestamp: {}\nGas used: {}\nTx count: {}\nRaw size: {} bytes\n",
                    format_b256(hash, 0),
                    header.number,
                    header.timestamp,
                    header.gas_used,
                    tx_count,
                    value.len()
                );
                Ok(Entry::new(summary, detail))
            }
            "block_number_to_hash" => {
                let number = decode_u64(key)?;
                let hash = decode_b256(value, "block hash")?;
                let summary = format!("block #{number} → {}", format_b256(hash, 12));
                let detail = format!("Block #{number}\nHash: {}", format_b256(hash, 0));
                Ok(Entry::new(summary, detail))
            }
            "tx" => {
                let hash = decode_b256(key, "tx hash")?;
                let mut slice = value;
                let tx = ZkEnvelope::decode_2718(&mut slice)?
                    .try_into_recovered()
                    .expect("transaction saved in DB is not EC recoverable");
                let summary = format!(
                    "{} → nonce {} to {}",
                    format_b256(hash, 12),
                    tx.nonce(),
                    format_optional_address(tx.to())
                );
                let detail = format!(
                    "Transaction hash: {}\nType: {:?}\nNonce: {}\nGas price: {}\nGas limit: {}\nTo: {}\nValue: {}\nRaw length: {} bytes\n",
                    format_b256(hash, 0),
                    tx.inner.kind(),
                    tx.nonce(),
                    tx.inner.effective_gas_price(None),
                    tx.gas_limit(),
                    format_optional_address(tx.to()),
                    tx.inner.value(),
                    value.len()
                );
                Ok(Entry::new(summary, detail))
            }
            "tx_receipt" => {
                let hash = decode_b256(key, "tx hash")?;
                let mut slice = value;
                let receipt = ZkReceiptEnvelope::decode_2718(&mut slice)?;
                let summary = format!(
                    "{} → status {} logs {}",
                    format_b256(hash, 12),
                    receipt.status(),
                    receipt.logs().len()
                );
                let detail = format!(
                    "Transaction hash: {}\nType: {}\nStatus: {}\nGas used: {}\nLogs: {}\nRaw length: {} bytes\n",
                    format_b256(hash, 0),
                    receipt.tx_type(),
                    receipt.status(),
                    receipt.cumulative_gas_used(),
                    receipt.logs().len(),
                    value.len()
                );
                Ok(Entry::new(summary, detail))
            }
            "tx_meta" => {
                let hash = decode_b256(key, "tx hash")?;
                let mut slice = value;
                let meta = TxMeta::decode(&mut slice)?;
                let summary = format!(
                    "{} → block {} (index {})",
                    format_b256(hash, 12),
                    meta.block_number,
                    meta.tx_index_in_block
                );
                let detail = format!(
                    "Transaction hash: {}\nBlock hash: {}\nBlock number: {}\nTimestamp: {}\nGas used: {}\nEffective gas price: {}\nIndex in block: {}\nLogs before this tx: {}\nContract address: {}\n",
                    format_b256(hash, 0),
                    format_b256(meta.block_hash, 0),
                    meta.block_number,
                    meta.block_timestamp,
                    meta.gas_used,
                    meta.effective_gas_price,
                    meta.tx_index_in_block,
                    meta.number_of_logs_before_this_tx,
                    meta.contract_address.map_or_else(|| "none".into(), format_address)
                );
                Ok(Entry::new(summary, detail))
            }
            "initiator_and_nonce_to_hash" => {
                ensure_len(key, 28, "initiator+nonce key")?;
                let (addr_bytes, nonce_bytes) = key.split_at(20);
                let address = Address::from_slice(addr_bytes);
                let nonce = decode_u64(nonce_bytes)?;
                let hash = decode_b256(value, "tx hash")?;
                let summary = format!(
                    "{} nonce {} → {}",
                    format_address(address),
                    nonce,
                    format_b256(hash, 12)
                );
                let detail = format!(
                    "Initiator: {}\nNonce: {}\nTransaction hash: {}\n",
                    format_address(address),
                    nonce,
                    format_b256(hash, 0)
                );
                Ok(Entry::new(summary, detail))
            }
            "meta" => {
                let key_str = String::from_utf8_lossy(key);
                let number = decode_u64(value)?;
                let summary = format!("{key_str} → {number}");
                let detail = format!("Metadata key `{key_str}`\nLatest block number: {number}");
                Ok(Entry::new(summary, detail))
            }
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}

struct TreeSchema;

impl Schema for TreeSchema {
    fn name(&self) -> &'static str {
        "tree"
    }

    fn db_path(&self, base: &Path) -> PathBuf {
        base.join("tree")
    }

    fn column_families(&self) -> &'static [&'static str] {
        &["default", "key_indices"]
    }

    fn format_entry(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Entry> {
        match cf {
            "default" => {
                let summary = if key == [0] {
                    "manifest".to_string()
                } else {
                    format!("node key {} ({} bytes)", short_hex(key, 12)?, value.len())
                };
                let detail = format!(
                    "Key hex: {}\nValue length: {} bytes\nValue hex (truncated): {}\n",
                    format_hex(key),
                    value.len(),
                    truncate_hex(value, 256)
                );
                Ok(Entry::new(summary, detail))
            }
            "key_indices" => {
                ensure_len(value, 16, "key index value")?;
                let hash = decode_b256(key, "key hash")?;
                let (index, version) = decode_u64_pair(value)?;
                let summary = format!(
                    "{} → index {} (version {})",
                    format_b256(hash, 12),
                    index,
                    version
                );
                let detail = format!(
                    "Key hash: {}\nLeaf index: {}\nFirst stored at version: {}\n",
                    format_b256(hash, 0),
                    index,
                    version
                );
                Ok(Entry::new(summary, detail))
            }
            other => Err(anyhow!("Unsupported column family `{other}`")),
        }
    }
}

fn schema_for_kind(kind: DbKind) -> Box<dyn Schema> {
    match kind {
        DbKind::BlockReplayWal => Box::new(BlockReplaySchema),
        DbKind::Preimages => Box::new(PreimagesSchema),
        DbKind::Repository => Box::new(RepositorySchema),
        DbKind::State => Box::new(StateSchema),
        DbKind::Tree => Box::new(TreeSchema),
    }
}

fn open_components(
    base_path: &Path,
    kind: DbKind,
) -> Result<(Box<dyn Schema>, RocksDb, Vec<String>, PathBuf)> {
    let schema = schema_for_kind(kind);
    let db_path = schema.db_path(base_path);
    if !db_path.exists() {
        return Err(anyhow!(
            "Database directory `{}` does not exist",
            db_path.display()
        ));
    }

    let mut options = Options::default();
    options.create_if_missing(false);
    let cf_descriptors: Vec<_> = schema
        .column_families()
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
        .collect();

    let db = RocksDb::open_cf_descriptors(&options, &db_path, cf_descriptors)
        .with_context(|| format!("opening RocksDB at {}", db_path.display()))?;
    let cf_names = schema
        .column_families()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();

    Ok((schema, db, cf_names, db_path))
}

struct TxCounts {
    l1: usize,
    l2: usize,
    upgrade: usize,
}

fn tx_counts(txs: &[ZkTransaction]) -> TxCounts {
    let mut counts = TxCounts {
        l1: 0,
        l2: 0,
        upgrade: 0,
    };
    for tx in txs {
        match tx.tx_type() {
            ZkTxType::L1 => counts.l1 += 1,
            ZkTxType::Upgrade => counts.upgrade += 1,
            ZkTxType::L2(_) => counts.l2 += 1,
        }
    }
    counts
}

fn tx_summary(tx: &ZkTransaction) -> String {
    format!("{} signer {}", tx.tx_type(), format_address(tx.signer()))
}

fn decode_u64(bytes: &[u8]) -> Result<u64> {
    ensure_len(bytes, 8, "u64")?;
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(arr))
}

fn decode_u64_pair(bytes: &[u8]) -> Result<(u64, u64)> {
    ensure_len(bytes, 16, "u64 pair")?;
    let (left, right) = bytes.split_at(8);
    Ok((decode_u64(left)?, decode_u64(right)?))
}

fn decode_b256(bytes: &[u8], what: &str) -> Result<B256> {
    ensure_len(bytes, 32, what)?;
    Ok(B256::from_slice(bytes))
}

fn ensure_len(bytes: &[u8], expected: usize, what: &str) -> Result<()> {
    if bytes.len() != expected {
        Err(anyhow!(
            "Invalid {what} length: expected {expected}, got {}",
            bytes.len()
        ))
    } else {
        Ok(())
    }
}

fn format_b256(value: B256, truncate_to: usize) -> String {
    let encoded = value.encode_hex::<String>();
    if truncate_to > 0 && encoded.len() > truncate_to * 2 {
        format!("0x{}…", &encoded[..truncate_to * 2])
    } else {
        format!("0x{encoded}")
    }
}

fn format_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn truncate_hex(bytes: &[u8], limit: usize) -> String {
    if bytes.len() <= limit {
        format_hex(bytes)
    } else {
        let truncated = &bytes[..limit];
        let mut hex_repr = format_hex(truncated);
        hex_repr.push('…');
        format!("{hex_repr} (total {} bytes)", bytes.len())
    }
}

fn ascii_preview(bytes: &[u8], limit: usize) -> String {
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

fn short_hex(bytes: &[u8], max: usize) -> Result<String> {
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

fn format_address(address: Address) -> String {
    format!("0x{}", hex::encode(address.as_slice()))
}

fn format_optional_address(address: Option<Address>) -> String {
    address.map(format_address).unwrap_or_else(|| "none".into())
}
