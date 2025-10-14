use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use zksync_os_rocksdb::rocksdb::IteratorMode;

use crate::db::{open_components, RocksDb};
use crate::schema::{Entry, Schema, DbKind, DB_KINDS};

pub struct App {
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
    pub fn new(base_path: PathBuf, limit: usize, initial_kind: DbKind) -> Result<Self> {
        if !base_path.exists() {
            return Err(anyhow!(
                "Base directory `{}` does not exist",
                base_path.display()
            ));
        }

        let kind_index = DB_KINDS
            .iter()
            .position(|&k| k == initial_kind)
            .ok_or_else(|| anyhow!("Unsupported database kind {initial_kind:?}"))?;

        let (schema, db, cf_names, db_path) = open_components(&base_path, initial_kind)?;

        let mut app = Self {
            base_path,
            limit: limit.max(1),
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

    pub fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
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
            KeyCode::Home | KeyCode::Char('g') if key.modifiers.is_empty() => self.jump_to_start(),
            KeyCode::End | KeyCode::Char('G') => self.jump_to_end(),
            KeyCode::Char('r') => self.reload_entries()?,
            _ => {}
        }
        Ok(false)
    }

    pub fn schema_name(&self) -> &'static str {
        self.schema.name()
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn column_families(&self) -> &[String] {
        &self.cf_names
    }

    pub fn selected_cf(&self) -> Option<usize> {
        if self.cf_names.is_empty() {
            None
        } else {
            Some(self.selected_cf)
        }
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn selected_entry(&self) -> Option<usize> {
        if self.entries.is_empty() {
            None
        } else {
            Some(self.selected_entry)
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn status_message(&self) -> Option<&str> {
        self.status.as_deref()
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
        let next = (self.selected_entry as isize + delta).clamp(0, len - 1);
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
        if self.cf_names.is_empty() {
            self.entries.clear();
            self.status = Some("No column families available".into());
            return Ok(());
        }

        let cf_name = &self.cf_names[self.selected_cf];
        let cf_handle = self
            .db
            .cf_handle(cf_name)
            .ok_or_else(|| anyhow!("missing column family `{cf_name}`"))?;

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
        let len = DB_KINDS.len() as isize;
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
        let kind = DB_KINDS
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

impl App {
    pub fn entries_len(&self) -> usize {
        self.entries.len()
    }
}
