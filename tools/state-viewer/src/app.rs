use std::{
    cmp::Ordering,
    collections::HashMap,
    mem,
    path::{Path, PathBuf},
};

use anyhow::{Result, anyhow};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::db::{DbStore, open_components};
use crate::schema::{DB_KINDS, DbKind, EntryField, EntryRecord, FieldKind, FieldValue, Schema};

#[derive(Debug, Default)]
enum Mode {
    #[default]
    Browse,
    Prompt(PromptState),
    Confirm(ConfirmState),
}

#[derive(Clone, Debug)]
struct SortState {
    field: String,
    descending: bool,
}

impl SortState {
    fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            descending: false,
        }
    }

    fn toggle(&mut self) {
        self.descending = !self.descending;
    }

    fn label(&self) -> String {
        let arrow = if self.descending { "↓" } else { "↑" };
        format!("{} {arrow}", self.field)
    }
}

#[derive(Clone, Debug)]
struct SearchFilter {
    cf: String,
    field: String,
    raw_input: String,
    value_label: String,
}

impl SearchFilter {
    fn label(&self) -> String {
        format!("{}={}", self.field, self.value_label)
    }
}

#[derive(Debug)]
pub(crate) struct PromptState {
    title: String,
    message: Option<String>,
    input: String,
    error: Option<String>,
    kind: PromptKind,
}

impl PromptState {
    fn new(kind: PromptKind, title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            message: None,
            input: String::new(),
            error: None,
            kind,
        }
    }

    fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    fn push_char(&mut self, ch: char) {
        self.input.push(ch);
    }

    fn pop_char(&mut self) {
        self.input.pop();
    }

    fn set_error(&mut self, message: impl Into<String>) {
        self.error = Some(message.into());
    }

    fn clear_error(&mut self) {
        self.error = None;
    }

    pub(crate) fn title(&self) -> &str {
        &self.title
    }

    pub(crate) fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    pub(crate) fn input(&self) -> &str {
        &self.input
    }

    pub(crate) fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}

#[derive(Clone, Debug)]
enum PromptKind {
    Search {
        cf: String,
    },
    SelectEditableField {
        cf: String,
        key: Vec<u8>,
        options: Vec<String>,
    },
    EditField {
        cf: String,
        key: Vec<u8>,
        field: String,
        kind: FieldKind,
    },
}

#[derive(Debug)]
pub(crate) struct ConfirmState {
    title: String,
    message: String,
    kind: ConfirmKind,
}

impl ConfirmState {
    fn new(kind: ConfirmKind, title: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            message: message.into(),
            kind,
        }
    }

    pub(crate) fn title(&self) -> &str {
        &self.title
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Clone, Debug)]
enum ConfirmKind {
    DeleteEntry { cf: String, key: Vec<u8> },
}

pub struct App {
    base_path: PathBuf,
    limit: usize,
    kind_index: usize,
    schema: Box<dyn Schema>,
    db: DbStore,
    cf_names: Vec<String>,
    selected_cf: usize,
    entries: Vec<EntryRecord>,
    selected_entry: usize,
    db_path: PathBuf,
    mode: Mode,
    sort_states: HashMap<String, SortState>,
    search_filter: Option<SearchFilter>,
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
            mode: Mode::default(),
            sort_states: HashMap::new(),
            search_filter: None,
            status: None,
        };
        app.reload_entries()?;
        Ok(app)
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
        match self.mode {
            Mode::Browse => self.handle_browse_key(key),
            Mode::Prompt(_) => self.handle_prompt_key(key),
            Mode::Confirm(_) => self.handle_confirm_key(key),
        }
    }

    fn handle_browse_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc => return Ok(true),
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
            KeyCode::Home => self.jump_to_start(),
            KeyCode::End => self.jump_to_end(),
            KeyCode::Char(ch) => {
                let lower = ch.to_ascii_lowercase();
                let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
                let shift = key.modifiers.contains(KeyModifiers::SHIFT);
                match lower {
                    'q' if !ctrl => return Ok(true),
                    'c' if ctrl => return Ok(true),
                    'g' if !ctrl && !shift => self.jump_to_start(),
                    'g' if shift && !ctrl => self.jump_to_end(),
                    'r' if !ctrl => self.reload_entries()?,
                    's' if shift => self.toggle_sort_order()?,
                    's' if !shift && !ctrl => self.advance_sort_field()?,
                    'x' if !shift && !ctrl => self.clear_sort_for_current_cf()?,
                    '/' if !ctrl => self.start_search_prompt()?,
                    'e' if !ctrl && !shift => self.start_edit_flow()?,
                    'd' if !ctrl && !shift => self.ask_delete_entry()?,
                    _ => {}
                }
            }
            _ => {}
        }
        Ok(false)
    }

    fn handle_prompt_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Esc => {
                self.mode = Mode::Browse;
                self.status = Some("Cancelled".into());
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.mode = Mode::Browse;
                self.status = Some("Cancelled".into());
            }
            KeyCode::Enter => {
                let prompt_state = match mem::replace(&mut self.mode, Mode::Browse) {
                    Mode::Prompt(state) => state,
                    other => {
                        self.mode = other;
                        return Ok(false);
                    }
                };
                self.process_prompt(prompt_state)?;
            }
            KeyCode::Backspace => {
                if let Mode::Prompt(prompt) = &mut self.mode {
                    prompt.clear_error();
                    prompt.pop_char();
                }
            }
            KeyCode::Tab => {
                if let Mode::Prompt(prompt) = &mut self.mode {
                    prompt.clear_error();
                    prompt.push_char(' ');
                }
            }
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Mode::Prompt(prompt) = &mut self.mode {
                    prompt.clear_error();
                    prompt.push_char(ch);
                }
            }
            _ => {}
        }
        Ok(false)
    }

    fn handle_confirm_key(&mut self, key: KeyEvent) -> Result<bool> {
        if !matches!(self.mode, Mode::Confirm(_)) {
            return Ok(false);
        }
        match key.code {
            KeyCode::Esc => self.cancel_confirm(),
            KeyCode::Char('n') | KeyCode::Char('N') => self.cancel_confirm(),
            KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => self.apply_confirm()?,
            _ => {}
        }
        Ok(false)
    }

    fn cancel_confirm(&mut self) {
        self.mode = Mode::Browse;
        self.status = Some("Cancelled".into());
    }

    fn apply_confirm(&mut self) -> Result<()> {
        let confirm_state = match mem::replace(&mut self.mode, Mode::Browse) {
            Mode::Confirm(state) => state,
            other => {
                self.mode = other;
                return Ok(());
            }
        };

        match confirm_state.kind {
            ConfirmKind::DeleteEntry { cf, key } => self.execute_delete(cf, key)?,
        }
        Ok(())
    }

    fn process_prompt(&mut self, mut prompt: PromptState) -> Result<()> {
        match prompt.kind.clone() {
            PromptKind::Search { cf } => {
                let input = prompt.input().trim();
                if input.is_empty() {
                    prompt.set_error("Enter a field=value pair or value to search");
                    self.mode = Mode::Prompt(prompt);
                    return Ok(());
                }
                if let Err(err) = self.execute_search(&cf, input) {
                    prompt.set_error(err.to_string());
                    self.mode = Mode::Prompt(prompt);
                } else {
                    self.mode = Mode::Browse;
                }
            }
            PromptKind::SelectEditableField { cf, key, options } => {
                let input = prompt.input().trim();
                if input.is_empty() {
                    prompt.set_error("Enter a field name to edit");
                    self.mode = Mode::Prompt(prompt);
                    return Ok(());
                }
                let field_name = if let Some(name) = options
                    .iter()
                    .find(|candidate| candidate.eq_ignore_ascii_case(input))
                {
                    name.clone()
                } else {
                    prompt.set_error(format!("Unknown editable field `{input}`"));
                    self.mode = Mode::Prompt(prompt);
                    return Ok(());
                };

                let current_entry = if let Some(entry) = self.find_entry_by_key(&key) {
                    entry
                } else {
                    self.mode = Mode::Browse;
                    self.status = Some("Entry is no longer available".into());
                    return Ok(());
                };

                let field = if let Some(field) = current_entry.field(&field_name) {
                    field
                } else {
                    self.mode = Mode::Browse;
                    self.status = Some("Field is no longer present".into());
                    return Ok(());
                };

                let current_value = field.value().to_string();
                let edit_prompt = PromptState::new(
                    PromptKind::EditField {
                        cf,
                        key,
                        field: field.name().to_string(),
                        kind: field.kind(),
                    },
                    format!("Edit `{}`", field.name()),
                )
                .with_message(format!("Current value: {current_value}"));
                self.mode = Mode::Prompt(edit_prompt);
            }
            PromptKind::EditField {
                cf,
                key,
                field,
                kind,
            } => {
                let input = prompt.input().trim();
                if input.is_empty() {
                    prompt.set_error("Provide a new value");
                    self.mode = Mode::Prompt(prompt);
                    return Ok(());
                }
                let parsed = match kind.parse_value(input) {
                    Ok(value) => value,
                    Err(err) => {
                        prompt.set_error(err.to_string());
                        self.mode = Mode::Prompt(prompt);
                        return Ok(());
                    }
                };
                if let Err(err) = self.apply_field_update(cf, key, field, parsed) {
                    prompt.set_error(err.to_string());
                    self.mode = Mode::Prompt(prompt);
                } else {
                    self.mode = Mode::Browse;
                }
            }
        }
        Ok(())
    }

    fn collect_fields_matching<F>(&self, mut predicate: F) -> Vec<String>
    where
        F: FnMut(&EntryField) -> bool,
    {
        let mut names: Vec<String> = Vec::new();
        for entry in &self.entries {
            for field in entry.fields() {
                if predicate(field)
                    && !names
                        .iter()
                        .any(|existing| existing.eq_ignore_ascii_case(field.name()))
                {
                    names.push(field.name().to_string());
                }
            }
        }
        names
    }

    fn advance_sort_field(&mut self) -> Result<()> {
        let cf_name = match self.selected_cf_name() {
            Some(name) => name.to_string(),
            None => {
                self.status = Some("Select a column family first".into());
                return Ok(());
            }
        };

        let sortable_fields = self.collect_fields_matching(|field| field.capabilities().sortable);
        if sortable_fields.is_empty() {
            self.status = Some("No sortable fields for this column family".into());
            return Ok(());
        }

        let next_field = match self.sort_states.get(&cf_name) {
            Some(state) => {
                let current_idx = sortable_fields
                    .iter()
                    .position(|name| name.eq_ignore_ascii_case(&state.field))
                    .unwrap_or(0);
                sortable_fields[(current_idx + 1) % sortable_fields.len()].clone()
            }
            None => sortable_fields[0].clone(),
        };

        self.sort_states
            .insert(cf_name.clone(), SortState::new(next_field.clone()));
        self.apply_sort_for_cf(&cf_name);
        self.status = Some(format!("Sorted by {next_field} ↑"));
        Ok(())
    }

    fn toggle_sort_order(&mut self) -> Result<()> {
        let cf_name = match self.selected_cf_name() {
            Some(name) => name.to_string(),
            None => {
                self.status = Some("Select a column family first".into());
                return Ok(());
            }
        };

        let sortable_fields = self.collect_fields_matching(|field| field.capabilities().sortable);
        if sortable_fields.is_empty() {
            self.status = Some("No sortable fields for this column family".into());
            return Ok(());
        }

        let state = self
            .sort_states
            .entry(cf_name.clone())
            .or_insert_with(|| SortState::new(sortable_fields[0].clone()));
        state.toggle();
        let descending = state.descending;
        let field_label = state.field.clone();
        let _ = state;
        self.apply_sort_for_cf(&cf_name);
        let direction = if descending { "↓" } else { "↑" };
        self.status = Some(format!("Sorted by {field_label} {direction}"));
        Ok(())
    }

    fn clear_sort_for_current_cf(&mut self) -> Result<()> {
        let cf_name = match self.selected_cf_name() {
            Some(name) => name.to_string(),
            None => {
                self.status = Some("Select a column family first".into());
                return Ok(());
            }
        };

        if self.sort_states.remove(&cf_name).is_some() {
            self.reload_entries_preserving_filter()?;
            self.status = Some("Cleared sorting".into());
        } else {
            self.status = Some("No active sorting".into());
        }
        Ok(())
    }

    fn apply_sort_for_cf(&mut self, cf_name: &str) {
        if let Some(state) = self.sort_states.get(cf_name) {
            let field = state.field.clone();
            let descending = state.descending;
            self.entries.sort_by(|lhs, rhs| {
                let ordering = App::compare_field_values(lhs, rhs, &field);
                if descending {
                    ordering.reverse()
                } else {
                    ordering
                }
            });
        }
    }

    fn apply_sort_for_current_cf(&mut self) {
        if let Some(cf) = self.selected_cf_name().map(|name| name.to_string()) {
            self.apply_sort_for_cf(&cf);
        }
    }
    fn compare_field_values(lhs: &EntryRecord, rhs: &EntryRecord, field_name: &str) -> Ordering {
        match (lhs.field_value(field_name), rhs.field_value(field_name)) {
            (Some(a), Some(b)) => a.cmp(b).unwrap_or_else(|| a.display().cmp(&b.display())),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }

    fn start_search_prompt(&mut self) -> Result<()> {
        let cf_name = match self.selected_cf_name() {
            Some(name) => name.to_string(),
            None => {
                self.status = Some("Select a column family first".into());
                return Ok(());
            }
        };
        let prompt = PromptState::new(
            PromptKind::Search {
                cf: cf_name.clone(),
            },
            format!("Search `{cf_name}`"),
        )
        .with_message("Enter field=value or value to search");
        self.mode = Mode::Prompt(prompt);
        Ok(())
    }

    fn resolve_field_kind(&self, field_name: &str) -> Option<(String, FieldKind)> {
        self.entries.iter().find_map(|entry| {
            entry
                .field(field_name)
                .map(|field| (field.name().to_string(), field.kind()))
        })
    }

    fn execute_search(&mut self, cf: &str, query: &str) -> Result<()> {
        let searchable_fields =
            self.collect_fields_matching(|field| field.capabilities().searchable);
        if searchable_fields.is_empty() {
            return Err(anyhow!("No searchable fields available for `{cf}`"));
        }

        let (field_name_input, value_input) = if let Some((field, value)) = query.split_once('=') {
            (field.trim(), value.trim())
        } else if searchable_fields.len() == 1 {
            (searchable_fields[0].as_str(), query.trim())
        } else {
            return Err(anyhow!(
                "Specify field=value. Available fields: {}",
                searchable_fields.join(", ")
            ));
        };

        if value_input.is_empty() {
            return Err(anyhow!("Provide a value to search for"));
        }

        let (canonical_field, field_kind) = self
            .resolve_field_kind(field_name_input)
            .ok_or_else(|| anyhow!("Field `{field_name_input}` is not available for search"))?;

        let search_value = field_kind.parse_value(value_input)?;

        let mut matches = Vec::new();
        for (key, value) in self.db.scan_all(cf)? {
            if let Ok(entry) = self.schema.decode_entry(cf, &key, &value)
                && entry
                    .field_value(&canonical_field)
                    .map(|field_value| field_value.eq_value(&search_value))
                    .unwrap_or(false)
            {
                matches.push(entry);
            }
        }

        let value_label = search_value.display();
        self.entries = matches;
        self.selected_entry = 0;
        let message = if self.entries.is_empty() {
            format!("No results for {canonical_field}={value_input}")
        } else {
            format!(
                "Found {} entries for {canonical_field}={value_label}",
                self.entries.len()
            )
        };
        self.status = Some(message);
        self.search_filter = Some(SearchFilter {
            cf: cf.to_string(),
            field: canonical_field.clone(),
            raw_input: query.to_string(),
            value_label,
        });
        self.apply_sort_for_cf(cf);
        Ok(())
    }

    fn start_edit_flow(&mut self) -> Result<()> {
        let cf_name = match self.selected_cf_name() {
            Some(name) => name.to_string(),
            None => {
                self.status = Some("Select a column family first".into());
                return Ok(());
            }
        };
        let entry = match self.selected_entry_record() {
            Some(entry) => entry,
            None => {
                self.status = Some("Select an entry first".into());
                return Ok(());
            }
        };
        let editable: Vec<String> = entry
            .fields()
            .iter()
            .filter(|field| field.capabilities().editable)
            .map(|field| field.name().to_string())
            .collect();
        if editable.is_empty() {
            self.status = Some("No editable fields for this entry".into());
            return Ok(());
        }

        let key = entry.key().to_vec();
        if editable.len() == 1 {
            let field_name = &editable[0];
            let field = entry.field(field_name).unwrap();
            let current_value = field.value().to_string();
            let prompt = PromptState::new(
                PromptKind::EditField {
                    cf: cf_name,
                    key,
                    field: field.name().to_string(),
                    kind: field.kind(),
                },
                format!("Edit `{}`", field.name()),
            )
            .with_message(format!("Current value: {current_value}"));
            self.mode = Mode::Prompt(prompt);
        } else {
            let field_list = editable.join(", ");
            let prompt = PromptState::new(
                PromptKind::SelectEditableField {
                    cf: cf_name,
                    key,
                    options: editable,
                },
                "Select field to edit",
            )
            .with_message(format!("Editable fields: {field_list}"));
            self.mode = Mode::Prompt(prompt);
        }
        Ok(())
    }

    fn ask_delete_entry(&mut self) -> Result<()> {
        let cf_name = match self.selected_cf_name() {
            Some(name) => name.to_string(),
            None => {
                self.status = Some("Select a column family first".into());
                return Ok(());
            }
        };
        let entry = match self.selected_entry_record() {
            Some(entry) => entry,
            None => {
                self.status = Some("Select an entry first".into());
                return Ok(());
            }
        };
        let confirm = ConfirmState::new(
            ConfirmKind::DeleteEntry {
                cf: cf_name.clone(),
                key: entry.key().to_vec(),
            },
            "Confirm deletion",
            format!("Delete `{}` from `{}`? (y/n)", entry.summary(), cf_name),
        );
        self.mode = Mode::Confirm(confirm);
        Ok(())
    }

    fn execute_delete(&mut self, cf: String, key: Vec<u8>) -> Result<()> {
        self.db.delete(&cf, &key)?;
        self.status = Some(format!("Deleted entry from `{cf}`"));
        self.reload_entries_preserving_filter()?;
        Ok(())
    }

    fn reload_entries_preserving_filter(&mut self) -> Result<()> {
        let filter = self.search_filter.clone();
        self.reload_entries()?;
        if let Some(filter) = filter
            && let Some(selected_cf) = self.selected_cf_name()
            && selected_cf.eq_ignore_ascii_case(&filter.cf)
        {
            self.execute_search(&filter.cf, &filter.raw_input)?;
        }

        Ok(())
    }

    fn apply_field_update(
        &mut self,
        cf: String,
        key: Vec<u8>,
        field: String,
        new_value: FieldValue,
    ) -> Result<()> {
        let entry_snapshot = match self.find_entry_by_key(&key) {
            Some(entry) => entry,
            None => {
                self.mode = Mode::Browse;
                self.status = Some("Entry is no longer available".into());
                return Ok(());
            }
        };

        if entry_snapshot
            .field_value(&field)
            .map(|value| value.eq_value(&new_value))
            .unwrap_or(false)
        {
            self.status = Some("Value unchanged".into());
            return Ok(());
        }

        let value_label = new_value.display();
        let new_raw = self
            .schema
            .update_value(&cf, entry_snapshot, &field, &new_value)?;
        self.db.put(&cf, &key, &new_raw)?;
        self.status = Some(format!("Updated {field} to {value_label}"));
        self.reload_entries_preserving_filter()?;
        Ok(())
    }

    fn find_entry_by_key(&self, key: &[u8]) -> Option<&EntryRecord> {
        self.entries.iter().find(|entry| entry.key() == key)
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

    pub fn selected_cf_name(&self) -> Option<&str> {
        self.selected_cf().map(|idx| self.cf_names[idx].as_str())
    }

    pub fn entries(&self) -> &[EntryRecord] {
        &self.entries
    }

    pub fn selected_entry(&self) -> Option<usize> {
        if self.entries.is_empty() {
            None
        } else {
            Some(self.selected_entry)
        }
    }

    pub fn selected_entry_record(&self) -> Option<&EntryRecord> {
        self.selected_entry().map(|idx| &self.entries[idx])
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn status_message(&self) -> Option<&str> {
        self.status.as_deref()
    }

    pub fn prompt_state(&self) -> Option<&PromptState> {
        match &self.mode {
            Mode::Prompt(prompt) => Some(prompt),
            _ => None,
        }
    }

    pub fn confirm_state(&self) -> Option<&ConfirmState> {
        match &self.mode {
            Mode::Confirm(confirm) => Some(confirm),
            _ => None,
        }
    }

    pub fn sort_label(&self) -> Option<String> {
        self.selected_cf_name()
            .and_then(|cf| self.sort_states.get(cf))
            .map(|state| state.label())
    }

    pub fn filter_label(&self) -> Option<String> {
        match (&self.search_filter, self.selected_cf_name()) {
            (Some(filter), Some(cf)) if cf.eq_ignore_ascii_case(&filter.cf) => Some(filter.label()),
            _ => None,
        }
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

        let cf_name = self.cf_names[self.selected_cf].clone();
        let mut entries = Vec::new();
        for (key, value) in self.db.scan(cf_name.as_str(), self.limit)? {
            match self.schema.decode_entry(cf_name.as_str(), &key, &value) {
                Ok(entry) => entries.push(entry),
                Err(err) => entries.push(EntryRecord::from_error(cf_name.as_str(), &key, err)),
            }
        }

        self.entries = entries;
        self.selected_entry = 0;
        self.search_filter = None;
        self.apply_sort_for_current_cf();
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
        self.sort_states.clear();
        self.search_filter = None;
        self.mode = Mode::Browse;
        self.kind_index = new_index;
        Ok(())
    }
}

impl App {
    pub fn entries_len(&self) -> usize {
        self.entries.len()
    }
}
