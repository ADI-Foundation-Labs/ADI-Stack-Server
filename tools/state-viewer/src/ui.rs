use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block as TuiBlock, Borders, List, ListItem, ListState, Paragraph, Wrap},
    Frame,
};

use crate::app::App;

pub fn draw(app: &App, frame: &mut Frame<'_>) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Min(10),
            Constraint::Length(9),
        ])
        .split(frame.size());

    let header_lines = build_header_lines(app);
    let header = Paragraph::new(header_lines)
        .block(TuiBlock::default().title("Info").borders(Borders::ALL));
    frame.render_widget(header, layout[0]);

    let body_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(26), Constraint::Min(20)])
        .split(layout[1]);

    render_column_families(app, frame, body_layout[0]);
    render_entries(app, frame, body_layout[1]);
    render_details(app, frame, layout[2]);
}

fn build_header_lines(app: &App) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    lines.push(Line::from(vec![
        Span::raw("Schema: "),
        Span::styled(
            app.schema_name(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("  DB Path: "),
        Span::raw(app.db_path().display().to_string()),
    ]));
    lines.push(Line::from(
        "Controls: Tab/Shift-Tab DB • ←/→ CF • ↑/↓ move • PgUp/PgDn page • g/G start/end • r reload • q exit",
    ));
    if let Some(status) = app.status_message() {
        lines.push(Line::from(status.to_owned()));
    } else {
        lines.push(Line::from(""));
    }
    lines
}

fn render_column_families(app: &App, frame: &mut Frame<'_>, area: ratatui::layout::Rect) {
    let items: Vec<ListItem> = app
        .column_families()
        .iter()
        .map(|name| ListItem::new(name.clone()))
        .collect();

    let mut state = ListState::default();
    if let Some(selected) = app.selected_cf() {
        state.select(Some(selected));
    }

    let list = List::new(items)
        .block(
            TuiBlock::default()
                .title("Column Families")
                .borders(Borders::ALL),
        )
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    frame.render_stateful_widget(list, area, &mut state);
}

fn render_entries(app: &App, frame: &mut Frame<'_>, area: ratatui::layout::Rect) {
    let entries = app.entries();
    let items: Vec<ListItem> = if entries.is_empty() {
        vec![ListItem::new("⟡ No entries loaded")]
    } else {
        entries
            .iter()
            .map(|entry| ListItem::new(entry.summary().to_string()))
            .collect()
    };

    let mut state = ListState::default();
    if let Some(selected) = app.selected_entry() {
        state.select(Some(selected));
    }

    let title = format!(
        "Entries (showing up to {}, loaded {})",
        app.limit(),
        app.entries_len()
    );

    let list = List::new(items)
        .block(
            TuiBlock::default()
                .title(title)
                .borders(Borders::ALL),
        )
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    frame.render_stateful_widget(list, area, &mut state);
}

fn render_details(app: &App, frame: &mut Frame<'_>, area: ratatui::layout::Rect) {
    let detail_text = match app.selected_entry() {
        Some(idx) => app.entries()[idx].detail().to_string(),
        None => "Select a column family or press r to reload.".to_string(),
    };

    let detail = Paragraph::new(detail_text)
        .block(
            TuiBlock::default()
                .title("Entry Details")
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false });

    frame.render_widget(detail, area);
}
