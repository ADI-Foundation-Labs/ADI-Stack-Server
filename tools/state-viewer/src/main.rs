mod app;
mod db;
mod schema;
mod ui;

use std::io::stdout;

use anyhow::Result;
use app::App;
use clap::Parser;
use crossterm::{
    event::{self, Event},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand as _,
};
use ratatui::{backend::CrosstermBackend, Terminal};
use schema::DbKind;

#[derive(Parser)]
#[command(author, version, about = "Inspect ZKsync OS RocksDB databases", long_about = None)]
struct Args {
    /// Base directory containing RocksDB databases (e.g. ./db/node1)
    #[arg(long, default_value = "./db/node1")]
    data_dir: std::path::PathBuf,
    /// Which database to inspect
    #[arg(long, value_enum, default_value = "repository")]
    db: DbKind,
    /// Maximum number of entries to load per column family
    #[arg(long, default_value_t = 256)]
    limit: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut app = App::new(args.data_dir, args.limit, args.db)?;

    enable_raw_mode()?;
    let mut stdout = stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, &mut app);

    disable_raw_mode()?;
    terminal.backend_mut().execute(LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut App,
) -> Result<()> {
    loop {
        terminal.draw(|frame| ui::draw(app, frame))?;

        if event::poll(std::time::Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if app.handle_key(key)? {
                    break;
                }
            }
        }
    }
    Ok(())
}
