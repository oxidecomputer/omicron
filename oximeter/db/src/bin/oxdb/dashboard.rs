// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Display query data in a terminal dashboard.

// Copyright 2023 Oxide Computer Company

use crate::QueryArgs;
use crossterm::event::DisableMouseCapture;
use crossterm::event::EnableMouseCapture;
use crossterm::execute;
use crossterm::terminal::disable_raw_mode;
use crossterm::terminal::enable_raw_mode;
use crossterm::terminal::EnterAlternateScreen;
use crossterm::terminal::LeaveAlternateScreen;
use dropshot::EmptyScanParams;
use dropshot::WhichPage;
use oximeter_db::Client;
use slog::Logger;
use std::io;
use std::num::NonZeroU32;
use tui::backend::CrosstermBackend;
use tui::Terminal;

/// Run a query and display the results in a dashboard.
pub async fn run(
    client: &Client,
    _log: &Logger,
    _query: &QueryArgs,
) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut it = 0;

    loop {
        let page = WhichPage::First(EmptyScanParams {});
        let count = NonZeroU32::new(1).unwrap();
        let results = client.timeseries_schema_list(&page, count).await?;
        let schema = results.items.first().unwrap();
        terminal.draw(|f| {
            let size = f.size();
            let block = tui::widgets::Block::default()
                .title(format!("{} ({})", schema.timeseries_name, it))
                .borders(tui::widgets::Borders::ALL);
            f.render_widget(block, size);
        })?;
        it += 1;
        std::thread::sleep(std::time::Duration::from_secs(1));
        if it == 5 {
            break;
        }
    }

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}
