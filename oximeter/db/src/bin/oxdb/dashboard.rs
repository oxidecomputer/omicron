// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Display query data in a terminal dashboard.

// Copyright 2023 Oxide Computer Company

mod event;
mod timeseries;
mod ui;

use crate::DashboardArgs;
use crossterm::event::Event;
use crossterm::event::EventStream;
use futures::FutureExt;
use futures::StreamExt;
use oximeter_db::Client;
use slog::Logger;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

// Spawn task which loops for events from crossterm and sends to main thread.
//
//  (Sends Option which is none if timeout occurred)
//
// Spawn tokio task which fetches the actual data and sends to the main thread.

// Handle events from the terminal
async fn event_loop(
    tx: mpsc::Sender<Event>,
    mut quit: broadcast::Receiver<()>,
) {
    let mut reader = EventStream::new();
    loop {
        let event = reader.next().fuse();
        tokio::select! {
            maybe_event = event => match maybe_event {
                Some(Ok(event)) => tx.send(event).await.unwrap(),
                Some(Err(e)) => panic!("{e}"),
                None => break,
            },
            _ = quit.recv() => break,
        }
    }
}

/// Run a query and display the results in a dashboard.
pub async fn run(
    client: Client,
    _log: Logger,
    args: DashboardArgs,
) -> anyhow::Result<()> {
    // Notify others when it's time to quit.
    let (quit_tx, quit_rx) = broadcast::channel(1);

    // Send events from the `event_loop` task to the UI loop.
    let (event_tx, event_rx) = mpsc::channel(32);
    tokio::spawn(event_loop(event_tx, quit_tx.subscribe()));

    // Spawn task for fetching and updating the actual plotted data.
    let (data_tx, data_rx) = mpsc::channel(32);
    tokio::spawn(timeseries::fetch_loop(
        client,
        args,
        data_tx,
        quit_tx.clone(),
        quit_tx.subscribe(),
    ));

    // Run the UI loop in the main thread.
    ui::run(event_rx, quit_tx, quit_rx, data_rx).await
}
