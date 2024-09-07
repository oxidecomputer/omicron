// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test utilities for reconfigurator execution.

use internal_dns::resolver::Resolver;
use nexus_db_queries::{context::OpContext, db::DataStore};
use nexus_types::deployment::{execution::EventBuffer, Blueprint};
use update_engine::TerminalKind;
use uuid::Uuid;

use crate::{overridables::Overridables, RealizeBlueprintOutput};

pub(crate) async fn realize_blueprint_and_expect(
    opctx: &OpContext,
    datastore: &DataStore,
    resolver: &Resolver,
    blueprint: &Blueprint,
    overrides: &Overridables,
) -> (RealizeBlueprintOutput, EventBuffer) {
    // Large enough to handle incoming messages without stalling.
    let (sender, mut receiver) = tokio::sync::mpsc::channel(128);
    let receiver_task = tokio::spawn(async move {
        let mut buffer = EventBuffer::default();
        while let Some(msg) = receiver.recv().await {
            buffer.add_event(msg);
        }
        buffer
    });

    let output = crate::realize_blueprint_with_overrides(
        opctx,
        datastore,
        resolver,
        blueprint,
        Uuid::new_v4(),
        overrides,
        sender,
    )
    .await
    // We expect here rather than in the caller because we want to assert that
    // the result is a `RealizeBlueprintOutput`. Because the latter is
    // `must_use`, the caller may assign it to `_` and miss the `expect` call.
    .expect("failed to execute blueprint");

    let buffer = receiver_task.await.expect("failed to receive events");
    eprintln!("realize_blueprint output: {:#?}", output);

    let status = buffer
        .root_execution_summary()
        .expect("buffer has a root execution")
        .execution_status;
    let terminal_info = status.terminal_info().unwrap_or_else(|| {
        panic!("expected status to be terminal: {:#?}", status)
    });

    assert_eq!(
        terminal_info.kind,
        TerminalKind::Completed,
        "expected completed"
    );

    (output, buffer)
}
