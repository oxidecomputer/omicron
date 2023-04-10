// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::time::Duration;

use anyhow::{bail, Context};
use buf_list::BufList;
use bytes::Buf;
use camino::Utf8PathBuf;
use display::make_displayer;
use omicron_test_utils::dev::test_setup_log;
use spec::{
    ComponentRegistrar, ExampleCompletionMetadata, ExampleComponent,
    ExampleStepId, ExampleStepMetadata, StepHandle, StepProgress, StepResult,
    UpdateEngine,
};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

mod display;
mod spec;

#[tokio::main(worker_threads = 2)]
async fn main() {
    let logctx = test_setup_log("update_engine_basic_example");

    let context = ExampleContext::new(&logctx.log);
    let (display_handle, sender) = make_displayer(&logctx.log);

    let engine = UpdateEngine::new(&logctx.log, sender);

    // Download component 1.
    let component_1 = engine.for_component(ExampleComponent::Component1);
    let download_handle_1 = context.register_download_step(
        &component_1,
        "https://www.example.org".to_owned(),
        1_048_576,
    );

    // An example of a skipped step for component 1.
    context.register_skipped_step(&component_1);

    // Write component 1 out to disk.
    context.register_write_step(&component_1, download_handle_1, false);

    // Download component 2.
    let component_2 = engine.for_component(ExampleComponent::Component2);
    let download_handle_2 = context.register_download_step(
        &component_2,
        "https://www.example.com".to_owned(),
        1_048_576 * 8,
    );

    // Now write component 2 out to disk.
    context.register_write_step(&component_2, download_handle_2, true);

    _ = engine.execute().await;

    // Wait until all messages have been received by the displayer.
    _ = display_handle.await;

    // Do not clean up the log file so people can inspect it.
}

/// Context shared across steps. This forms the lifetime "'a" defined by the
/// UpdateEngine.
struct ExampleContext {
    log: slog::Logger,
    temp_dir: TempDir,
}

impl ExampleContext {
    fn new(log: &slog::Logger) -> Self {
        Self {
            log: log.new(slog::o!("component" => "ExampleContext")),
            temp_dir: TempDir::new().expect("error"),
        }
    }

    fn register_download_step<'a>(
        &'a self,
        registrar: &ComponentRegistrar<'_, 'a>,
        url: String,
        num_bytes: u64,
    ) -> StepHandle<BufList> {
        registrar
            .new_step(
                ExampleStepId::Download,
                format!("Downloading component: {url}"),
                move |cx| async move {
                    // Simulate a download for the artifact, with some retries.
                    slog::debug!(
                        &self.log,
                        "Attempt 1: simulating a download that fails \
                     at 25% ({}/{num_bytes} bytes)",
                        num_bytes / 4,
                    );

                    tokio::time::sleep(Duration::from_millis(100)).await;
                    cx.send_progress(StepProgress::with_current_and_total(
                        num_bytes / 8,
                        num_bytes,
                        serde_json::Value::Null,
                    ))
                    .await;

                    tokio::time::sleep(Duration::from_millis(100)).await;
                    cx.send_progress(StepProgress::with_current_and_total(
                        num_bytes / 4,
                        num_bytes,
                        serde_json::Value::Null,
                    ))
                    .await;

                    // Now indicate that the attempt has failed.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    cx.send_progress(StepProgress::retry(
                        "Simulated failure at 25%",
                    ))
                    .await;

                    slog::debug!(
                        &self.log,
                        "Attempt 2: simulating a download that succeeds \
                     ({num_bytes} bytes)",
                    );

                    // Try a second time, and this time go all the way to 100%.
                    let mut buf_list = BufList::new();
                    for i in 0..10 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        cx.send_progress(StepProgress::with_current_and_total(
                            num_bytes * i / 10,
                            num_bytes,
                            serde_json::Value::Null,
                        ))
                        .await;
                        buf_list.push_chunk(&b"downloaded-data"[..]);
                    }

                    Ok(StepResult::success(
                        buf_list,
                        ExampleCompletionMetadata::Download { num_bytes },
                    ))
                },
            )
            .register()
    }

    fn register_write_step<'a>(
        &'a self,
        registrar: &ComponentRegistrar<'_, 'a>,
        download_handle: StepHandle<BufList>,
        should_error: bool,
    ) {
        let component = *registrar.component();

        // A `StepHandle`'s value can ordinarily only be used by one step. In
        // this example we're going to share the output across multiple steps
        // using into_shared.
        let download_handle = download_handle.into_shared();
        let download_handle_2 = download_handle.clone();

        registrar
            .new_step(
                ExampleStepId::Write,
                "Writing artifact",
                move |cx| async move {
                    let mut buf_list =
                        download_handle.into_value(cx.token()).await;
                    let num_bytes = buf_list.num_bytes() as u64;

                    let destination: Utf8PathBuf = self
                        .temp_dir
                        .path()
                        .join(format!("write_{component:?}"))
                        .try_into()
                        .context("could not convert path to UTF-8")?;

                    let mut file = tokio::fs::File::create(&destination)
                        .await
                        .context("failed to open file for writing")?;
                    let mut total_written = 0;
                    while buf_list.has_remaining() {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        let written_bytes = file
                            .write_buf(&mut buf_list)
                            .await
                            .context("error writing data")?;

                        total_written += written_bytes;
                        cx.send_progress(StepProgress::with_current_and_total(
                            total_written as u64,
                            num_bytes,
                            serde_json::Value::Null,
                        ))
                        .await;

                        if should_error
                            && (buf_list.remaining() as u64) < num_bytes / 2
                        {
                            // Error out as a demonstration.
                            bail!("error!");
                        }
                    }

                    // Demonstrate how to show a warning.
                    Ok(StepResult::warning(
                        (),
                        ExampleCompletionMetadata::Write {
                            num_bytes,
                            destination,
                        },
                        "Example warning",
                    ))
                },
            )
            .with_metadata_fn(move |cx| async move {
                let buf_list = download_handle_2.into_value(cx.token()).await;
                ExampleStepMetadata::Write {
                    path: self.temp_dir.path().to_owned(),
                    num_bytes: buf_list.num_bytes() as u64,
                }
            })
            .register();
    }

    fn register_skipped_step<'a>(
        &'a self,
        registrar: &ComponentRegistrar<'_, 'a>,
    ) {
        registrar
            .new_step(ExampleStepId::Skipped, "This step does nothing", |_cx| async move {
                Ok(StepResult::skipped((), (), "Step skipped"))
            })
            .register();
    }
}
