// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{io::IsTerminal, time::Duration};

use anyhow::{Context, Result, bail};
use buf_list::BufList;
use bytes::Buf;
use camino::Utf8PathBuf;
use camino_tempfile::Utf8TempDir;
use clap::{Parser, ValueEnum};
use display::make_displayer;
use omicron_test_utils::dev::test_setup_log;
use spec::{
    ComponentRegistrar, EventBuffer, ExampleCompletionMetadata,
    ExampleComponent, ExampleSpec, ExampleStepId, ExampleStepMetadata,
    ExampleWriteSpec, ExampleWriteStepId, StepHandle, StepProgress,
    StepSkipped, StepWarning, UpdateEngine,
};
use tokio::{io::AsyncWriteExt, sync::mpsc};
use update_engine::{
    StepContext, StepSuccess,
    events::{Event, ProgressUnits},
};

mod display;
mod spec;

#[expect(
    clippy::disallowed_macros,
    reason = "using tokio::main avoids an `oxide-tokio-rt` dependency for \
        examples"
)]
#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    let app = App::parse();
    app.exec().await
}

#[derive(Debug, Parser)]
struct App {
    /// Display style to use.
    #[clap(long, short = 's', default_value_t, value_enum)]
    display_style: DisplayStyleOpt,

    /// Prefix to set on all log messages with display-style=line.
    #[clap(long, short = 'p')]
    prefix: Option<String>,
}

impl App {
    async fn exec(self) -> Result<()> {
        let logctx = test_setup_log("update_engine_basic_example");

        let display_style = match self.display_style {
            DisplayStyleOpt::ProgressBar => DisplayStyle::ProgressBar,
            DisplayStyleOpt::Line => DisplayStyle::Line,
            DisplayStyleOpt::Group => DisplayStyle::Group,
            DisplayStyleOpt::Auto => {
                if std::io::stdout().is_terminal() {
                    DisplayStyle::ProgressBar
                } else {
                    DisplayStyle::Line
                }
            }
        };

        let context = ExampleContext::new(&logctx.log);
        let (display_handle, sender) =
            make_displayer(&logctx.log, display_style, self.prefix);

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

        // Create temporary directories for component 1.
        let temp_dirs_handle_1 =
            context.register_create_temp_dirs_step(&component_1, 2);

        // Write component 1 out to disk.
        context.register_write_step(
            &component_1,
            download_handle_1,
            temp_dirs_handle_1,
            None,
        );

        // Download component 2.
        let component_2 = engine.for_component(ExampleComponent::Component2);
        let download_handle_2 = context.register_download_step(
            &component_2,
            "https://www.example.com".to_owned(),
            1_048_576 * 8,
        );

        // Create temporary directories for component 2.
        let temp_dirs_handle_2 =
            context.register_create_temp_dirs_step(&component_2, 3);

        // Now write component 2 out to disk.
        context.register_write_step(
            &component_2,
            download_handle_2,
            temp_dirs_handle_2,
            Some(1),
        );

        _ = engine.execute().await;

        // Wait until all messages have been received by the displayer.
        _ = display_handle.await;

        // Do not clean up the log file so people can inspect it.

        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Default, ValueEnum)]
enum DisplayStyleOpt {
    ProgressBar,
    Line,
    Group,
    #[default]
    Auto,
}

#[derive(Copy, Clone, Debug)]
enum DisplayStyle {
    ProgressBar,
    Line,
    Group,
}

/// Context shared across steps. This forms the lifetime "'a" defined by the
/// UpdateEngine.
struct ExampleContext {
    log: slog::Logger,
}

impl ExampleContext {
    fn new(log: &slog::Logger) -> Self {
        Self { log: log.new(slog::o!("component" => "ExampleContext")) }
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
                async move |cx| {
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
                        ProgressUnits::BYTES,
                        serde_json::Value::Null,
                    ))
                    .await;

                    tokio::time::sleep(Duration::from_millis(100)).await;
                    cx.send_progress(StepProgress::with_current_and_total(
                        num_bytes / 4,
                        num_bytes,
                        ProgressUnits::BYTES,
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

                    // Try a second time, and this time go to 80%.
                    let mut buf_list = BufList::new();
                    for i in 0..8 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        cx.send_progress(StepProgress::with_current_and_total(
                            num_bytes * i / 10,
                            num_bytes,
                            ProgressUnits::BYTES,
                            serde_json::Value::Null,
                        ))
                        .await;
                        buf_list.push_chunk(&b"downloaded-data"[..]);
                    }

                    // Now indicate a progress reset.
                    cx.send_progress(StepProgress::reset(
                        serde_json::Value::Null,
                        "Progress reset",
                    ))
                    .await;

                    // Try again, and this time succeed.
                    let mut buf_list = BufList::new();
                    for i in 0..=10 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        cx.send_progress(StepProgress::with_current_and_total(
                            num_bytes * i / 10,
                            num_bytes,
                            ProgressUnits::BYTES,
                            serde_json::Value::Null,
                        ))
                        .await;
                        buf_list.push_chunk(&b"downloaded-data"[..]);
                    }

                    StepSuccess::new(buf_list)
                        .with_metadata(ExampleCompletionMetadata::Download {
                            num_bytes,
                        })
                        .into()
                },
            )
            .register()
    }

    fn register_create_temp_dirs_step<'a>(
        &'a self,
        registrar: &ComponentRegistrar<'_, 'a>,
        total_count: usize,
    ) -> StepHandle<Vec<Utf8TempDir>> {
        registrar
            .new_step(
                ExampleStepId::CreateTempDir,
                format!("Creating {total_count} temporary directories"),
                async move |cx| {
                    // Simulate a creation of a number of temporary directories.
                    let mut dirs = Vec::with_capacity(total_count);
                    let mut paths = Vec::with_capacity(total_count);
                    for current in 0..total_count as u64 {
                        tokio::time::sleep(Duration::from_millis(200)).await;

                        let temp_dir = Utf8TempDir::new()
                            .context("failed to create temp dir")?;
                        paths.push(temp_dir.path().to_owned());
                        dirs.push(temp_dir);
                        cx.send_progress(StepProgress::with_current_and_total(
                            current,
                            total_count as u64,
                            ProgressUnits::BYTES,
                            Default::default(),
                        ))
                        .await;
                    }

                    StepSuccess::new(dirs)
                        .with_metadata(
                            ExampleCompletionMetadata::CreateTempDir { paths },
                        )
                        .into()
                },
            )
            .register()
    }

    fn register_write_step<'a>(
        &'a self,
        registrar: &ComponentRegistrar<'_, 'a>,
        download_handle: StepHandle<BufList>,
        temp_dirs_handle: StepHandle<Vec<Utf8TempDir>>,
        // The index at which this should fail, if any.
        error_index: Option<usize>,
    ) {
        let component = *registrar.component();

        // A `StepHandle`'s value can ordinarily only be used by one function.
        // In this example we're going to share the output across multiple steps
        // using into_shared.
        let download_handle = download_handle.into_shared();
        let download_handle_2 = download_handle.clone();

        registrar
            .new_step(
                ExampleStepId::Write,
                "Writing artifact to temporary directories",
                async move |cx| {
                    let buf_list = download_handle.into_value(cx.token()).await;
                    let temp_dirs =
                        temp_dirs_handle.into_value(cx.token()).await;

                    let destinations: Vec<_> = temp_dirs
                        .iter()
                        .map(|dir| {
                            dir.path().join(format!("write_{component:?}.out"))
                        })
                        .collect();

                    cx.with_nested_engine(|engine| {
                        register_nested_write_steps(
                            &self.log,
                            engine,
                            component,
                            &destinations,
                            buf_list,
                            error_index,
                            &cx,
                        );
                        Ok(())
                    })
                    .await?;

                    // Demonstrate how to show a warning.
                    StepWarning::new((), "Example warning").into()
                },
            )
            .with_metadata_fn(async move |cx| {
                let buf_list = download_handle_2.into_value(cx.token()).await;
                ExampleStepMetadata::Write {
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
            .new_step(
                ExampleStepId::Skipped,
                "This step does nothing",
                async |_cx| StepSkipped::new((), "Step skipped").into(),
            )
            .register();
    }
}

fn register_nested_write_steps<'a>(
    log: &'a slog::Logger,
    engine: &mut UpdateEngine<'a, ExampleWriteSpec>,
    component: ExampleComponent,
    destinations: &'a [Utf8PathBuf],
    buf_list: BufList,
    error_index: Option<usize>,
    parent_cx: &'a StepContext<ExampleSpec>,
) {
    for (index, destination) in destinations.into_iter().enumerate() {
        let mut buf_list = buf_list.clone();
        engine
            .new_step(
                component,
                ExampleWriteStepId::Write {
                    destination: destination.to_owned(),
                },
                format!("Writing to {destination}"),
                async move |cx| {
                    parent_cx
                        .send_progress(StepProgress::with_current_and_total(
                            index as u64,
                            destinations.len() as u64,
                            "destinations",
                            Default::default(),
                        ))
                        .await;

                    let mut remote_engine_receiver = create_remote_engine(
                        log,
                        component,
                        buf_list.clone(),
                        destination.clone(),
                    );
                    let mut buffer = EventBuffer::default();
                    let mut last_seen = None;
                    while let Some(event) = remote_engine_receiver.recv().await
                    {
                        // Only send progress up to 50% to demonstrate
                        // not receiving full progress.
                        if let Event::Progress(event) = &event {
                            if let Some(counter) = event.kind.progress_counter()
                            {
                                if let Some(total) = counter.total {
                                    if counter.current > total / 2 {
                                        break;
                                    }
                                }
                            }
                        }

                        buffer.add_event(event);
                        let report =
                            buffer.generate_report_since(&mut last_seen);
                        cx.send_nested_report(report)
                            .await
                            .expect("this engine should never fail");
                    }

                    let mut file =
                        tokio::fs::File::create(destination)
                            .await
                            .context("failed to open file for writing")?;

                    let num_bytes = buf_list.num_bytes() as u64;
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
                            ProgressUnits::BYTES,
                            (),
                        ))
                        .await;

                        if (error_index == Some(index))
                            && (buf_list.remaining() as u64) < num_bytes / 2
                        {
                            // Error out as a demonstration.
                            bail!("error!");
                        }
                    }

                    StepSuccess::new(()).into()
                },
            )
            .register();
    }
}

/// Sets up a remote engine that can be used to execute steps.
fn create_remote_engine(
    log: &slog::Logger,
    component: ExampleComponent,
    mut buf_list: BufList,
    destination: Utf8PathBuf,
) -> mpsc::Receiver<Event<ExampleWriteSpec>> {
    let (sender, receiver) = tokio::sync::mpsc::channel(128);
    let engine = UpdateEngine::new(log, sender);
    engine
        .for_component(component)
        .new_step(
            ExampleWriteStepId::Write { destination: destination.clone() },
            format!("Writing to {destination} (remote, fake)"),
            async move |cx| {
                let num_bytes = buf_list.num_bytes();
                let mut total_written = 0;

                while buf_list.has_remaining() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    // Don't actually write these bytes -- this engine is just
                    // for demoing.
                    let written_bytes =
                        (num_bytes / 10).min(buf_list.num_bytes());
                    total_written += written_bytes;
                    buf_list.advance(written_bytes);
                    cx.send_progress(StepProgress::with_current_and_total(
                        total_written as u64,
                        num_bytes as u64,
                        ProgressUnits::new_const("fake bytes"),
                        (),
                    ))
                    .await;
                }

                StepSuccess::new(()).into()
            },
        )
        .register();

    tokio::spawn(async move {
        engine.execute().await.expect("remote engine succeeded")
    });

    receiver
}
