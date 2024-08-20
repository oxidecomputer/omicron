// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::{borrow::Cow, time::Duration};

use anyhow::{anyhow, bail, Context, Result};
use debug_ignore::DebugIgnore;
use indexmap::{map::Entry, IndexMap};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use tokio::{sync::mpsc, task::JoinHandle};
use update_engine::{
    display::{GroupDisplay, LineDisplay, LineDisplayStyles},
    events::ProgressCounter,
};

use crate::{
    spec::{
        Event, EventBuffer, ExampleComponent, ExampleStepId,
        ExampleStepMetadata, ProgressEvent, ProgressEventKind, StepEventKind,
        StepInfoWithMetadata, StepOutcome,
    },
    DisplayStyle,
};

/// An example that displays an event stream on the command line.
pub(crate) fn make_displayer(
    log: &slog::Logger,
    display_style: DisplayStyle,
    prefix: Option<String>,
) -> (JoinHandle<Result<()>>, mpsc::Sender<Event>) {
    let (sender, receiver) = mpsc::channel(512);
    let log = log.clone();
    let join_handle =
        match display_style {
            DisplayStyle::ProgressBar => tokio::task::spawn(async move {
                display_progress_bar(&log, receiver).await
            }),
            DisplayStyle::Line => tokio::task::spawn(async move {
                display_line(&log, receiver, prefix).await
            }),
            DisplayStyle::Group => tokio::task::spawn(async move {
                display_group(&log, receiver).await
            }),
        };

    (join_handle, sender)
}

async fn display_line(
    log: &slog::Logger,
    mut receiver: mpsc::Receiver<Event>,
    prefix: Option<String>,
) -> Result<()> {
    slog::info!(log, "setting up display");
    let mut buffer = EventBuffer::new(8);
    let mut display = LineDisplay::new(std::io::stdout());
    // For now, always colorize. TODO: figure out whether colorization should be
    // done based on always/auto/never etc.
    if supports_color::on(supports_color::Stream::Stdout).is_some() {
        display.set_styles(LineDisplayStyles::colorized());
    }
    if let Some(prefix) = prefix {
        display.set_prefix(prefix);
    }
    display.set_progress_interval(Duration::from_millis(50));
    while let Some(event) = receiver.recv().await {
        buffer.add_event(event);
        display.write_event_buffer(&buffer)?;
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
enum GroupDisplayKey {
    Example,
    Other,
}

async fn display_group(
    log: &slog::Logger,
    mut receiver: mpsc::Receiver<Event>,
) -> Result<()> {
    slog::info!(log, "setting up display");

    let mut display = GroupDisplay::new(
        log,
        [
            (GroupDisplayKey::Example, "example"),
            (GroupDisplayKey::Other, "other"),
        ],
        std::io::stdout(),
    );
    // For now, always colorize. TODO: figure out whether colorization should be
    // done based on always/auto/never etc.
    if supports_color::on(supports_color::Stream::Stdout).is_some() {
        display.set_styles(LineDisplayStyles::colorized());
    }

    display.set_progress_interval(Duration::from_millis(50));

    let mut example_buffer = EventBuffer::default();
    let mut example_buffer_last_seen = None;
    let mut other_buffer = EventBuffer::default();
    let mut other_buffer_last_seen = None;

    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Print out status lines every 2 seconds.
                display.write_stats("Status")?;
            }
            event = receiver.recv() => {
                let Some(event) = event else { break };
                example_buffer.add_event(event.clone());
                other_buffer.add_event(event);

                display.add_event_report(
                    &GroupDisplayKey::Example,
                    example_buffer.generate_report_since(&mut example_buffer_last_seen),
                )?;
                display.add_event_report(
                    &GroupDisplayKey::Other,
                    other_buffer.generate_report_since(&mut other_buffer_last_seen),
                )?;
                display.write_events()?;
            }
        }
    }

    // Print status at the end.
    display.write_stats("Summary")?;

    Ok(())
}

async fn display_progress_bar(
    log: &slog::Logger,
    mut receiver: mpsc::Receiver<Event>,
) -> Result<()> {
    let first_event =
        receiver.recv().await.context("at least one event is expected")?;
    let mut state = MessageDisplayState::new(log, first_event).await?;

    while let Some(event) = receiver.recv().await {
        state.handle_event(event)?;
    }

    Ok(())
}

pub struct MessageDisplayState {
    log: slog::Logger,
    mp: MultiProgress,
    pb_main: ProgressBar,
    sty_aux: ProgressStyle,
    component_tree: IndexMap<ExampleComponent, ComponentNode>,
}

impl MessageDisplayState {
    pub(crate) async fn new(
        log: &slog::Logger,
        first_event: Event,
    ) -> Result<Self> {
        let log = log.new(slog::o!("component" => "MessageDisplayState"));
        let mp = MultiProgress::new();

        let sty_main = ProgressStyle::with_template(
            "{prefix}[{pos:>2}/{len:2}] {spinner:.green} {msg}",
        )
        .unwrap();

        let sty_aux = ProgressStyle::with_template(
            "{prefix}{msg}\n{prefix}  [{bar:30.green/yellow}] [{pos}/{len}]",
        )
        .unwrap()
        .progress_chars("=> ");

        let Event::Step(step_event) = first_event else {
            bail!("received invalid event: {first_event:?}");
        };
        let progress_event = step_event.progress_event().expect(
            "first event should always have a progress associated with it",
        );
        let StepEventKind::ExecutionStarted {
            steps,
            components,
            first_step: _,
        } = step_event.kind
        else {
            bail!("received invalid step event kind: {step_event:?}");
        };

        let pb_main = mp.add(ProgressBar::new(steps.len() as u64));
        pb_main.set_message("Running...");
        pb_main.set_style(sty_main.clone());

        // Create the tree of components.
        let component_tree = components
            .into_iter()
            .map(|summary| {
                let pb = ProgressBar::hidden();
                pb.set_style(sty_main.clone());
                pb.set_message(summary.component.to_string());
                pb.set_prefix("  ");
                pb.set_length(summary.total_component_steps as u64);
                let node = ComponentNode::new(pb);
                (summary.component, node)
            })
            .collect();

        let mut ret =
            MessageDisplayState { log, mp, pb_main, sty_aux, component_tree };
        ret.handle_progress_event(progress_event)?;

        Ok(ret)
    }

    fn handle_event(&mut self, event: Event) -> Result<()> {
        match event {
            Event::Step(event) => {
                let progress_event = event.progress_event();
                match event.kind {
                    StepEventKind::NoStepsDefined => {
                        bail!("at least one step expected")
                    }
                    StepEventKind::ExecutionStarted { .. } => {
                        bail!("already past the first step")
                    }
                    StepEventKind::ProgressReset { step, .. } => {
                        let node = self.handle_and_get_node(step)?;
                        node.reset();
                    }
                    StepEventKind::AttemptRetry {
                        step,
                        next_attempt,
                        attempt_elapsed,
                        message,
                        ..
                    } => {
                        let node = self.handle_and_get_node(step)?;
                        node.retry(next_attempt, attempt_elapsed, message);
                    }
                    StepEventKind::StepCompleted {
                        step,
                        attempt,
                        outcome,
                        attempt_elapsed,
                        ..
                    } => {
                        let node = self.handle_and_get_node(step)?;
                        node.finish(attempt, outcome, attempt_elapsed);
                        self.pb_main.inc(1);
                    }
                    StepEventKind::ExecutionCompleted {
                        last_step,
                        last_attempt,
                        last_outcome,
                        step_elapsed,
                        attempt_elapsed: _,
                    } => {
                        let last_node = self.handle_and_get_node(last_step)?;
                        last_node.finish(
                            last_attempt,
                            last_outcome,
                            step_elapsed,
                        );
                    }
                    StepEventKind::ExecutionFailed {
                        failed_step,
                        total_attempts,
                        attempt_elapsed,
                        message,
                        ..
                    } => {
                        let failed_node =
                            self.handle_and_get_node(failed_step)?;
                        failed_node.abandon(
                            &message,
                            total_attempts,
                            attempt_elapsed,
                        );
                    }
                    StepEventKind::ExecutionAborted {
                        aborted_step,
                        attempt,
                        attempt_elapsed,
                        message,
                        ..
                    } => {
                        let aborted_node =
                            self.handle_and_get_node(aborted_step)?;
                        aborted_node.abandon(
                            &message,
                            attempt,
                            attempt_elapsed,
                        );
                    }
                    StepEventKind::Nested { .. } => {
                        // TODO: display nested events
                    }
                    StepEventKind::Unknown => {}
                }

                if let Some(progress_event) = progress_event {
                    self.handle_progress_event(progress_event)?;
                }
            }
            Event::Progress(event) => self.handle_progress_event(event)?,
        }

        self.pb_main.tick();

        Ok(())
    }

    fn handle_progress_event(&mut self, event: ProgressEvent) -> Result<()> {
        match event.kind {
            ProgressEventKind::WaitingForProgress { step, .. } => {
                // Create this node.
                self.handle_and_get_node(step)?;
            }
            ProgressEventKind::Progress { step, progress, .. } => {
                let node = self.handle_and_get_node(step)?;
                node.progress(progress);
            }
            ProgressEventKind::Nested { .. } => {
                // TODO: display nested events
            }
            ProgressEventKind::Unknown => {}
        }

        self.pb_main.tick();

        Ok(())
    }

    fn handle_and_get_node(
        &mut self,
        step: StepInfoWithMetadata,
    ) -> Result<&mut ItemNode> {
        let component_node =
            self.component_tree.get_mut(&step.info.component).ok_or_else(
                || anyhow!("component {:?} not found", step.info.component),
            )?;

        // We need to add this component to the multiprogress if it hasn't been added
        // yet.
        if !component_node.pb_added {
            slog::info!(
                &self.log,
                "adding new progress bar for {:?}",
                step.info.component
            );
            component_node.pb.set_prefix("  ");
            self.mp.add(component_node.pb.clone());
            component_node.pb_added = true;
        }

        component_node.pb.tick();

        let after = component_node
            .items
            .last()
            .map(|(_, node)| node.pb.clone())
            .unwrap_or_else(|| component_node.pb.clone());

        let item_node = match component_node.items.entry(step.info.id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                // Add the progress bar after the last one for this component.
                entry.insert(ItemNode::new(
                    step,
                    &self.mp,
                    component_node.pb.clone(),
                    self.sty_aux.clone(),
                    &after,
                ))
            }
        };

        Ok(item_node)
    }
}

#[derive(Debug)]
struct ComponentNode {
    pb: ProgressBar,
    pb_added: bool,
    items: IndexMap<ExampleStepId, ItemNode>,
}

impl ComponentNode {
    fn new(pb: ProgressBar) -> Self {
        Self { pb, pb_added: false, items: IndexMap::new() }
    }
}

#[derive(Debug)]
struct ItemNode {
    info: StepInfoWithMetadata,
    mp: MultiProgress,
    component_pb: ProgressBar,
    pb: ProgressBar,
    style: DebugIgnore<ProgressStyle>,
}

impl ItemNode {
    fn new(
        info: StepInfoWithMetadata,
        mp: &MultiProgress,
        component_pb: ProgressBar,
        style: ProgressStyle,
        insert_after: &ProgressBar,
    ) -> Self {
        let pb = Self::new_item_pb(
            &style,
            1,
            &info.info.description,
            info.metadata.as_ref(),
        );
        let pb = mp.insert_after(insert_after, pb);
        Self { info, mp: mp.clone(), component_pb, pb, style: style.into() }
    }

    fn progress(&self, progress: Option<ProgressCounter>) {
        if let Some(progress) = progress {
            self.pb.set_position(progress.current);
            if let Some(total) = progress.total {
                self.pb.set_length(total);
            }
        }
    }

    fn reset(&self) {
        // Just reset all state.
        self.pb.reset();
    }

    fn retry(
        &mut self,
        next_attempt: usize,
        attempt_elapsed: Duration,
        message: Cow<'static, str>,
    ) {
        // Abandon the current progress bar, and create a new one.
        self.pb.abandon_with_message(format!(
            "{} {}{} ({attempt_elapsed:?}): {}",
            "✖".yellow(),
            format!("(attempt {}) ", next_attempt - 1).bold(),
            self.info.info.description,
            message.red(),
        ));
        let new_pb = Self::new_item_pb(
            &self.style,
            next_attempt,
            &self.info.info.description,
            self.info.metadata.as_ref(),
        );
        let new_pb = self.mp.insert_after(&self.pb, new_pb);
        self.pb = new_pb;
    }

    fn finish(
        &self,
        last_attempt: usize,
        outcome: StepOutcome,
        attempt_elapsed: Duration,
    ) {
        match outcome {
            StepOutcome::Success { .. } => {
                self.pb.finish_with_message(format!(
                    "{} {}{} ({attempt_elapsed:?})",
                    "✔".green(),
                    Self::attempt_str(last_attempt).bold(),
                    self.info.info.description,
                ));
            }
            StepOutcome::Warning { message, .. } => {
                self.pb.finish_with_message(format!(
                    "{} {}{} ({attempt_elapsed:?}): {}",
                    "✔".yellow(),
                    Self::attempt_str(last_attempt).bold(),
                    self.info.info.description,
                    message.yellow(),
                ));
            }
            StepOutcome::Skipped { message, .. } => {
                // Hide the progress bar for skipped steps: just show the
                // message.
                self.pb.set_style(
                    ProgressStyle::with_template("{prefix}{msg}").unwrap(),
                );
                self.pb.finish_with_message(format!(
                    "* {}{}: skipped: {}",
                    Self::attempt_str(last_attempt).bold(),
                    self.info.info.description,
                    message.yellow()
                ));
            }
        }

        self.component_pb.inc(1);
        // Is this the last step in this component?
        if self.info.info.is_last_step_in_component() {
            self.component_pb.finish();
        }
    }

    fn abandon(
        &self,
        message: &str,
        total_attempts: usize,
        attempt_elapsed: Duration,
    ) {
        self.pb.abandon_with_message(format!(
            "{} {}{} ({attempt_elapsed:?}): {}",
            "✖".red(),
            Self::attempt_str(total_attempts).bold(),
            self.info.info.description,
            message.red(),
        ));
        self.component_pb.abandon();
    }

    fn attempt_str(attempt: usize) -> String {
        if attempt > 1 {
            format!("(attempt {attempt}) ")
        } else {
            String::new()
        }
    }

    fn new_item_pb(
        style: &ProgressStyle,
        attempt: usize,
        description: &str,
        metadata: Option<&ExampleStepMetadata>,
    ) -> ProgressBar {
        let metadata_message = match metadata {
            Some(ExampleStepMetadata::Write { num_bytes }) => {
                format!(" ({num_bytes} bytes)")
            }
            None => String::new(),
        };
        let pb = ProgressBar::hidden();
        pb.set_style(style.clone());
        pb.set_message(format!(
            "* {}{description}{metadata_message}",
            Self::attempt_str(attempt).bold(),
        ));
        pb.set_prefix("    ");
        pb
    }
}
