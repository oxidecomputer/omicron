// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use anyhow::bail;
use futures::StreamExt;
use schemars::JsonSchema;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    events::{Event, ProgressUnits, StepProgress},
    EventBuffer, ExecutionId, StepContext, StepSpec, StepSuccess, UpdateEngine,
};

#[derive(JsonSchema)]
pub(crate) enum TestSpec {}

impl StepSpec for TestSpec {
    type Component = String;
    type StepId = usize;
    type StepMetadata = serde_json::Value;
    type ProgressMetadata = serde_json::Value;
    type CompletionMetadata = serde_json::Value;
    type SkippedMetadata = serde_json::Value;
    type Error = anyhow::Error;
}

pub(crate) static TEST_EXECUTION_UUID: &str =
    "2cc08a14-5e96-4917-bc70-e98293a3b703";

pub fn test_execution_id() -> ExecutionId {
    ExecutionId(TEST_EXECUTION_UUID.parse().expect("valid UUID"))
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum GenerateTestEventsKind {
    Completed,
    Failed,
    Aborted,
}

pub(crate) async fn generate_test_events(
    log: &slog::Logger,
    kind: GenerateTestEventsKind,
) -> Vec<Event<TestSpec>> {
    let (sender, receiver) = crate::channel();
    let engine = UpdateEngine::new(log, sender);

    match kind {
        GenerateTestEventsKind::Completed => {
            define_test_steps(log, &engine, LastStepOutcome::Completed);
            engine.execute().await.expect("execution successful");
        }
        GenerateTestEventsKind::Failed => {
            define_test_steps(log, &engine, LastStepOutcome::Failed);
            engine.execute().await.expect_err("execution failed");
        }
        GenerateTestEventsKind::Aborted => {
            // In this case, the last step signals that it has been reached via
            // sending a message over this channel, and then waits forever. We
            // abort execution by calling into the AbortHandle.
            let (sender, receiver) = oneshot::channel();
            define_test_steps(log, &engine, LastStepOutcome::Aborted(sender));
            let abort_handle = engine.abort_handle();
            let mut execute_fut = std::pin::pin!(engine.execute());
            let mut receiver = std::pin::pin!(receiver);
            let mut receiver_done = false;
            loop {
                tokio::select! {
                    res = &mut execute_fut => {
                        res.expect_err("execution should have been aborted, but completed successfully");
                        break;
                    }
                    _ = &mut receiver, if !receiver_done => {
                        receiver_done = true;
                        abort_handle
                            .abort("test engine deliberately aborted")
                            .expect("engine should still be alive");
                    }
                }
            }
        }
    }

    ReceiverStream::new(receiver).collect().await
}

#[derive(Debug)]
enum LastStepOutcome {
    Completed,
    Failed,
    Aborted(oneshot::Sender<()>),
}

#[derive(Debug)]
enum Never {}

fn define_test_steps(
    log: &slog::Logger,
    engine: &UpdateEngine<TestSpec>,
    last_step_outcome: LastStepOutcome,
) {
    engine
        .new_step("foo".to_owned(), 1, "Step 1", move |_cx| async move {
            StepSuccess::new(()).into()
        })
        .register();

    engine
        .new_step("bar".to_owned(), 2, "Step 2", move |cx| async move {
            for _ in 0..20 {
                cx.send_progress(StepProgress::with_current_and_total(
                    5,
                    20,
                    ProgressUnits::BYTES,
                    Default::default(),
                ))
                .await;

                cx.send_progress(StepProgress::reset(
                    Default::default(),
                    "reset step 2",
                ))
                .await;

                cx.send_progress(StepProgress::retry("retry step 2")).await;
            }
            StepSuccess::new(()).into()
        })
        .register();

    engine
        .new_step(
            "nested".to_owned(),
            3,
            "Step 3 (this is nested)",
            move |parent_cx| async move {
                parent_cx
                    .with_nested_engine(|engine| {
                        define_nested_engine(&parent_cx, engine, 3, "steps");
                        Ok(())
                    })
                    .await
                    .expect_err("this is expected to fail");

                // Define a second nested engine -- this verifies that internal
                // buffer indexes match up.
                parent_cx
                    .with_nested_engine(|engine| {
                        define_nested_engine(
                            &parent_cx,
                            engine,
                            10,
                            // The tests in buffer.rs expect the units to be
                            // "steps" exactly once, so use a different name here.
                            "steps (again)",
                        );
                        Ok(())
                    })
                    .await
                    .expect_err("this is expected to fail");

                StepSuccess::new(()).into()
            },
        )
        .register();

    let log = log.clone();
    engine
    .new_step(
        "remote-nested".to_owned(),
        20,
        "Step 4 (remote nested)",
        move |cx| async move {
            let (sender, mut receiver) = crate::channel();
            let mut engine = UpdateEngine::new(&log, sender);
            define_remote_nested_engine(&mut engine, 20);

            let mut buffer = EventBuffer::default();

            let mut execute_fut = std::pin::pin!(engine.execute());
            let mut execute_done = false;
            loop {
                tokio::select! {
                    res = &mut execute_fut, if !execute_done => {
                        res.expect("remote nested engine completed successfully");
                        execute_done = true;
                    }
                    Some(event) = receiver.recv() => {
                        // Generate complete reports to ensure deduping
                        // happens within StepContexts.
                        buffer.add_event(event);
                        cx.send_nested_report(buffer.generate_report()).await?;
                    }
                    else => {
                        break;
                    }
                }
            }

            StepSuccess::new(()).into()
        },
    )
    .register();

    // The step index here (100) is large enough to be higher than all nested
    // steps.
    engine
        .new_step("baz".to_owned(), 100, "Step 5", move |_cx| async move {
            match last_step_outcome {
                LastStepOutcome::Completed => StepSuccess::new(()).into(),
                LastStepOutcome::Failed => {
                    bail!("last step failed")
                }
                LastStepOutcome::Aborted(sender) => {
                    sender.send(()).expect("receiver should be alive");
                    // The driver of the engine is responsible for aborting it
                    // at this point.
                    std::future::pending::<Never>().await;
                    unreachable!("pending future can never resolve");
                }
            }
        })
        .register();
}

fn define_nested_engine<'a>(
    parent_cx: &'a StepContext<TestSpec>,
    engine: &mut UpdateEngine<'a, TestSpec>,
    start_id: usize,
    step_units: &'static str,
) {
    engine
        .new_step(
            "nested-foo".to_owned(),
            start_id + 1,
            "Nested step 1",
            move |cx| async move {
                parent_cx
                    .send_progress(StepProgress::with_current_and_total(
                        1,
                        3,
                        step_units,
                        Default::default(),
                    ))
                    .await;
                cx.send_progress(StepProgress::progress(Default::default()))
                    .await;
                StepSuccess::new(()).into()
            },
        )
        .register();

    engine
        .new_step::<_, _, ()>(
            "nested-bar".to_owned(),
            start_id + 2,
            "Nested step 2 (fails)",
            move |cx| async move {
                // This is used by NestedProgressCheck below.
                parent_cx
                    .send_progress(StepProgress::with_current_and_total(
                        2,
                        3,
                        step_units,
                        Default::default(),
                    ))
                    .await;

                cx.send_progress(StepProgress::with_current(
                    50,
                    "units",
                    Default::default(),
                ))
                .await;

                parent_cx
                    .send_progress(StepProgress::with_current_and_total(
                        3,
                        3,
                        step_units,
                        Default::default(),
                    ))
                    .await;

                bail!("failing step")
            },
        )
        .register();
}

fn define_remote_nested_engine(
    engine: &mut UpdateEngine<'_, TestSpec>,
    start_id: usize,
) {
    engine
        .new_step(
            "nested-foo".to_owned(),
            start_id + 1,
            "Nested step 1",
            move |cx| async move {
                cx.send_progress(StepProgress::progress(Default::default()))
                    .await;
                StepSuccess::new(()).into()
            },
        )
        .register();

    engine
        .new_step::<_, _, ()>(
            "nested-bar".to_owned(),
            start_id + 2,
            "Nested step 2",
            move |cx| async move {
                cx.send_progress(StepProgress::with_current(
                    20,
                    "units",
                    Default::default(),
                ))
                .await;

                StepSuccess::new(()).into()
            },
        )
        .register();
}
