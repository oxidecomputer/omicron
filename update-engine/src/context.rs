// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use std::fmt;
use std::marker::PhantomData;

use derive_where::derive_where;
use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};

use crate::{
    errors::ExecutionError,
    events::{Event, StepProgress},
    NestedSpec, StepSpec, UpdateEngine,
};

/// Context for a step's execution function.
///
/// This is passed into the function registered for a step, and can be used to
/// send progress updates as the function continues execution.
///
/// # Notes
///
/// `StepContext` deliberately does not implement `Clone`, to make it more
/// likely that it is dropped at the same time the future completes.
#[derive(Debug)]
pub struct StepContext<S: StepSpec> {
    log: slog::Logger,
    payload_sender: mpsc::Sender<StepContextPayload<S>>,
    token: StepHandleToken<S>,
}

impl<S: StepSpec> StepContext<S> {
    pub(crate) fn new(
        log: &slog::Logger,
        payload_sender: mpsc::Sender<StepContextPayload<S>>,
    ) -> Self {
        Self { log: log.clone(), payload_sender, token: StepHandleToken::new() }
    }

    /// Sends a progress update to the update engine.
    #[inline]
    pub async fn send_progress(&self, progress: StepProgress<S>) {
        self.payload_sender
            .send(StepContextPayload::Progress(progress))
            .await
            .expect("our code always keeps the receiver open")
    }

    /// Creates a nested execution engine.
    ///
    /// An individual step can generate other steps: these steps are treated as
    /// *nested*, and carry their own progress.
    pub async fn with_nested_engine<'a, 'this, F, S2>(
        &'this self,
        engine_fn: F,
    ) -> Result<CompletionContext<S2>, S2::Error>
    where
        'this: 'a,
        F: FnOnce(&mut UpdateEngine<'a, S2>) -> Result<(), S2::Error> + Send,
        S2: StepSpec,
    {
        let (sender, mut receiver) = mpsc::channel(128);
        let mut engine = UpdateEngine::new(&self.log, sender);
        // Create the engine's steps.
        (engine_fn)(&mut engine)?;

        // Now run the engine.
        let engine = engine.execute();
        tokio::pin!(engine);

        let mut result = None;
        let mut events_done = false;

        loop {
            tokio::select! {
                ret = &mut engine, if result.is_none() => {
                    match ret {
                        Ok(cx) => {
                            result = Some(Ok(cx));
                        }
                        Err(ExecutionError::EventSendError(_)) => {
                            unreachable!("we always keep the receiver open")
                        }
                        Err(ExecutionError::StepFailed { error, .. }) => {
                            result = Some(Err(error));
                        }
                    }
                }
                event = receiver.recv(), if !events_done => {
                    match event {
                        Some(event) => {
                            match event.into_generic() {
                                Ok(event) => {
                                    self.payload_sender.send(
                                        StepContextPayload::Nested(event)
                                    )
                                    .await
                                    .expect("we always keep the receiver open");
                                }
                                Err(error) => {
                                    // All we can really do is log this as a warning.
                                    // This
                                    slog::warn!(self.log, "error serializing nested event: {error}");
                                }
                            }
                        }
                        None => {
                            events_done = true;
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }

        result.expect("the loop only exits if result is set")
    }

    /// Retrieves a token used to fetch the value out of a [`StepHandle`].
    pub fn token(&self) -> &StepHandleToken<S> {
        &self.token
    }
}

#[derive_where(Debug)]
pub(crate) enum StepContextPayload<S: StepSpec> {
    Progress(StepProgress<S>),
    Nested(Event<NestedSpec>),
}

/// Context for a step's metadata-generation function.
///
/// This is passed into the function registered to generate a step's metadata.
///
/// # Notes
///
/// `MetadataContext` deliberately does not implement `Clone`, to make it more
/// likely that it is dropped at the same time the future completes.
#[derive_where(Debug)]
pub struct MetadataContext<S: StepSpec> {
    token: StepHandleToken<S>,
}

impl<S: StepSpec> MetadataContext<S> {
    pub(crate) fn new() -> Self {
        Self { token: StepHandleToken::new() }
    }

    /// Retrieves a token used to fetch the value out of a [`StepHandle`].
    pub fn token(&self) -> &StepHandleToken<S> {
        &self.token
    }
}

/// Context returned by a successful
/// [`UpdateEngine::execute`](crate::UpdateEngine::execute).
///
/// This can be used to retrieve the value of a `StepHandle`. In the future, it
/// may also be extended to provide more information
#[derive_where(Debug)]
pub struct CompletionContext<S: StepSpec> {
    token: StepHandleToken<S>,
}

impl<S: StepSpec> CompletionContext<S> {
    pub(crate) fn new() -> Self {
        Self { token: StepHandleToken::new() }
    }

    /// Retrieves a token used to fetch the value out of a [`StepHandle`].
    pub fn token(&self) -> &StepHandleToken<S> {
        &self.token
    }
}

/// A token to retrieve the value within a [`StepHandle`].
///
/// For more information, see the documentation for [`StepHandle`].
#[derive_where(Debug)]
pub struct StepHandleToken<S>(PhantomData<S>);

impl<S> StepHandleToken<S> {
    fn new() -> Self {
        Self(PhantomData)
    }
}

/// A way to obtain a step's result.
///
/// This handle can be used to transfer data between steps.
///
/// `StepHandle`s must be awaited:
///
/// 1. Either within a future step,
/// 2. Or after the engine completes executing.
///
/// To ensure this, the only way to retrieve a `StepHandle` is with a
/// `StepHandleToken`, returned by:
///
/// * [`StepContext::token`]
/// * [`MetadataContext::token`]
/// * [`CompletionContext::token`]
///
/// It is important that `StepHandle`s never be awaited outside the context of a
/// step that comes afterwards -- doing so will cause an immediate deadlock.
///
/// Dropping a `StepHandle` has no effect on whether the step itself runs.
#[derive_where(Debug; T: fmt::Debug)]
pub struct StepHandle<T, S> {
    receiver: oneshot::Receiver<T>,
    _marker: PhantomData<S>,
}

impl<T, S> StepHandle<T, S> {
    pub(crate) fn new(receiver: oneshot::Receiver<T>) -> Self {
        Self { receiver, _marker: PhantomData }
    }

    /// Resolves to the output from a step, `T`.
    pub async fn into_value(self, _token: &StepHandleToken<S>) -> T {
        self.receiver.await.expect("update-engine always sends a value")
    }

    /// Returns a shared handle.
    ///
    /// A `SharedStepHandle` can be used to retrieve the output of a future
    /// across multiple dependent steps.
    pub fn into_shared(self) -> SharedStepHandle<T, S>
    where
        T: Clone,
    {
        SharedStepHandle {
            receiver: self.receiver.shared(),
            _marker: self._marker,
        }
    }
}

/// A shared version of [`StepHandle`].
///
/// A `SharedStepHandle` is cloneable, and can be used to retrieve the output of
/// a step across multiple dependent steps.
#[derive_where(Debug; T: fmt::Debug)]
#[derive_where(Clone; T: Clone)]
pub struct SharedStepHandle<T, S> {
    receiver: futures::future::Shared<oneshot::Receiver<T>>,
    _marker: PhantomData<S>,
}

impl<T: Clone, S> SharedStepHandle<T, S> {
    /// Resolves to the output from a step, `T`.
    pub async fn into_value(self, _token: &StepHandleToken<S>) -> T {
        self.receiver.await.expect("update-engine always sends a value")
    }
}
