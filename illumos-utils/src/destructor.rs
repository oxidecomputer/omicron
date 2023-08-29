// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A queue to handle delayed destruction of objects

use anyhow::Error;
use async_trait::async_trait;
use futures::future::{FusedFuture, FutureExt, Shared};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use omicron_common::backoff::{
    retry_notify, retry_policy_internal_service, BackoffError,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

type SharedBoxFuture<T> = Shared<Pin<Box<dyn Future<Output = T> + Send>>>;

/// Future stored within [`Destructor<T>`].
struct ShutdownWaitFuture(SharedBoxFuture<Result<(), String>>);

impl Future for ShutdownWaitFuture {
    type Output = Result<(), String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

impl FusedFuture for ShutdownWaitFuture {
    fn is_terminated(&self) -> bool {
        self.0.is_terminated()
    }
}

enum Message<T> {
    // An object to the destructor to delete.
    Data(T),
    // A signal for the destructor to exit when no additional work remains.
    Exit,
}

struct DestructorWorker<T> {
    rx: mpsc::UnboundedReceiver<Message<T>>,
    futs: FuturesUnordered<ShutdownWaitFuture>,
}

impl<T: Deletable> DestructorWorker<T> {
    async fn run(&mut self) {
        let mut exit = false;
        loop {
            tokio::select! {
                Some(_) = self.futs.next() => {
                    if exit && self.futs.is_empty() {
                        return;
                    }
                }
                msg = self.rx.recv() => {
                    let msg = msg.unwrap_or(Message::Exit);
                    match msg {
                        Message::Data(object) => self.enqueue_destroy(object),
                        Message::Exit => {
                            exit = true;
                            if self.futs.is_empty() {
                                return;
                            }
                        },
                    }
                },
            }
        }
    }

    fn enqueue_destroy(&self, object: T) {
        self.futs.push(ShutdownWaitFuture(
            async move {
                let do_delete = || async {
                    object.delete().await.map_err(|err| {
                        BackoffError::transient(err.to_string())
                    })?;
                    Ok(())
                };
                let log_failure = |_err: String, _| {};
                retry_notify(
                    retry_policy_internal_service(),
                    do_delete,
                    log_failure,
                )
                .await?;
                Ok(())
            }
            .boxed()
            .shared(),
        ));
    }
}

struct Inner<T> {
    tx: mpsc::UnboundedSender<Message<T>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

/// A destructor which asynchronously destroys objects,
/// which can throw errors during destruction.
pub struct Destructor<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

// I wish this could be derived, but the derive(Clone) macro is silly,
// and has the impression that "T: Clone", which is not true.
impl<T> Clone for Destructor<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<T: Deletable> Destructor<T> {
    /// Creates a new destructor with a background task to consume destroyed
    /// objects.
    pub fn new() -> Self {
        let futs = FuturesUnordered::new();
        let (tx, rx) = mpsc::unbounded_channel::<Message<T>>();

        let handle = tokio::task::spawn(async move {
            let mut worker = DestructorWorker { rx, futs };
            worker.run().await;
        });
        Self { inner: Arc::new(Mutex::new(Inner { tx, handle: Some(handle) })) }
    }

    /// Destroys an object asynchronously in a background task.
    pub fn enqueue_destroy(&self, object: T) {
        self.inner
            .lock()
            .unwrap()
            .tx
            .send(Message::Data(object))
            .map_err(|err| err.to_string())
            // Unwrap safety:
            // - The DestructorWorker can only be stopped by "try_close"
            // - "try_close" can only succeed if it's called on the last strong reference to the
            // Destructor.
            // - Since "enqueue_destroy" was called on a "Destructor", one more reference must
            // exist.
            .unwrap();
    }

    /// Closes the destructor if this is the only reference to it.
    ///
    /// Consumes "self" to prevent subsequent objects from being enqueued
    /// to the Destructor.
    ///
    /// If there is more than one reference to "self", it is returned
    /// as the Result's error type.
    pub async fn try_close(self) -> Result<(), Self> {
        let handle = {
            let mut inner = self.inner.lock().unwrap();
            if inner.handle.is_none() {
                return Ok(());
            }
            if Arc::strong_count(&self.inner) != 1 {
                drop(inner);
                return Err(self);
            }
            // Unwrap safety: the handle must be "Some", so we only get here if the
            // DestructorWorker is still running.
            //
            // This relies on the assumption that "Exit" is the only way to stop the
            // worker.
            inner
                .tx
                .send(Message::Exit)
                .map_err(|err| err.to_string())
                .unwrap();

            // Unwrap safety: we validated "inner.handle.is_none()" was false
            // earlier, under a Mutex.
            inner.handle.take().unwrap()
        };
        let _ = handle.await;
        Ok(())
    }
}

/// Describes an object which can be destroyed asynchronously.
///
/// This method is intended to be callable from a drop method,
/// extracting all context of the object that later needs to be
/// deleted.
#[async_trait]
pub trait Deletable: Send + Sync + 'static {
    async fn delete(&self) -> Result<(), Error>;
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::bail;

    // Helper trait for creating a "destructible object" from "Object"
    trait FromContext<C> {
        fn new(ctx: C) -> Self;
    }

    // A test object, which contains some context, and hands it to the
    // destructor when it's dropped.
    struct Object<C, D>
    where
        D: Deletable + FromContext<C>,
    {
        ctx: Option<C>,
        destructor: Destructor<D>,
    }

    impl<C, D> Drop for Object<C, D>
    where
        D: Deletable + FromContext<C>,
    {
        fn drop(&mut self) {
            self.destructor.enqueue_destroy(D::new(self.ctx.take().unwrap()));
        }
    }

    #[tokio::test]
    async fn test_delayed_delete() {
        // Object under test, with deferred deletion
        type Context = Arc<Mutex<bool>>;

        struct ObjectDestruction {
            deleted: Context,
        }

        impl FromContext<Context> for ObjectDestruction {
            fn new(ctx: Context) -> Self {
                Self { deleted: ctx }
            }
        }

        #[async_trait]
        impl Deletable for ObjectDestruction {
            async fn delete(&self) -> Result<(), Error> {
                *self.deleted.lock().unwrap() = true;
                Ok(())
            }
        }

        // Test:
        // - Create the object to-be-detroyed
        // - Drop it
        // - Watch it get destroyed asynchronously

        let mut destructor = Destructor::<ObjectDestruction>::new();
        let deleted = Arc::new(Mutex::new(false));
        let obj = Object {
            ctx: Some(deleted.clone()),
            destructor: destructor.clone(),
        };

        assert!(!*deleted.lock().unwrap());
        drop(obj);
        // The object may or may not be destroyed until the destructor is fully
        // closed.
        while let Err(d) = destructor.try_close().await {
            destructor = d;
        }
        assert!(*deleted.lock().unwrap());
    }

    #[tokio::test]
    async fn test_delayed_delete_with_error() {
        // Object under test, with deferred deletion
        type Context = Arc<Mutex<i32>>;

        struct ObjectDestruction {
            delete_count: Context,
        }

        impl FromContext<Context> for ObjectDestruction {
            fn new(ctx: Context) -> Self {
                Self { delete_count: ctx }
            }
        }

        #[async_trait]
        impl Deletable for ObjectDestruction {
            async fn delete(&self) -> Result<(), Error> {
                let mut count = self.delete_count.lock().unwrap();
                *count += 1;
                if *count < 3 {
                    bail!("Deletion failure!");
                }
                Ok(())
            }
        }

        // Test:
        // - Create the object to-be-detroyed
        // - Drop it
        // - Observe that it does not become deleted successfully
        // until after a few failures
        // - Observe that it is deleted successfully after that

        let mut destructor = Destructor::<ObjectDestruction>::new();
        let delete_count = Arc::new(Mutex::new(0));
        let obj = Object {
            ctx: Some(delete_count.clone()),
            destructor: destructor.clone(),
        };

        assert!(*delete_count.lock().unwrap() == 0);
        drop(obj);
        // The object may or may not be destroyed until the destructor is fully
        // closed.
        while let Err(d) = destructor.try_close().await {
            destructor = d;
        }
        assert!(*delete_count.lock().unwrap() == 3);
    }

    #[tokio::test]
    async fn test_destructor_bad_close() {
        type Context = ();

        struct ObjectDestruction {}

        impl FromContext<Context> for ObjectDestruction {
            fn new(_ctx: Context) -> Self {
                Self {}
            }
        }

        #[async_trait]
        impl Deletable for ObjectDestruction {
            async fn delete(&self) -> Result<(), Error> {
                Ok(())
            }
        }

        // While multiple strong references exist to a
        // destructor, it cannot be closed. This is intended
        // to prevent "concurrent enqueue" with closing.
        let destructor = Destructor::<ObjectDestruction>::new();
        let destructor2 = destructor.clone();
        let destructor =
            destructor.try_close().await.expect_err("Should have failed");
        drop(destructor2);
        assert!(destructor.try_close().await.is_ok());

        // Observe the same outcome even if there are objects
        // being deleted.
        let destructor = Destructor::<ObjectDestruction>::new();
        let obj = Object { ctx: Some(()), destructor: destructor.clone() };
        drop(obj);
        let destructor2 = destructor.clone();
        let destructor =
            destructor.try_close().await.expect_err("Should have failed");
        drop(destructor2);
        assert!(destructor.try_close().await.is_ok());

        // Note that the strong reference inside "obj"
        // also counts as a strong reference.
        let destructor = Destructor::<ObjectDestruction>::new();
        let obj = Object { ctx: Some(()), destructor: destructor.clone() };
        let destructor =
            destructor.try_close().await.expect_err("Should have failed");
        drop(obj);
        assert!(destructor.try_close().await.is_ok());

        // Dropping the destructor before the object is fine, if weird.
        // It prevents the worker from being "joined" upon.
        let destructor = Destructor::<ObjectDestruction>::new();
        let obj = Object { ctx: Some(()), destructor: destructor.clone() };
        drop(destructor);
        drop(obj);

        // Try the other drop order too.
        let destructor = Destructor::<ObjectDestruction>::new();
        let obj = Object { ctx: Some(()), destructor: destructor.clone() };
        drop(obj);
        drop(destructor);
    }
}
