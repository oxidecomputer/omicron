// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

// TODO-RAINCLAUDE: A value that is computed at most once, by the first caller
// that needs it, and shared with every later caller. Whatever the computation
// returns is stored, including a failure, so later callers don't retry it.
//
// `state` says how far the computation has got, and the `sled_expunge` task
// reports it in its status:
// * `NotStarted` means no caller has needed the value.
// * `Unfinished` means a caller started computing the value, but none has
//   finished.
// * `Done` holds the value.
pub struct LazyOnce<T> {
    started: AtomicBool,
    cell: tokio::sync::OnceCell<T>,
}

#[derive(Debug)]
pub enum LazyOnceState<'a, T> {
    NotStarted,
    Unfinished,
    Done(&'a T),
}

impl<T> LazyOnce<T> {
    pub fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            cell: tokio::sync::OnceCell::new(),
        }
    }

    pub fn new_with(value: T) -> Self {
        Self {
            started: AtomicBool::new(true),
            cell: tokio::sync::OnceCell::new_with(Some(value)),
        }
    }

    pub async fn get_or_init<F, Fut>(&self, init: F) -> &T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        self.cell
            .get_or_init(|| {
                self.started.store(true, Ordering::Relaxed);
                init()
            })
            .await
    }

    pub fn state(&self) -> LazyOnceState<'_, T> {
        match self.cell.get() {
            Some(value) => LazyOnceState::Done(value),
            None if self.started.load(Ordering::Relaxed) => {
                LazyOnceState::Unfinished
            }
            None => LazyOnceState::NotStarted,
        }
    }
}

impl<T> Default for LazyOnce<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_lazy_once_states() {
        let lazy = LazyOnce::<u32>::new();
        match lazy.state() {
            LazyOnceState::NotStarted => {}
            state @ (LazyOnceState::Unfinished | LazyOnceState::Done(_)) => {
                panic!("expected a value not started, found {state:?}")
            }
        }

        let cancelled = lazy.get_or_init(|| std::future::pending::<u32>());
        assert_eq!(
            futures::poll!(Box::pin(cancelled)),
            std::task::Poll::Pending
        );
        match lazy.state() {
            LazyOnceState::Unfinished => {}
            state @ (LazyOnceState::NotStarted | LazyOnceState::Done(_)) => {
                panic!("expected an unfinished value, found {state:?}")
            }
        }

        assert_eq!(*lazy.get_or_init(|| async { 7 }).await, 7);
        assert_eq!(*lazy.get_or_init(|| async { 8 }).await, 7);
        match lazy.state() {
            LazyOnceState::Done(value) => assert_eq!(*value, 7),
            state @ (LazyOnceState::NotStarted | LazyOnceState::Unfinished) => {
                panic!("expected a value, found {state:?}")
            }
        }
    }
}
