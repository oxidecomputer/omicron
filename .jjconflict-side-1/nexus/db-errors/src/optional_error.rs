// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::result::Error as DieselError;
use std::sync::{Arc, Mutex};

/// Helper utility for passing non-retryable errors out-of-band from
/// transactions.
///
/// Transactions prefer to act on the `diesel::result::Error` type,
/// but transaction users may want more meaningful error types.
/// This utility helps callers safely propagate back Diesel errors while
/// retaining auxiliary error info.
pub struct OptionalError<E>(Arc<Mutex<Option<E>>>);

impl<E> Clone for OptionalError<E> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<E: std::fmt::Debug> OptionalError<E> {
    #[expect(clippy::new_without_default)] // ehh Default isn't really logical here?
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    /// Sets "Self" to the value of `error` and returns `DieselError::RollbackTransaction`.
    pub fn bail(&self, err: E) -> DieselError {
        (*self.0.lock().unwrap()).replace(err);
        DieselError::RollbackTransaction
    }

    /// If `diesel_error` is retryable, returns it without setting Self.
    ///
    /// Otherwise, sets "Self" to the value of `err`, and returns
    /// `DieselError::RollbackTransaction`.
    pub fn bail_retryable_or(
        &self,
        diesel_error: DieselError,
        err: E,
    ) -> DieselError {
        self.bail_retryable_or_else(diesel_error, |_diesel_error| err)
    }

    /// If `diesel_error` is retryable, returns it without setting Self.
    ///
    /// Otherwise, sets "Self" to the value of `f` applied to `diesel_err`, and
    /// returns `DieselError::RollbackTransaction`.
    pub fn bail_retryable_or_else<F>(
        &self,
        diesel_error: DieselError,
        f: F,
    ) -> DieselError
    where
        F: FnOnce(DieselError) -> E,
    {
        if crate::retryable(&diesel_error) {
            diesel_error
        } else {
            self.bail(f(diesel_error))
        }
    }

    /// If "Self" was previously set to a non-retryable error, return it.
    pub fn take(self) -> Option<E> {
        (*self.0.lock().unwrap()).take()
    }
}
