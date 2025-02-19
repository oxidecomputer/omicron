// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Extract the only item from an iterator

use std::fmt::Debug;
use thiserror::Error;

/// Error returned by [`iter_only`]
#[derive(Debug, Error)]
pub enum OnlyError {
    #[error("list was unexpectedly empty")]
    Empty,

    #[error(
        "found at least two elements in a list that was expected to contain \
         exactly one: {0} {1}"
    )]
    // Store the debug representations directly here rather than the values
    // so that `OnlyError: 'static` (so that it can be used as the cause of
    // another error) even when `T` is not 'static.
    Extra(String, String),
}

/// Extract the only item from an iterator, failing if there are 0 or more than
/// one item
pub fn iter_only<T: Debug>(
    mut iter: impl Iterator<Item = T>,
) -> Result<T, OnlyError> {
    let first = iter.next().ok_or(OnlyError::Empty)?;
    match iter.next() {
        None => Ok(first),
        Some(second) => Err(OnlyError::Extra(
            format!("{:?}", first),
            format!("{:?}", second),
        )),
    }
}
