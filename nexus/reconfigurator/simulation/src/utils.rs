// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for internal simulation.

use std::fmt;

use swrite::{SWrite, swrite};

pub(crate) fn join_comma_or_none<I, T: fmt::Display>(iter: I) -> String
where
    I: IntoIterator<Item = T>,
{
    let mut iter = iter.into_iter();
    match iter.next() {
        Some(first) => iter.fold(first.to_string(), |mut acc, x| {
            swrite!(acc, ", {}", x);
            acc
        }),
        None => "(none)".to_string(),
    }
}
