// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for pluralization of text.

pub fn errors_str(count: usize) -> &'static str {
    if count == 1 { "error" } else { "errors" }
}

pub fn consumers_str(count: usize) -> &'static str {
    if count == 1 { "consumer" } else { "consumers" }
}
