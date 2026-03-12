// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared utilities for saga actions.

use omicron_common::api::external::Error;
use steno::ActionError;

/// Convert an [`Error`] into a [`steno::ActionError`].
///
/// This is a restricted wrapper around [`steno::ActionError::action_failed`]
/// that only accepts `omicron_common::api::external::Error`. The underlying
/// steno method accepts any serializable type, which makes it easy to
/// accidentally pass in a raw `String` or other type that loses structured
/// error information.
pub fn saga_action_failed(error: Error) -> steno::ActionError {
    #[expect(clippy::disallowed_methods)]
    ActionError::action_failed(error)
}
