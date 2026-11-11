// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

mod artifact;
mod async_temp_file;
mod bootstrap;
mod dispatch;
mod errors;
mod fetch;
mod hardware;
#[cfg(test)]
mod mock_peers;
mod peers;
mod reporter;
#[cfg(test)]
mod test_helpers;
mod write;

pub use dispatch::*;
pub use write::*;

#[cfg(feature = "image-standard")]
compile_error!("Installinator should not be built with `-i standard`");
