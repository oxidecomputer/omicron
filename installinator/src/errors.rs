// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{net::Ipv6Addr, time::Duration};

use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum ArtifactFetchError {
    #[error("peer {peer} returned an HTTP error")]
    HttpError { peer: Ipv6Addr, error: wicketd_client::Error },

    #[error("peer {peer} timed out ({timeout:?}) after returning {bytes_fetched} bytes")]
    Timeout { peer: Ipv6Addr, timeout: Duration, bytes_fetched: usize },
}
