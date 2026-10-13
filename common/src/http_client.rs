// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Construction of HTTP clients for talking to other control plane services.
//!
//! By default, building a [`reqwest::Client`] loads the system's TLS root
//! certificates from disk, and fails if none can be read (for example, if the
//! process has run out of file descriptors). Services within the control plane
//! talk to each other over plain HTTP, so they have no use for those
//! certificates. The builders in this module trust no TLS roots at all, which
//! means building a client does no I/O.
//!
//! See <https://github.com/oxidecomputer/omicron/issues/10871>.

use std::time::Duration;

/// The connect and request timeout applied by a progenitor-generated
/// `Client::new`.
const PROGENITOR_DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);

/// Returns a [`reqwest::ClientBuilder`] for plain-HTTP communication with
/// other control plane services.
///
/// The resulting client trusts no TLS root certificates: requests to `https`
/// URLs will fail certificate verification. Clients that need to speak TLS
/// (e.g., to services outside the rack) should not use this builder.
pub fn internal_http_client_builder() -> reqwest::ClientBuilder {
    reqwest::ClientBuilder::new().tls_certs_only([])
}

/// Returns a [`reqwest::Client`] for plain-HTTP communication with other
/// control plane services, configured with the same timeouts as a
/// progenitor-generated `Client::new`.
///
/// Pass this to a generated client's `new_with_client` in place of calling its
/// `new`, which builds a client that loads TLS root certificates from disk.
pub fn internal_http_client() -> reqwest::Client {
    internal_http_client_builder()
        .connect_timeout(PROGENITOR_DEFAULT_TIMEOUT)
        .timeout(PROGENITOR_DEFAULT_TIMEOUT)
        .build()
        .expect("building a client without TLS roots does no I/O")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn internal_http_client_builds() {
        let _ = internal_http_client();
    }
}
