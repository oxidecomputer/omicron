// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Centralized configuration for HTTP clients used to communicate with other
//! control plane services.
//!
//! Clients for internal services should be built with the functions in this
//! module (rather than [`reqwest::ClientBuilder::new`] or a
//! progenitor-generated `Client::new`), so that they share a common
//! configuration.

use std::time::Duration;

/// The connect and request timeout applied by a progenitor-generated
/// `Client::new`.
const PROGENITOR_DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);

/// Returns a [`reqwest::ClientBuilder`] for plain-HTTP communication with
/// other control plane services.
///
/// The resulting client trusts no TLS root certificates: requests to `https`
/// URLs will fail certificate verification. This builder is not suitable for
/// requests to services outside the rack; in Nexus, use the `external_client`
/// module (`nexus/src/app/external_client.rs`) for those, which applies
/// additional configuration for talking to external services, including
/// protection against server-side request forgery.
pub fn internal_http_client_builder() -> reqwest::ClientBuilder {
    // By default, building a `reqwest::Client` loads the system's TLS root
    // certificates from disk, and fails if none can be read (for example, if
    // the process has run out of file descriptors). Services within the
    // control plane talk to each other over plain HTTP, so they have no use
    // for those certificates: trust none, so building a client does no I/O.
    //
    // See <https://github.com/oxidecomputer/omicron/issues/10871>.
    reqwest::ClientBuilder::new().tls_certs_only([])
}

/// Returns a [`reqwest::Client`] for plain-HTTP communication with other
/// control plane services, configured with the same timeouts as a
/// progenitor-generated `Client::new`.
///
/// Pass this to a generated client's `new_with_client` in place of calling its
/// `new`, which builds a client that loads TLS root certificates from disk
/// and panics if that fails.
pub fn internal_http_client() -> Result<reqwest::Client, reqwest::Error> {
    internal_http_client_builder()
        .connect_timeout(PROGENITOR_DEFAULT_TIMEOUT)
        .timeout(PROGENITOR_DEFAULT_TIMEOUT)
        .build()
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::File;

    // This test exhausts the process's file descriptors, so it relies on
    // nextest running each test in its own process.
    #[test]
    fn internal_http_client_builds_without_file_descriptors() {
        // Lower the soft limit on open files, so that we don't need to open
        // very many to run out.
        let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        // SAFETY: `limit` is a valid, writable `rlimit`.
        let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
        assert_eq!(rc, 0, "getrlimit: {}", std::io::Error::last_os_error());
        limit.rlim_cur = limit.rlim_cur.min(64);
        // SAFETY: `limit` is a valid `rlimit`.
        let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) };
        assert_eq!(rc, 0, "setrlimit: {}", std::io::Error::last_os_error());

        let mut held = Vec::new();
        let err = loop {
            match File::open("/dev/null") {
                Ok(f) => held.push(f),
                Err(e) => break e,
            }
        };
        assert_eq!(
            err.raw_os_error(),
            Some(libc::EMFILE),
            "expected to run out of file descriptors, got: {err}"
        );

        internal_http_client()
            .expect("internal client should build without file descriptors");

        // On these platforms, reqwest's default client loads TLS roots from
        // disk, so it should fail to build. If this starts succeeding, the
        // test above no longer demonstrates anything about TLS roots.
        //
        // This check is skipped on macOS, where building the default client
        // does no I/O: the platform verifier consults the system trust store
        // only when verifying a certificate.
        #[cfg(any(target_os = "linux", target_os = "illumos"))]
        reqwest::ClientBuilder::new().build().expect_err(
            "expected a default reqwest client to fail to build without \
             file descriptors (has reqwest stopped loading TLS roots when \
             building a client?)",
        );

        drop(held);
    }
}
