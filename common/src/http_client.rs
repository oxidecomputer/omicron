// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Construction of HTTP clients for talking to other control plane services.
//!
//! By default, building a [`reqwest::Client`] loads the system's TLS root
//! certificates from disk, and fails if none can be read (for example, if the
//! process has run out of file descriptors). Services within the control plane
//! talk to each other over plain HTTP, so they have no use for those
//! certificates.
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

        // Panics if the client cannot be built.
        let _ = internal_http_client();

        // On these platforms, reqwest's default client loads TLS roots from
        // disk, so it should fail to build. If this starts succeeding, the
        // test above no longer demonstrates anything about TLS roots.
        #[cfg(any(target_os = "linux", target_os = "illumos"))]
        reqwest::ClientBuilder::new().build().expect_err(
            "expected a default reqwest client to fail to build without \
             file descriptors (has reqwest stopped loading TLS roots when \
             building a client?)",
        );

        drop(held);
    }
}
