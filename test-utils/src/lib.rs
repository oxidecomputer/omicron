// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to assist with testing across Omicron crates.

// We only use rustdoc for internal documentation, including private items, so
// it's expected that we'll have links to private items in the docs.
#![allow(rustdoc::private_intra_doc_links)]

use anyhow::anyhow;
use anyhow::Context;
use headers::authorization::Credentials;

pub mod certificates;
pub mod dev;

#[macro_use]
extern crate slog;

/// Tests whether legacy spoof authentication works
///
/// This is used to validate configuration in different environments.
pub async fn test_spoof_works(base_url: &str) -> Result<bool, anyhow::Error> {
    let url = format!("{}/v1/me", base_url);
    let header_value = headers::authorization::Authorization::bearer(
        "oxide-spoof-001de000-05e4-4000-8000-000000004007",
    )
    .context("building authorization header")?;
    let response = shared_client::new()
        .get(&url)
        .header(http::header::AUTHORIZATION, header_value.0.encode())
        .send()
        .await
        .context("failed to send request")?;
    let status = response.status();
    if status.is_success() {
        return Ok(true);
    }
    if status == http::StatusCode::UNAUTHORIZED {
        return Ok(false);
    }

    Err(anyhow!("unexpected response from /v1/me (status code {:?})", status))
}
