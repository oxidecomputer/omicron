// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Validates that legacy spoof-based authentication does not work
#![cfg(test)]

use crate::helpers::ctx::ClientParams;
use anyhow::Result;
use std::time::Duration;

#[tokio::test]
async fn no_spoof_here() -> Result<()> {
    let client_params = ClientParams::new()?;
    let reqwest_client = client_params
        .reqwest_builder()
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(60));
    let base_url = client_params.base_url();
    assert!(
        !omicron_test_utils::test_spoof_works(reqwest_client, &base_url)
            .await?,
        "unexpectedly succeeded in using spoof authn!"
    );
    Ok(())
}
