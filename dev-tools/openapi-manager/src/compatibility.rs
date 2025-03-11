// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Determine if one OpenAPI spec is a subset of another

use openapiv3::OpenAPI;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("all changes are considered incompatible right now")]
pub struct OpenApiCompatibilityError {}

pub fn api_compatible(
    spec1: &OpenAPI,
    spec2: &OpenAPI,
) -> Vec<OpenApiCompatibilityError> {
    if *spec1 != *spec2 {
        vec![OpenApiCompatibilityError {}]
    } else {
        Vec::new()
    }
}
