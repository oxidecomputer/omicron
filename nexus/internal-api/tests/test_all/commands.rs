// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use expectorate::assert_contents;
use nexus_internal_api::{
    openapi_definition, NexusInternalApi_to_stub_api_description,
};
use openapiv3::OpenAPI;

#[test]
fn test_nexus_openapi_internal() {
    let api = NexusInternalApi_to_stub_api_description().unwrap();
    let def = openapi_definition(&api);

    // We can't use `def.json()` for the `assert_contents` string comparison
    // below, because serde_json::Value may end up reordering items depending
    // on which features are enabled.
    let mut json_out = Vec::new();
    def.write(&mut json_out).unwrap();
    let json_str = String::from_utf8(json_out).unwrap();

    let spec: OpenAPI =
        serde_json::from_str(&json_str).expect("definition was valid OpenAPI");

    // Check for lint errors.
    let errors = openapi_lint::validate(&spec);
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));

    // Confirm that the output hasn't changed. It's expected that we'll change
    // this file as the API evolves, but pay attention to the diffs to ensure
    // that the changes match your expectations.
    assert_contents("../../openapi/nexus-internal.json", &json_str);
}
