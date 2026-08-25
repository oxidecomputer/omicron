// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = parse)]
pub fn parse_js(query: &str) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(&oxql::parse(query))
        .map_err(|error| JsValue::from_str(&error.to_string()))
}
