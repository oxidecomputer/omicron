// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = parse)]
pub fn parse_js(query: &str) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(&oxql::parse(query))
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[wasm_bindgen(js_name = completionContext)]
pub fn completion_context_js(
    query: &str,
    cursor: usize,
) -> Result<JsValue, JsValue> {
    let cursor = utf16_to_byte_offset(query, cursor).ok_or_else(|| {
        JsValue::from_str("cursor is not a valid JavaScript string boundary")
    })?;
    let mut context = oxql::completion_context(query, cursor)
        .map_err(|error| JsValue::from_str(&error.message))?;
    context.replacement.start =
        byte_to_utf16_offset(query, context.replacement.start);
    context.replacement.end =
        byte_to_utf16_offset(query, context.replacement.end);
    serde_wasm_bindgen::to_value(&context)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

fn utf16_to_byte_offset(source: &str, target: usize) -> Option<usize> {
    let mut utf16_offset = 0;
    for (byte_offset, character) in source.char_indices() {
        if utf16_offset == target {
            return Some(byte_offset);
        }
        utf16_offset += character.len_utf16();
        if utf16_offset > target {
            return None;
        }
    }
    (utf16_offset == target).then_some(source.len())
}

fn byte_to_utf16_offset(source: &str, byte_offset: usize) -> usize {
    source[..byte_offset].encode_utf16().count()
}
