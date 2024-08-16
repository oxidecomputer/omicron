// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

//! Procedural macro to emi Rust code matching a TOML timeseries definition.

extern crate proc_macro;

use oximeter_types::schema::SCHEMA_DIRECTORY;

/// Generate code to use the timeseries from one target.
///
/// This macro accepts a single filename, which should be a file in the
/// `oximeter/schema` subdirectory, containing a valid timeseries definition. It
/// attempts parse the file and generate code used to produce samples from those
/// timeseries. It will generate a submodule named by the target in the file,
/// with a Rust struct for the target and each metric defined in the file.
#[proc_macro]
pub fn use_timeseries(
    tokens: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match syn::parse::<syn::LitStr>(tokens) {
        Ok(token) => {
            let path = match extract_timeseries_path(&token) {
                Ok(path) => path,
                Err(err) => return err,
            };

            // Now that we have a verified file name only, look up the contents
            // within the schema directory; validate it; and generate code from
            // it.
            let contents = match std::fs::read_to_string(&path) {
                Ok(c) => c,
                Err(e) => {
                    let msg = format!(
                        "Failed to read timeseries schema \
                        from file '{}': {:?}",
                        path.display(),
                        e,
                    );
                    return syn::Error::new(token.span(), msg)
                        .into_compile_error()
                        .into();
                }
            };
            match oximeter_schema::codegen::use_timeseries(&contents) {
                Ok(toks) => {
                    let path_ = path.display().to_string();
                    return quote::quote! {
                        /// Include the schema file itself to ensure we recompile
                        /// when that changes.
                        const _: &str = include_str!(#path_);
                        #toks
                    }
                    .into();
                }
                Err(e) => {
                    let msg = format!(
                        "Failed to generate timeseries types \
                        from '{}': {:?}",
                        path.display(),
                        e,
                    );
                    return syn::Error::new(token.span(), msg)
                        .into_compile_error()
                        .into();
                }
            }
        }
        Err(e) => return e.into_compile_error().into(),
    }
}

// Extract the full path to the timeseries definition, from the macro input
// tokens. We currently only allow a filename with no other path components, to
// avoid looking in directories other than the `SCHEMA_DIRECTORY`.
fn extract_timeseries_path(
    token: &syn::LitStr,
) -> Result<std::path::PathBuf, proc_macro::TokenStream> {
    let make_err = || {
        let path = std::path::Path::new(SCHEMA_DIRECTORY)
            .canonicalize()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| SCHEMA_DIRECTORY.to_string());
        let msg = format!(
            "Input must be a valid filename with no \
            path components and directly within the \
            schema directory '{}'",
            path,
        );
        Err(syn::Error::new(token.span(), msg).into_compile_error().into())
    };
    let value = token.value();
    if value.is_empty() {
        return make_err();
    }
    let Some(filename) = std::path::Path::new(&value).file_name() else {
        return make_err();
    };
    if filename != value.as_str() {
        return make_err();
    }
    Ok(std::path::Path::new(SCHEMA_DIRECTORY).join(&filename))
}
