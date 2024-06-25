use anyhow::Context;
use anyhow::Result;
use oximeter_impl::schema::ir::load_schema;
use oximeter_impl::schema::SCHEMA_DIRECTORY;
use std::env;
use std::fs;
use std::path;

fn main() -> Result<()> {
    let mut all_schema = Vec::new();
    for entry in fs::read_dir(SCHEMA_DIRECTORY).with_context(|| {
        format!("failed to read schema directory '{SCHEMA_DIRECTORY}'")
    })? {
        let entry = entry.with_context(|| {
            format!(
                "failed to read entry in schema directory '{SCHEMA_DIRECTORY}'"
            )
        })?;
        let path = entry.path();
        let contents = fs::read_to_string(&path).with_context(|| {
            format!("failed to read schema file '{}'", path.display(),)
        })?;
        let schema = load_schema(&contents).with_context(|| {
            format!("failed to load schema from '{}'", path.display())
        })?;
        all_schema.extend(schema);
        println!("cargo::rerun-if-changed={}", path.display());
    }

    let len = all_schema.len();
    let tokens = quote::quote! {
        /// Return all timeseries schema known at build time.
        pub fn all_timeseries_schema() -> &'static [::oximeter::TimeseriesSchema] {
            static SCHEMA:
                ::std::sync::OnceLock<[::oximeter::TimeseriesSchema; #len]>
            = ::std::sync::OnceLock::new();
            SCHEMA.get_or_init(|| {
                [
                    #( #all_schema, )*
                ]
            })
        }
    };

    let file = syn::parse_file(&tokens.to_string())
        .context("failed to parse schema function as syn::File")?;
    let contents = prettyplease::unparse(&file);
    let out_dir = env::var("OUT_DIR").expect("Cargo should set OUT_DIR");
    let out_file = path::Path::new(&out_dir).join("all_schema.rs");
    fs::write(&out_file, contents).with_context(|| {
        format!(
            "failed to write all schema function to '{}'",
            out_file.display()
        )
    })
}
