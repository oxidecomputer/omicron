/// Derive the `Metric` trait for a struct.
#[proc_macro_attribute]
pub fn metric(
    attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    oximeter_macro_impl::metric(attr.into(), input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
