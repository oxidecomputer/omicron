/// Derive the `Target` trait for a type.
#[proc_macro_derive(Target)]
pub fn target(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    oximeter_macro_impl::target(input.into())
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
