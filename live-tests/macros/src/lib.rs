// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Macro to wrap a live test function that automatically creates and cleans up
//! the `LiveTestContext`

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// Define a test function that uses `LiveTestContext`
///
/// This is usable only within the `omicron-live-tests` crate.
///
/// Similar to `nexus_test`, this macro lets you define a test function that
/// behaves like `tokio::test` except that it accepts an argument of type
/// `&LiveTestContext`.  The `LiveTestContext` is cleaned up on _successful_
/// return of the test function.  On failure, debugging information is
/// deliberately left around.
///
/// Example usage:
///
/// ```ignore
/// #[live_test]
/// async fn test_my_test_case(lc: &LiveTestContext) {
///   assert!(true);
/// }
/// ```
///
/// We use this instead of implementing Drop on LiveTestContext because we want
/// the teardown to only happen when the test doesn't fail (which causes a panic
/// and unwind).
#[proc_macro_attribute]
pub fn live_test(_attrs: TokenStream, input: TokenStream) -> TokenStream {
    let input_func = parse_macro_input!(input as ItemFn);

    let mut correct_signature = true;
    if input_func.sig.variadic.is_some()
        || input_func.sig.inputs.len() != 1
        || input_func.sig.asyncness.is_none()
    {
        correct_signature = false;
    }

    // Verify we're returning an empty tuple
    correct_signature &= match input_func.sig.output {
        syn::ReturnType::Default => true,
        syn::ReturnType::Type(_, ref t) => {
            if let syn::Type::Tuple(syn::TypeTuple { elems, .. }) = &**t {
                elems.is_empty()
            } else {
                false
            }
        }
    };
    if !correct_signature {
        panic!("func signature must be async fn(&LiveTestContext)");
    }

    let func_ident_string = input_func.sig.ident.to_string();
    let func_ident = input_func.sig.ident.clone();
    let new_block = quote! {
        {
            #input_func

            let ctx = crate::common::LiveTestContext::new(
                #func_ident_string
            ).await.expect("setting up LiveTestContext");
            #func_ident(&ctx).await;
            ctx.cleanup_successful().await;
        }
    };
    let mut sig = input_func.sig.clone();
    sig.inputs.clear();
    let func = ItemFn {
        attrs: input_func.attrs,
        vis: input_func.vis,
        sig,
        block: Box::new(syn::parse2(new_block).unwrap()),
    };
    TokenStream::from(quote!(
    #[::tokio::test]
    #func
    ))
}
