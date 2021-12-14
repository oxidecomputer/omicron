use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// Attribute for wrapping a test function to handle automatically
/// creating and destroying a ControlPlaneTestContext. If the wrapped test
/// fails, the context will intentionally not be cleaned up to support
/// debugging. The wrapped test a
///
/// Example usage:
///
/// // #[nexus_test]
/// // async fn test_my_test_case(cptestctx: &ControlPlaneTestContext) {
/// //   assert!(true);
/// // }
///
/// We use this instead of implementing Drop on ControlPlaneTestContext because
/// we want the teardown to only happen when the test doesn't fail (which causes
/// a panic and unwind).
#[proc_macro_attribute]
pub fn nexus_test(_metadata: TokenStream, input: TokenStream) -> TokenStream {
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
                elems.len() == 0
            } else {
                false
            }
        }
    };
    if !correct_signature {
        panic!("func signature must be async fn(&ControlPlaneTestContext)");
    }

    let func_ident_string = input_func.sig.ident.to_string();
    let func_ident = input_func.sig.ident.clone();
    let new_block = quote! {
        {
            #input_func

            let ctx = ::nexus_test_utils::test_setup(#func_ident_string).await;
            #func_ident(&ctx).await;
            ctx.teardown().await;
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
