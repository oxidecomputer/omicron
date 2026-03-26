use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashSet as Set;
use syn::parse::{Parse, ParseStream, Result};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{ItemFn, Token, parse_macro_input};

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) enum NexusTestArg {
    ServerUnderTest(syn::Path),
    ExtraSledAgents(u16),
}

impl syn::parse::Parse for NexusTestArg {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let name: syn::Path = input.parse()?;
        let _eq_token: syn::token::Eq = input.parse()?;

        if name.is_ident("server") {
            let path: syn::Path = input.parse()?;
            return Ok(Self::ServerUnderTest(path));
        }

        if name.is_ident("extra_sled_agents") {
            let value: syn::LitInt = input.parse()?;
            let value = value.base10_parse::<u16>()?;
            return Ok(Self::ExtraSledAgents(value));
        }

        Err(syn::Error::new(name.span(), "unrecognized argument to nexus_test"))
    }
}

struct NexusTestArgs(Set<NexusTestArg>);

impl Parse for NexusTestArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let vars =
            Punctuated::<NexusTestArg, Token![,]>::parse_terminated(input)?;
        Ok(NexusTestArgs(vars.into_iter().collect()))
    }
}

/// Attribute for wrapping a test function to handle automatically
/// creating and destroying a ControlPlaneTestContext. If the wrapped test
/// fails, the context will intentionally not be cleaned up to support
/// debugging. The wrapped test a
///
/// Example usage:
///
/// ```ignore
/// use ControlPlaneTestContext =
///     nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;
/// #[nexus_test]
/// async fn test_my_test_case(cptestctx: &ControlPlaneTestContext) {
///   assert!(true);
/// }
/// ```
///
/// We use this instead of implementing Drop on ControlPlaneTestContext because
/// we want the teardown to only happen when the test doesn't fail (which causes
/// a panic and unwind).
#[proc_macro_attribute]
pub fn nexus_test(attrs: TokenStream, input: TokenStream) -> TokenStream {
    let input_func = parse_macro_input!(input as ItemFn);

    let mut correct_signature = true;
    if input_func.sig.variadic.is_some()
        || input_func.sig.inputs.len() != 1
        || input_func.sig.asyncness.is_none()
    {
        correct_signature = false;
    }

    // By default, import "omicron_nexus::Server" as the server under test.
    //
    // However, a caller can supply their own implementation of the server
    // using:
    //
    // #[nexus_test(server = <CUSTOM SERVER>)]
    //
    // This mechanism allows Nexus unit test to be tested using the `nexus_test`
    // macro without a circular dependency on nexus-test-utils.
    //
    // As well, a caller can request that extra sled agents be built using
    //
    // #[nexus_test(extra_sled_agents = N)]

    let nexus_test_args = parse_macro_input!(attrs as NexusTestArgs);

    let mut which_nexus =
        syn::parse_str::<syn::Path>("::omicron_nexus::Server").unwrap();
    let mut extra_sled_agents: u16 = 0;

    for arg in nexus_test_args.0 {
        match arg {
            NexusTestArg::ServerUnderTest(server) => {
                which_nexus = server;
            }

            NexusTestArg::ExtraSledAgents(value) => {
                extra_sled_agents = value;
            }
        }
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
        panic!("func signature must be async fn(&ControlPlaneTestContext)");
    }

    let func_ident_string = input_func.sig.ident.to_string();
    let func_ident = input_func.sig.ident.clone();
    let new_block = quote! {
        {
            #input_func

            let ctx = ::nexus_test_utils::ControlPlaneBuilder::new(
                #func_ident_string,
            )
            .with_extra_sled_agents(#extra_sled_agents)
            .start::<#which_nexus>()
            .await;
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
