//! This macro defines attributes associated with HTTP handlers. These
//! attributes are used both to define an HTTP API and to generate an OpenAPI
//! Spec (OAS) v3 document that describes the API.

extern crate proc_macro;

use proc_macro2::{TokenStream, TokenTree};
use quote::quote;
use quote::ToTokens;

use std::collections::HashMap;

use serde::Deserialize;

use serde_tokenstream::from_tokenstream;
use serde_tokenstream::Error;
use syn::spanned::Spanned;

// We use the `abort` macro to identify known, aberrant conditions while
// processing macro parameters. This is based on `proc_macro_error::abort`
// but modified to be useable in testing contexts where a `proc_macro2::Span`
// cannot be used in an error context.
macro_rules! abort {
    ($span:expr, $($tts:tt)*) => {
        if cfg!(test) {
            panic!($($tts)*)
        } else {
            proc_macro_error::abort!($span, $($tts)*)
        }
    };
}

/// name - in macro; required
/// in - in macro; required
/// description - in macro; optional
/// required - in code: Option<T>
/// deprecated - in macro; optional/future work
/// allowEmptyValue - future work
///
/// style - ignore for now
/// explode - talk to dap
/// allowReserved - future work
/// schema - in code: derived from type
/// example - not supported (see examples)
/// examples - in macro: optional/future work

#[derive(Deserialize, Debug)]
enum InType {
    #[serde(rename = "query")]
    Query,
    #[serde(rename = "header")]
    Header,
    #[serde(rename = "path")]
    Path,
    #[serde(rename = "cookie")]
    Cookie,
}

#[derive(Deserialize, Debug)]
struct Parameter {
    name: String,
    #[serde(rename = "in")]
    inn: InType,
    description: Option<String>,
    deprecated: Option<bool>,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug)]
enum MethodType {
    DELETE,
    GET,
    PATCH,
    POST,
    PUT,
}

impl MethodType {
    fn as_str(self) -> &'static str {
        match self {
            MethodType::DELETE => "DELETE",
            MethodType::GET => "GET",
            MethodType::PATCH => "PATCH",
            MethodType::POST => "POST",
            MethodType::PUT => "PUT",
        }
    }
}

#[derive(Deserialize, Debug)]
struct Metadata {
    method: MethodType,
    path: String,
    parameters: Vec<Parameter>,
}

/// Attribute to apply to an HTTP endpoint.
/// TODO(doc) explain intended use
#[proc_macro_error::proc_macro_error]
#[proc_macro_attribute]
pub fn endpoint(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match do_endpoint(attr, item) {
        Ok(result) => result,
        Err(err) => err.to_compile_error().into(),
    }
}

fn do_endpoint(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> Result<proc_macro::TokenStream, Error> {
    let metadata = from_tokenstream::<Metadata>(&TokenStream::from(attr))?;

    let method = metadata.method.as_str();
    let path = metadata.path;

    let param_map = metadata
        .parameters
        .iter()
        .map(|parameter| (&parameter.name, parameter))
        .collect::<HashMap<_, _>>();

    let ast: syn::ItemFn = syn::parse(item).unwrap();
    let name = ast.sig.ident.clone();

    let mut context = false;

    let cty = quote! {
        Arc<RequestContext>
    };

    let args = &ast.sig.inputs;

    // Do validation of the fn paramters against the metadata we have.
    for (i, arg) in args.iter().enumerate() {
        match arg {
            syn::FnArg::Typed(parameter) => match &*parameter.pat {
                syn::Pat::Ident(id) => {
                    let mut tt = TokenStream::new();
                    parameter.ty.to_tokens(&mut tt);
                    if tokenstream_eq(&cty, &tt) {
                        context = true;
                        if i != 0 {
                            return Err(Error::new(
                                arg.span(),
                                "context parameter needs to be first",
                            ));
                        }
                    } else {
                        if param_map.get(&id.ident.to_string()).is_none() {
                            return Err(Error::new(
                                arg.span(),
                                "param not described",
                            ));
                        }
                    }
                }
                _ => {
                    return Err(Error::new(
                        parameter.span(),
                        "unexpected parameter type",
                    ));
                }
            },
            syn::FnArg::Receiver(_self) => {
                return Err(Error::new(
                    arg.span(),
                    "attribute cannot be applied to a method that uses self",
                ));
            }
        }
    }

    let mut vars = vec![];
    let mut ins = vec![];

    if context {
        ins.push(quote! { rqctx });
    }

    for arg in args.iter().skip(if context { 1 } else { 0 }) {
        match arg {
            syn::FnArg::Receiver(_) => panic!("caught above"),
            syn::FnArg::Typed(parameter) => match &*parameter.pat {
                syn::Pat::Ident(id) => {
                    let meta = param_map.get(&id.ident.to_string()).unwrap();
                    let ident = &id.ident;
                    let ty = &parameter.ty;

                    match meta.inn {
                        InType::Path => {
                            vars.push(quote! {
                                let #ident: #ty =
                                    http_extract_path_param(
                                        &rqctx.path_variables,
                                        &stringify!(#ident).to_string()
                                    )?.clone();
                            });
                            ins.push(quote! { #ident });
                        }
                        _ => panic!("not implemented"),
                    }
                }
                _ => abort!(parameter, "unexpected parameter type"),
            },
        }
    }

    let method_ident = quote::format_ident!("{}", method);

    let stream = quote! {
        #[allow(non_camel_case_types, missing_docs)]
        pub struct #name;
        impl #name {
            fn register(api: &mut dropshot::ApiDescription) {
                #ast
                async fn handle(
                    rqctx: Arc<RequestContext>
                ) -> Result<HttpResponseOkObject<ApiProjectView>, HttpError> {
                    #(#vars;)*
                    #name(#(#ins),*).await
                }
                api.register(Method::#method_ident, #path, HttpRouteHandler::new(handle));
            }
        }
    };
    Ok(stream.into())
}

/// Conservative TokenStream equality.
fn tokenstream_eq(a: &TokenStream, b: &TokenStream) -> bool {
    let mut aa = a.clone().into_iter();
    let mut bb = b.clone().into_iter();

    loop {
        match (aa.next(), bb.next()) {
            (None, None) => break true,
            (Some(TokenTree::Ident(at)), Some(TokenTree::Ident(bt)))
                if at == bt =>
            {
                continue
            }
            (Some(TokenTree::Punct(at)), Some(TokenTree::Punct(bt)))
                if at.as_char() == bt.as_char() =>
            {
                continue
            }
            _ => break false,
        }
    }
}
