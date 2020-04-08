//! This macro defines attributes associated with HTTP handlers. These
//! attributes are used both to define an HTTP API and to generate an OpenAPI
//! Spec (OAS) v3 document that describes the API.

extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::quote;

use serde::Deserialize;

use serde_tokenstream::from_tokenstream;
use serde_tokenstream::Error;

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
    #[serde(rename = "cookie")]
    Cookie,
    #[serde(rename = "header")]
    Header,
    #[serde(rename = "path")]
    Path,
    #[serde(rename = "query")]
    Query,
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

    let ast: syn::ItemFn = syn::parse(item).unwrap();

    let _ = extract_doc(&ast);

    let name = ast.sig.ident.clone();
    let method_ident = quote::format_ident!("{}", method);

    // The final TokenStream returned will have a few components that reference
    // `#name`, the name of the method to which this macro was applied.
    let stream = quote! {
        // struct type called `#name` that has no members
        #[allow(non_camel_case_types, missing_docs)]
        pub struct #name {}
        // constant of type `#name` whose identifier is also #name
        #[allow(non_upper_case_globals, missing_docs)]
        const #name: #name = #name {};

        // impl of `From<#name>` for Endpoint that allows the constant `#name`
        // to be passed into `ApiDescription::register()`
        impl<'a> From<#name> for Endpoint<'a> {
            fn from(_: #name) -> Self {
                #ast

                Endpoint::new(
                    HttpRouteHandler::new(#name),
                    Method::#method_ident,
                    #path,
                )
            }
        }
    };

    Ok(stream.into())
}

fn extract_doc(item: &syn::ItemFn) -> Result<(), Error> {
    println!("{:?}", item);
    for attr in &item.attrs {
        println!("attr {:?}", attr);
        let meta = attr.parse_meta().unwrap();
        if let syn::Meta::NameValue(nv) = meta {
            if nv.path.leading_colon.is_none()
                && nv.path.segments.len() == 1
                && &nv.path.segments[0].ident
                    == &syn::Ident::new("doc", proc_macro2::Span::call_site())
            {
                if let syn::Lit::Str(s) = nv.lit {
                    println!("parse {:?}", attr.parse_meta());
                    println!("path {:?}", attr.path);
                    println!("  s {:?}", s.value());
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    #[test]
    fn doc() {
        let item = quote! {
            /// this looks like a comment, but it's actually part of the test
            /// this too
            #[doc = "testing"]
            #[doc(hidden)]
            fn do_nothing() {}
        };

        let ast: syn::ItemFn = syn::parse2(item).unwrap();

        let _ = extract_doc(&ast);

        panic!("bad");
    }
}
