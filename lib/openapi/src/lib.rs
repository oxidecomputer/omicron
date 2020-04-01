//! This crate defines attributes associated with HTTP handlers. These
//! attributes are used both to define an HTTP API and to generate an OpenAPI
//! Spec (OAS) v3 document that describes the API.

extern crate proc_macro;

use proc_macro2::{TokenStream, TokenTree};
use quote::quote;
use quote::ToTokens;

use std::collections::HashMap;

use serde::Deserialize;

mod serde_tokenstream;
use crate::serde_tokenstream::from_tokenstream;

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
#[proc_macro_attribute]
pub fn endpoint(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    do_endpoint(attr, item).unwrap()
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

fn do_endpoint(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> Result<proc_macro::TokenStream, &'static str> {
    let attr2 = TokenStream::from(attr);

    let metadata = match from_tokenstream::<Metadata>(&attr2) {
        Ok(value) => value,
        Err(err) => panic!(err),
    };

    println!("{:?}", metadata);

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

    println!("cty = {:?}", cty);

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
                            return Err("context parameter needs to be first");
                        }
                    } else {
                        if param_map.get(&id.ident.to_string()).is_none() {
                            println!("{:?}", id.ident.to_string());
                            return Err("param not described");
                        }
                    }
                }
                _ => return Err("unexpected parameter type"),
            },
            syn::FnArg::Receiver(_selph) => {
                return Err(
                    "attribute cannot be applied to a method that uses self"
                )
            }
        }
    }

    let ins = args.iter().skip(1).map(|arg| match arg {
        syn::FnArg::Receiver(selph) => abort!(
            selph,
            "attribute cannot be applied to a method that uses self"
        ),
        syn::FnArg::Typed(parameter) => match &*parameter.pat {
            syn::Pat::Ident(id) => {
                let ident = &id.ident;
                let ty = &parameter.ty;
                quote! {
                    let #ident: #ty = x;
                }
            }
            _ => abort!(parameter, "unexpected parameter type"),
        },
    });

    let x1 = quote! {
        fn do_nothing() {}
    };
    let x2 = quote! {
        fn do_less() {}
    };
    let mut xxx = quote! {};
    xxx.extend(x1);
    xxx.extend(x2);

    let method_ident = quote::format_ident!("{}", method);

    let stream = quote! {
        #[allow(non_camel_case_types, missing_docs)]
        pub struct #name;
        impl #name {
            #xxx

            fn register(router: &mut HttpRouter) {
                #ast
                router.insert(Method::#method_ident, #path, HttpRouteHandler::new(#name));

                #(#ins;)*
            }
        }
    };
    Ok(stream.into())
}

#[derive(Clone, Debug, Deserialize)]
enum MapEntry {
    Value(String),
    Struct(HashMap<String, MapEntry>),
    Array(Vec<MapEntry>),
}

#[cfg(test)]
mod tests {
    use quote::quote;

    #[test]
    #[should_panic(
        expected = r#"expected an identifier, but found `"potato"`"#
    )]
    fn bad_ident() {
        let _ = super::to_map(
            &quote! {
                "potato" = potato
            }
            .into(),
        );
    }

    #[test]
    #[should_panic(expected = "expected `=` following `howdy`")]
    fn just_ident() {
        let _ = super::to_map(
            &quote! {
                howdy
            }
            .into(),
        );
    }

    #[test]
    #[should_panic(expected = "expected `=`, but found `there`")]
    fn no_equals() {
        let _ = super::to_map(
            &quote! {
                hi there
            }
            .into(),
        );
    }

    #[test]
    #[should_panic(expected = "parentheses not allowed")]
    fn paren_grouping() {
        let _ = super::to_map(
            &quote! {
                hi = (a, b, c)
            }
            .into(),
        );
    }

    #[test]
    #[should_panic(expected = "expected a value following `=`")]
    fn no_value() {
        let _ = super::to_map(
            &quote! {
                x =
            }
            .into(),
        );
    }

    #[test]
    fn simple() {
        let m = super::to_map(
            &quote! {
                hi = there
            }
            .into(),
        );

        let hi = m.get("hi");
        assert!(hi.is_some());

        match hi.unwrap() {
            crate::MapEntry::Value(s) => assert_eq!(s, "there"),
            _ => panic!("unexpected value"),
        }
    }

    #[test]
    fn simple2() {
        let m = super::to_map(
            &quote! {
                message = "hi there"
            }
            .into(),
        );

        let message = m.get("message");
        assert!(message.is_some());

        match message.unwrap() {
            crate::MapEntry::Value(s) => assert_eq!(s, r#"hi there"#),
            _ => panic!("unexpected value"),
        }
    }

    #[test]
    fn trailing_comma() {
        let m = super::to_map(
            &quote! {
                hi = there,
            }
            .into(),
        );

        let hi = m.get("hi");
        assert!(hi.is_some());

        match hi.unwrap() {
            crate::MapEntry::Value(s) => assert_eq!(s, "there"),
            _ => panic!("unexpected value"),
        }
    }

    #[test]
    #[should_panic(expected = "expected an identifier, but found `,`")]
    fn double_comma() {
        let m = super::to_map(
            &quote! {
                hi = there,,
            }
            .into(),
        );

        let hi = m.get("hi");
        assert!(hi.is_some());

        match hi.unwrap() {
            crate::MapEntry::Value(s) => assert_eq!(s, "there"),
            _ => panic!("unexpected value"),
        }
    }

    #[test]
    #[should_panic(expected = "expected a value, but found `?`")]
    fn bad_value() {
        let _ = super::to_map(
            &quote! {
                wat = ?
            }
            .into(),
        );
    }

    #[test]
    #[should_panic(expected = "expected a value, but found `42`")]
    fn bad_value2() {
        let _ = super::to_map(
            &quote! {
                the_meaning_of_life_the_universe_and_everything = 42
            }
            .into(),
        );
    }

    #[test]
    #[should_panic(expected = "expected an object {...}, but found `1`")]
    fn bad_array() {
        let _ = super::to_map(
            &quote! {
                array = [1, 2, 3]
            }
            .into(),
        );
    }

    #[test]
    fn simple_array() {
        let _ = super::to_map(
            &quote! {
                array = []
            }
            .into(),
        );
    }

    #[test]
    fn simple_array2() {
        let _ = super::to_map(
            &quote! {
                array = [{}, {}]
            }
            .into(),
        );
    }

    #[test]
    fn simple_array3() {
        let _ = super::to_map(
            &quote! {
                array = [{}, {},]
            }
            .into(),
        );
    }

    #[test]
    #[should_panic(expected = "expected `,`, but found `<`")]
    fn bad_array2() {
        let _ = super::to_map(
            &quote! {
                array = [{}<-]
            }
            .into(),
        );
    }
    /*
    #[test]
    fn bad_bad_bad() {
        use super::MapEntry;
        use std::collections::HashMap;
        let _: super::Result<HashMap<String, MapEntry>> =
            super::serde_tokenstream::from_tokenstream(
                &quote! {
                    array = [{}<-]
                }
                .into(),
            );
    }
    */
}
