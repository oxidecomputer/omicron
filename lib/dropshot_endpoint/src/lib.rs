//! This macro defines attributes associated with HTTP handlers. These
//! attributes are used both to define an HTTP API and to generate an OpenAPI
//! Spec (OAS) v3 document that describes the API.

extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::quote;
use serde::Deserialize;
use serde_derive_internals::ast::Container;
use serde_derive_internals::{Ctxt, Derive};
use syn::{parse_macro_input, DeriveInput, ItemFn};

use serde_tokenstream::from_tokenstream;
use serde_tokenstream::Error;

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

    let ast: ItemFn = syn::parse(item)?;

    let name = &ast.sig.ident;
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

                Endpoint::new(#name, Method::#method_ident, #path)
            }
        }
    };

    Ok(stream.into())
}

#[proc_macro_derive(ExtractorParameter)]
pub fn derive_parameter(
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    let ctxt = Ctxt::new();

    let cont = Container::from_ast(&ctxt, &input, Derive::Deserialize).unwrap();
    // TODO error checking
    let _ = ctxt.check();

    let fields = cont
        .data
        .all_fields()
        .map(|f| match &f.member {
            syn::Member::Named(ident) => {
                let name = ident.to_string();
                quote! {
                    dropshot::EndpointParameter {
                        name: #name.to_string(),
                        inn: _in.clone(),
                        description: None,
                        required: true, // TODO look at option
                        examples: vec![],
                    }
                }
            }
            _ => quote! {},
        })
        .collect::<Vec<_>>();

    // Construct the appropriate where clause.
    let name = cont.ident;
    let mut generics = cont.generics.clone();

    for tp in cont.generics.type_params() {
        let ident = &tp.ident;
        let pred: syn::WherePredicate = syn::parse2(quote! {
            #ident : dropshot::ExtractorParameter
        })
        .unwrap();
        generics.make_where_clause().predicates.push(pred);
    }

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let stream = quote! {
        impl #impl_generics dropshot::ExtractorParameter for #name #ty_generics
        #where_clause
        {
            fn generate(_in: dropshot::EndpointParameterLocation)
                -> Vec<dropshot::EndpointParameter>
            {
                vec![ #(#fields),* ]
            }
        }
    };

    stream.into()
}

fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
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
                    println!("  s {}", s.value());
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
            /// this looks like a "comment", but it's actually part of the test
            /// this too
            #[doc = "testing"]
            #[doc(hidden)]
            fn do_nothing() {}
        };

        let ast: syn::ItemFn = syn::parse2(item).unwrap();

        let _ = extract_doc(&ast);

        panic!("bad");
    }

    use schemars::{schema_for, JsonSchema};
    use std::marker::PhantomData;

    #[derive(JsonSchema)]
    struct Foo {
        a: String,
        b: String,
    }

    struct Bar {
        a: String,
    }

    fn x<T>()
    where
        T: JsonSchema,
    {
        let schema = schema_for!(T);
        println!("{:?}", schema);
    }

    #[test]
    fn thing() {
        x::<Foo>();
        panic!("bad");
    }
}
