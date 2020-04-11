//! This macro defines attributes associated with HTTP handlers. These
//! attributes are used both to define an HTTP API and to generate an OpenAPI
//! Spec (OAS) v3 document that describes the API.

extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use quote::ToTokens;
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
    _dropshot_crate: Option<String>,
}

const DROPSHOT: &str = "dropshot";

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
    let method_ident = format_ident!("{}", method);

    let description = extract_doc_from_attrs(&ast.attrs).map(|s| {
        quote! {
            endpoint.description = Some(#s.to_string());
        }
    });

    let dropshot = get_crate(metadata._dropshot_crate);

    // The final TokenStream returned will have a few components that reference
    // `#name`, the name of the method to which this macro was applied...
    let stream = quote! {
        // ... a struct type called `#name` that has no members
        #[allow(non_camel_case_types, missing_docs)]
        pub struct #name {}
        // ... a constant of type `#name` whose identifier is also #name
        #[allow(non_upper_case_globals, missing_docs)]
        const #name: #name = #name {};

        // ... an impl of `From<#name>` for ApiEndpoint that allows the constant
        // `#name` to be passed into `ApiDescription::register()`
        impl From<#name> for #dropshot::ApiEndpoint {
            fn from(_: #name) -> Self {
                #ast

                #[allow(unused_mut)]
                let mut endpoint = #dropshot::ApiEndpoint::new(
                    #name,
                    Method::#method_ident,
                    #path,
                );
                #description
                endpoint
            }
        }
    };

    Ok(stream.into())
}

#[proc_macro_derive(ExtractedParameter, attributes(dropshot))]
pub fn derive_parameter(
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    let ctxt = Ctxt::new();

    let cont = Container::from_ast(&ctxt, &input, Derive::Deserialize).unwrap();
    // TODO error checking
    let _ = ctxt.check();

    let dropshot = get_crate(get_crate_attr(&cont));

    let fields = cont
        .data
        .all_fields()
        .map(|f| {
            match &f.member {
                syn::Member::Named(ident) => {
                    let doc = extract_doc_from_attrs(&f.original.attrs)
                        .map_or_else(
                            || quote! { None },
                            |s| quote! { Some(#s.to_string()) },
                        );
                    let name = ident.to_string();
                    quote! {
                        #dropshot::ApiEndpointParameter {
                            name: #name.to_string(),
                            inn: _in.clone(),
                            description: #doc ,
                            required: true, // TODO look for Option type
                            examples: vec![],
                        }
                    }
                }
                _ => quote! {},
            }
        })
        .collect::<Vec<_>>();

    // Construct the appropriate where clause.
    let name = cont.ident;
    let mut generics = cont.generics.clone();

    for tp in cont.generics.type_params() {
        let ident = &tp.ident;
        let pred: syn::WherePredicate = syn::parse2(quote! {
            #ident : #dropshot::ExtractedParameter
        })
        .unwrap();
        generics.make_where_clause().predicates.push(pred);
    }

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let stream = quote! {
        impl #impl_generics #dropshot::ExtractedParameter for #name #ty_generics
        #where_clause
        {
            fn generate(_in: #dropshot::ApiEndpointParameterLocation)
                -> Vec<#dropshot::ApiEndpointParameter>
            {
                vec![ #(#fields),* ]
            }
        }
    };

    stream.into()
}

fn get_crate(var: Option<String>) -> TokenStream {
    if let Some(s) = var {
        if let Ok(ts) = syn::parse_str(s.as_str()) {
            return ts;
        }
    }
    syn::Ident::new(DROPSHOT, proc_macro2::Span::call_site()).to_token_stream()
}

fn get_crate_attr(
    cont: &serde_derive_internals::ast::Container,
) -> Option<String> {
    cont.original
        .attrs
        .iter()
        .filter_map(|attr| {
            if let Ok(meta) = attr.parse_meta() {
                if let syn::Meta::List(list) = meta {
                    if list.path.is_ident(&syn::Ident::new(
                        "dropshot",
                        proc_macro2::Span::call_site(),
                    )) && list.nested.len() == 1
                    {
                        if let Some(syn::NestedMeta::Meta(
                            syn::Meta::NameValue(nv),
                        )) = list.nested.first()
                        {
                            if nv.path.is_ident(&syn::Ident::new(
                                "crate",
                                proc_macro2::Span::call_site(),
                            )) {
                                if let syn::Lit::Str(s) = &nv.lit {
                                    return Some(s.value());
                                }
                            }
                        }
                    }
                }
            }
            None
        })
        .last()
}

#[allow(dead_code)]
fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

fn extract_doc_from_attrs(attrs: &Vec<syn::Attribute>) -> Option<String> {
    let doc = syn::Ident::new("doc", proc_macro2::Span::call_site());

    attrs
        .iter()
        .filter_map(|attr| {
            if let Ok(meta) = attr.parse_meta() {
                if let syn::Meta::NameValue(nv) = meta {
                    if nv.path.is_ident(&doc) {
                        if let syn::Lit::Str(s) = nv.lit {
                            let comment = s.value();
                            if comment.starts_with(" ")
                                && !comment.starts_with("  ")
                            {
                                // Trim off the first character if the comment
                                // begins with a single space.
                                return Some(format!(
                                    "{}",
                                    &comment.as_str()[1..]
                                ));
                            } else {
                                return Some(comment);
                            }
                        }
                    }
                }
            }
            None
        })
        .fold(None, |acc, comment| {
            Some(format!("{}{}", acc.unwrap_or(String::new()), comment))
        })
}

#[cfg(test)]
mod tests {}
