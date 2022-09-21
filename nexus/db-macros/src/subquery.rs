// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Procedure macro for deriving subquery-related information.

use super::NameValue;

use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{DeriveInput, Error};

/// Looks for a Meta-style attribute with a particular identifier.
///
/// As an example, for an attribute like `#[subquery(foo = bar)]`, we can find this
/// attribute by calling `get_subquery_attr(&item.attrs, "foo")`.
fn get_subquery_attr(
    attrs: &[syn::Attribute],
    name: &str,
) -> Option<NameValue> {
    attrs
        .iter()
        .filter(|attr| attr.path.is_ident("subquery"))
        .filter_map(|attr| attr.parse_args::<NameValue>().ok())
        .find(|nv| nv.name.is_ident(name))
}

// Implementation of `#[derive(Subquery)]`
pub(crate) fn derive_impl(tokens: TokenStream) -> syn::Result<TokenStream> {
    let item = syn::parse2::<DeriveInput>(tokens)?;
    let name = &item.ident;

    let subquery_nv = get_subquery_attr(&item.attrs, "name").ok_or_else(|| {
        Error::new(
            item.span(),
            format!(
                "Resource needs 'name' attribute.\n\
                     Try adding #[subquery(name = your_subquery_module)] to {}.",
                name
            ),
        )
    })?;

    // TODO: We should ensure that a field named "query" exists within this
    // struct. We currently rely on it existing.
    //
    // Don't bother parsing type, but we use it when impl'ing Subquery.

    let as_query_source_impl =
        build_query_source_impl(name, &subquery_nv.value);
    let subquery_impl = build_subquery_impl(name, &subquery_nv.value);

    Ok(quote! {
        #as_query_source_impl
        #subquery_impl
    })
}

// TODO: Should we use diesel's "QuerySource" and "AsQuery" here?
//
// I think that could work for most "select" queries, but might break joins.
fn build_query_source_impl(
    name: &syn::Ident,
    subquery_module: &syn::Path,
) -> TokenStream {
    quote! {
        impl crate::db::subquery::AsQuerySource for #name {
            type QuerySource = #subquery_module::table;
            fn query_source(&self) -> Self::QuerySource {
                #subquery_module::table
            }
        }
    }
}

fn build_subquery_impl(
    name: &syn::Ident,
    subquery_module: &syn::Path,
) -> TokenStream {
    quote! {
        impl ::diesel::query_builder::QueryFragment<::diesel::pg::Pg> for #name {
            fn walk_ast<'a>(
                &'a self,
                mut out: ::diesel::query_builder::AstPass<'_, 'a, ::diesel::pg::Pg>
            ) -> ::diesel::QueryResult<()> {
                #subquery_module::table.walk_ast(out)?;
                Ok(())
            }
        }

        impl crate::db::subquery::Subquery for #name {
            fn query(&self) -> &dyn ::diesel::query_builder::QueryFragment<::diesel::pg::Pg> {
                &self.query
            }
        }
    }
}
