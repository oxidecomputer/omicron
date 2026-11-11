// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Code shared by Nexus macros.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse_quote;

/// The type of a primary key in a database.
#[derive(Debug)]
pub enum PrimaryKeyType {
    /// A regular type.
    Standard(Box<syn::Type>),

    /// A typed UUID, which requires special handling.
    TypedUuid {
        /// The external type. This is used almost everywhere.
        external: Box<syn::Type>,

        /// The internal type used in nexus-db-model.
        db: Box<syn::Type>,
    },
}

impl PrimaryKeyType {
    /// Constructs a new `TypedUuid` variant.
    pub fn new_typed_uuid(kind: &syn::Ident) -> Self {
        let external = parse_quote!(::omicron_uuid_kinds::TypedUuid<::omicron_uuid_kinds::#kind>);
        let db = parse_quote!(crate::typed_uuid::DbTypedUuid<::omicron_uuid_kinds::#kind>);
        PrimaryKeyType::TypedUuid { external, db }
    }

    /// Returns the external type for this primary key.
    pub fn external(&self) -> &syn::Type {
        match self {
            PrimaryKeyType::Standard(path) => path,
            PrimaryKeyType::TypedUuid { external, .. } => external,
        }
    }

    /// Converts self into the external type.
    pub fn into_external(self) -> syn::Type {
        match self {
            PrimaryKeyType::Standard(path) => *path,
            PrimaryKeyType::TypedUuid { external, .. } => *external,
        }
    }

    /// Returns the database type for this primary key.
    ///
    /// For the `TypedUuid` variant, the db type is only accessible within the
    /// `nexus-db-model` crate.
    pub fn db(&self) -> &syn::Type {
        match self {
            PrimaryKeyType::Standard(path) => path,
            PrimaryKeyType::TypedUuid { db, .. } => db,
        }
    }

    /// Converts self into the database type.
    pub fn into_db(self) -> syn::Type {
        match self {
            PrimaryKeyType::Standard(path) => *path,
            PrimaryKeyType::TypedUuid { db, .. } => *db,
        }
    }

    /// Returns tokens for a conversion from external to db types, given an
    /// expression as input.
    ///
    /// This is specialized for the nexus-db-model crate.
    pub fn external_to_db_nexus_db_model(
        &self,
        tokens: TokenStream,
    ) -> TokenStream {
        match self {
            PrimaryKeyType::Standard(_) => tokens,
            PrimaryKeyType::TypedUuid { .. } => {
                quote! { crate::to_db_typed_uuid(#tokens) }
            }
        }
    }

    /// Returns tokens for a conversion from external to db types, given an
    /// expression as input.
    ///
    /// This is used for all crates *except* nexus-db-model.
    pub fn external_to_db_other(&self, tokens: TokenStream) -> TokenStream {
        match self {
            PrimaryKeyType::Standard(_) => tokens,
            PrimaryKeyType::TypedUuid { .. } => {
                quote! { ::nexus_db_model::to_db_typed_uuid(#tokens) }
            }
        }
    }

    /// Returns tokens for a conversion from db to external types, given an
    /// expression as input.
    pub fn db_to_external(&self, tokens: TokenStream) -> TokenStream {
        match self {
            PrimaryKeyType::Standard(_) => tokens,
            PrimaryKeyType::TypedUuid { .. } => {
                quote! { ::omicron_uuid_kinds::TypedUuid::from(#tokens) }
            }
        }
    }
}
