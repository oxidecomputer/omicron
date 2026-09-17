// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `#[derive(Unparse)]` for `clap` subcommand enums and argument structs.
//!
//! The macro reads the same item `clap`'s derives read, and the little it
//! needs to know it gets from two places. From the tokens: each variant's
//! subcommand name (`#[command(name = ...)]`, `#[command(rename_all = ...)]`
//! on the enum or the variant, or clap's default kebab-case of the variant
//! name), which variants and fields are `#[command(flatten)]` or
//! `#[command(subcommand)]`, which fields are `#[arg(skip)]` or renamed
//! with `#[arg(id = ...)]`, and whether a field is a `bool`, an
//! `Option<T>`, or a `Vec<T>`, by the same syntactic test `clap_derive`
//! uses. The legacy `#[clap(...)]` and `#[structopt(...)]` spellings are
//! read as well. Everything else about an argument (positional or not, its
//! `--long` name, its value delimiter, its action, whether it needs `--`)
//! is read at run time from the `clap::Command` the type itself builds, so
//! this macro never re-derives clap's `#[arg]` rules.
//!
//! The deriving type must itself derive clap's `Subcommand` or `Args`, and
//! a `#[command(flatten)]` field's type must derive `Unparse` too, since
//! the generated code asks it to add its arguments to the parent's. A
//! generic value type is not supported: the generated code has to know at
//! compile time whether a value is a `ValueEnum`.
//!
//! See `omicron_clap_unparse` for the trait and what the generated code
//! calls.

use heck::{
    ToKebabCase, ToLowerCamelCase, ToShoutySnakeCase, ToSnakeCase,
    ToUpperCamelCase,
};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::ext::IdentExt;
use syn::meta::ParseNestedMeta;
use syn::spanned::Spanned;
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, Field, Fields, GenericArgument,
    LitStr, Path, PathArguments, PathSegment, Result, Type, TypePath, Variant,
    parse_macro_input,
};

#[proc_macro_derive(Unparse)]
pub fn derive_unparse(
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input).unwrap_or_else(Error::into_compile_error).into()
}

fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let name = &input.ident;
    let mut generics = input.generics.clone();
    for param in generics.type_params_mut() {
        param.bounds.push(syn::parse_quote!(::omicron_clap_unparse::Unparse));
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let prelude = quote! {
        use ::omicron_clap_unparse::__private as __p;
        #[allow(unused_imports)]
        use ::omicron_clap_unparse::__private::{
            LeafViaDisplay as _, LeafViaValueEnum as _, Unparse as _,
            UnparseArgs as _,
        };
    };

    match &input.data {
        Data::Enum(data) => {
            let casing = command_attrs(&input.attrs)?.rename_all;
            let arms = data
                .variants
                .iter()
                .map(|variant| variant_arm(variant, casing))
                .collect::<Result<Vec<_>>>()?;
            let body = if arms.is_empty() {
                quote! { match *self {} }
            } else {
                quote! { match self { #(#arms)* } }
            };
            Ok(quote! {
                #[automatically_derived]
                impl #impl_generics ::omicron_clap_unparse::Unparse
                    for #name #ty_generics #where_clause
                {
                    fn unparse(&self, argv: &mut ::omicron_clap_unparse::Argv) {
                        #prelude
                        #body
                    }
                }
            })
        }
        Data::Struct(data) => {
            let emits = match &data.fields {
                Fields::Named(fields) => fields
                    .named
                    .iter()
                    .map(|field| {
                        let name = field.ident.as_ref().expect("named");
                        field_emit(field, &quote! { &self.#name })
                    })
                    .collect::<Result<Vec<_>>>()?,
                Fields::Unit => Vec::new(),
                Fields::Unnamed(_) => {
                    return Err(Error::new(
                        input.span(),
                        "Unparse: a tuple struct is not a clap `Args`",
                    ));
                }
            };
            // An `Args` struct gets both: `UnparseArgs` emits its fields
            // into whatever level it is flattened into, and `Unparse` is
            // that on a level of its own.
            Ok(quote! {
                #[automatically_derived]
                impl #impl_generics ::omicron_clap_unparse::__private::UnparseArgs
                    for #name #ty_generics #where_clause
                {
                    fn unparse_args(
                        &self,
                        __cmd: &::omicron_clap_unparse::__private::clap::Command,
                        __level: &mut ::omicron_clap_unparse::__private::Level,
                    ) {
                        #prelude
                        #(#emits)*
                    }
                }

                #[automatically_derived]
                impl #impl_generics ::omicron_clap_unparse::Unparse
                    for #name #ty_generics #where_clause
                {
                    fn unparse(&self, argv: &mut ::omicron_clap_unparse::Argv) {
                        #prelude
                        let __cmd = __p::args::<Self>();
                        let mut __level = __p::Level::new();
                        __p::UnparseArgs::unparse_args(self, &__cmd, &mut __level);
                        __level.finish(argv);
                    }
                }
            })
        }
        Data::Union(_) => {
            Err(Error::new(input.span(), "Unparse: unions are not supported"))
        }
    }
}

/// One `match` arm, unparsing one variant.
fn variant_arm(
    variant: &Variant,
    casing: Option<Casing>,
) -> Result<TokenStream> {
    let attrs = command_attrs(&variant.attrs)?;
    if attrs.skip {
        return Err(Error::new(
            variant.span(),
            "Unparse: a `#[command(skip)]` variant can never have been parsed",
        ));
    }
    if attrs.external_subcommand {
        return Err(Error::new(
            variant.span(),
            "Unparse: `external_subcommand` is not supported",
        ));
    }
    let ident = &variant.ident;
    let name = match attrs.name {
        Some(name) => name,
        None => attrs
            .rename_all
            .or(casing)
            .unwrap_or(Casing::Kebab)
            .apply(&ident.unraw().to_string()),
    };

    match &variant.fields {
        Fields::Unit => Ok(quote! {
            Self::#ident => { argv.word(#name); }
        }),
        Fields::Unnamed(fields) => {
            if fields.unnamed.len() != 1 {
                return Err(Error::new(
                    variant.span(),
                    "Unparse: a tuple variant must have exactly one field",
                ));
            }
            let arm = if attrs.flatten {
                quote! {
                    Self::#ident(inner) => { __p::Unparse::unparse(inner, argv); }
                }
            } else {
                quote! {
                    Self::#ident(inner) => {
                        argv.word(#name);
                        __p::Unparse::unparse(inner, argv);
                    }
                }
            };
            Ok(arm)
        }
        Fields::Named(fields) => {
            // Fields are bound under prefixed names so that a field called
            // `argv` or `inner` cannot shadow the generated code's own.
            let bindings = fields
                .named
                .iter()
                .map(|field| {
                    let name = field.ident.as_ref().expect("named");
                    let binding = format_ident!("__f_{}", name.unraw());
                    quote! { #name: #binding }
                })
                .collect::<Vec<_>>();
            let emits = fields
                .named
                .iter()
                .map(|field| {
                    let name = field.ident.as_ref().expect("named");
                    let binding = format_ident!("__f_{}", name.unraw());
                    field_emit(field, &quote! { #binding })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(quote! {
                Self::#ident { #(#bindings),* } => {
                    argv.word(#name);
                    let __cmd = __p::subcommand::<Self>(#name);
                    let mut __owned = __p::Level::new();
                    let __level = &mut __owned;
                    #(#emits)*
                    __owned.finish(argv);
                }
            })
        }
    }
}

/// Code that emits one field into `__level`, given an expression for a
/// reference to the field.
fn field_emit(field: &Field, value: &TokenStream) -> Result<TokenStream> {
    let attrs = command_attrs(&field.attrs)?;
    let arg = arg_attrs(&field.attrs)?;
    if arg.skip {
        return Ok(quote! {});
    }
    if attrs.flatten {
        // A flattened `Args` struct's arguments live in this level.
        return Ok(match Ty::of(&field.ty) {
            Ty::Option(_) => quote! {
                if let Some(inner) = #value {
                    __p::UnparseArgs::unparse_args(inner, &__cmd, __level);
                }
            },
            _ => quote! {
                __p::UnparseArgs::unparse_args(#value, &__cmd, __level);
            },
        });
    }
    if attrs.subcommand {
        return Ok(match Ty::of(&field.ty) {
            Ty::Option(_) => quote! {
                if let Some(inner) = #value { __level.subcommand(inner); }
            },
            _ => quote! { __level.subcommand(#value); },
        });
    }
    let id = match arg.id {
        Some(id) => id,
        None => field.ident.as_ref().expect("named").unraw().to_string(),
    };
    let meta = quote! { &__p::arg_meta(&__cmd, #id) };
    Ok(match Ty::of(&field.ty) {
        Ty::Bool => quote! { __p::emit_bool(__level, #meta, *#value); },
        Ty::Option(inner) => {
            let leaf = leaf(&inner, &quote! { v });
            quote! {
                __p::emit_optional(
                    __level, #meta, (#value).as_ref().map(|v| #leaf),
                );
            }
        }
        Ty::Vec(inner) => {
            let leaf = leaf(&inner, &quote! { v });
            quote! {
                __p::emit_many(
                    __level, #meta, (#value).iter().map(|v| #leaf).collect(),
                );
            }
        }
        Ty::Other(ty) => {
            let leaf = leaf(&ty, value);
            quote! { __p::emit_one(__level, #meta, #leaf); }
        }
        Ty::Unsupported(what) => {
            return Err(Error::new(
                field.ty.span(),
                format!("Unparse: `{what}` fields are not supported"),
            ));
        }
    })
}

/// The text of one value: clap's name for a `ValueEnum`, else its `Display`.
fn leaf(ty: &Type, value: &TokenStream) -> TokenStream {
    quote! { (&__p::Leaf::<#ty>::new()).leaf(#value) }
}

/// The shapes of a field's type that change how it is emitted, decided the
/// way `clap_derive` decides them: by the last path segment.
enum Ty {
    Bool,
    Option(Type),
    Vec(Type),
    Other(Type),
    Unsupported(&'static str),
}

impl Ty {
    fn of(ty: &Type) -> Self {
        if is_simple(ty, "bool") {
            Self::Bool
        } else if let Some(inner) = sub_type(ty, "Vec") {
            if sub_type(inner, "Vec").is_some() {
                Self::Unsupported("Vec<Vec<T>>")
            } else {
                Self::Vec(inner.clone())
            }
        } else if let Some(inner) = sub_type(ty, "Option") {
            if sub_type(inner, "Option").is_some() {
                Self::Unsupported("Option<Option<T>>")
            } else if sub_type(inner, "Vec").is_some() {
                Self::Unsupported("Option<Vec<T>>")
            } else {
                Self::Option(inner.clone())
            }
        } else {
            Self::Other(ty.clone())
        }
    }
}

fn last_segment(ty: &Type) -> Option<&PathSegment> {
    match ty {
        Type::Path(TypePath {
            qself: None,
            path: Path { leading_colon: None, segments },
        }) if segments.len() == 1 => segments.first(),
        _ => None,
    }
}

fn is_simple(ty: &Type, name: &str) -> bool {
    last_segment(ty)
        .is_some_and(|seg| seg.ident == name && seg.arguments.is_none())
}

/// `T` in `name<T>`.
fn sub_type<'a>(ty: &'a Type, name: &str) -> Option<&'a Type> {
    let seg = last_segment(ty).filter(|seg| seg.ident == name)?;
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    match args.args.first() {
        Some(GenericArgument::Type(inner)) if args.args.len() == 1 => {
            Some(inner)
        }
        _ => None,
    }
}

/// What this macro reads from `#[command(...)]`.
#[derive(Default)]
struct CommandAttrs {
    name: Option<String>,
    rename_all: Option<Casing>,
    flatten: bool,
    subcommand: bool,
    skip: bool,
    external_subcommand: bool,
}

fn command_attrs(attrs: &[Attribute]) -> Result<CommandAttrs> {
    let mut out = CommandAttrs::default();
    for attr in attrs
        .iter()
        .filter(|attr| is_any(attr, &["command", "clap", "structopt"]))
    {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                out.name = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("rename_all") {
                let lit = meta.value()?.parse::<LitStr>()?;
                out.rename_all = Some(Casing::parse(&lit)?);
            } else if meta.path.is_ident("flatten") {
                out.flatten = true;
            } else if meta.path.is_ident("subcommand") {
                out.subcommand = true;
            } else if meta.path.is_ident("skip") {
                out.skip = true;
                skip_value(&meta)?;
            } else if meta.path.is_ident("external_subcommand") {
                out.external_subcommand = true;
            } else {
                skip_value(&meta)?;
            }
            Ok(())
        })?;
    }
    Ok(out)
}

/// What this macro reads from `#[arg(...)]`.
#[derive(Default)]
struct ArgAttrs {
    id: Option<String>,
    skip: bool,
}

fn arg_attrs(attrs: &[Attribute]) -> Result<ArgAttrs> {
    let mut out = ArgAttrs::default();
    for attr in
        attrs.iter().filter(|attr| is_any(attr, &["arg", "clap", "structopt"]))
    {
        attr.parse_nested_meta(|meta| {
            // `name` is clap's older spelling of `id`.
            if meta.path.is_ident("id") || meta.path.is_ident("name") {
                out.id = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("skip") {
                out.skip = true;
                skip_value(&meta)?;
            } else {
                skip_value(&meta)?;
            }
            Ok(())
        })?;
    }
    Ok(out)
}

fn is_any(attr: &Attribute, names: &[&str]) -> bool {
    names.iter().any(|name| attr.path().is_ident(name))
}

/// Consumes an attribute argument's `= value` or `(...)`, if it has one, so
/// that arguments this macro does not read are passed over rather than
/// rejected.
fn skip_value(meta: &ParseNestedMeta<'_>) -> Result<()> {
    if meta.input.peek(syn::Token![=]) {
        meta.value()?.parse::<Expr>()?;
    } else if meta.input.peek(syn::token::Paren) {
        let content;
        syn::parenthesized!(content in meta.input);
        content.parse::<TokenStream>()?;
    }
    Ok(())
}

/// `clap`'s `rename_all` casings, applied as `clap_derive` applies them.
#[derive(Clone, Copy)]
enum Casing {
    Camel,
    Kebab,
    Pascal,
    ScreamingSnake,
    Snake,
    Lower,
    Upper,
    Verbatim,
}

impl Casing {
    fn parse(lit: &LitStr) -> Result<Self> {
        let normalized = lit.value().to_upper_camel_case().to_lowercase();
        Ok(match normalized.as_str() {
            "camel" | "camelcase" => Self::Camel,
            "kebab" | "kebabcase" => Self::Kebab,
            "pascal" | "pascalcase" => Self::Pascal,
            "screamingsnake" | "screamingsnakecase" => Self::ScreamingSnake,
            "snake" | "snakecase" => Self::Snake,
            "lower" | "lowercase" => Self::Lower,
            "upper" | "uppercase" => Self::Upper,
            "verbatim" | "verbatimcase" => Self::Verbatim,
            other => {
                return Err(Error::new(
                    lit.span(),
                    format!("unsupported casing: `{other}`"),
                ));
            }
        })
    }

    fn apply(self, s: &str) -> String {
        match self {
            Self::Pascal => s.to_upper_camel_case(),
            Self::Kebab => s.to_kebab_case(),
            Self::Camel => s.to_lower_camel_case(),
            Self::ScreamingSnake => s.to_shouty_snake_case(),
            Self::Snake => s.to_snake_case(),
            Self::Lower => s.to_snake_case().replace('_', ""),
            Self::Upper => s.to_shouty_snake_case().replace('_', ""),
            Self::Verbatim => s.to_owned(),
        }
    }
}
