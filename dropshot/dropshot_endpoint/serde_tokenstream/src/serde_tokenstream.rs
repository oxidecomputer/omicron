use core::iter::Peekable;
use std::fmt::{self, Display};

use proc_macro2::{Delimiter, Group, TokenStream, TokenTree};
use serde::de::{
    DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
};
use serde::{Deserialize, Deserializer};
use syn::{ExprLit, Lit};

// print out the raw tree of TokenStream structures.
#[allow(dead_code)]
fn treeify(depth: usize, tt: &TokenStream) {
    for i in tt.clone().into_iter() {
        match i {
            proc_macro2::TokenTree::Group(group) => {
                println!(
                    "{:width$}group {}",
                    "",
                    match group.delimiter() {
                        Delimiter::Parenthesis => "()",
                        Delimiter::Brace => "{}",
                        Delimiter::Bracket => "[]",
                        Delimiter::None => "none",
                    },
                    width = depth * 2
                );
                treeify(depth + 1, &group.stream())
            }
            _ => println!("{:width$}{:?}", "", i, width = depth * 2),
        }
    }
}

pub type Error = syn::Error;
pub type Result<T> = std::result::Result<T, Error>;

pub fn from_tokenstream<'a, T>(tokens: &'a TokenStream) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = TokenDe::from_tokenstream(tokens);
    match T::deserialize(&mut deserializer) {
        // On success, check that there aren't additional, unparsed tokens.
        Ok(result) => match deserializer.next() {
            None => Ok(result),
            Some(token) => Err(Error::new(
                token.span(),
                format!("expected EOF but found `{}`", token),
            )),
        },
        // Pass through expected errors.
        Err(InternalError::Normal(err)) => Err(err),

        // Other errors should not be able to reach this point.
        Err(InternalError::NoData(msg)) => panic!(
            "Error::NoData should never propagate to the caller: {}",
            msg
        ),
        Err(InternalError::Unknown) => {
            panic!("Error::Unknown should never propagate to the caller")
        }
    }
}

#[derive(Clone, Debug)]
enum InternalError {
    Normal(Error),
    NoData(String),
    Unknown,
}

type InternalResult<T> = std::result::Result<T, InternalError>;

struct TokenDe {
    input: Peekable<Box<dyn Iterator<Item = TokenTree>>>,
    current: Option<TokenTree>,
    last: Option<TokenTree>,
}

impl<'de> TokenDe {
    fn from_tokenstream(input: &'de TokenStream) -> Self {
        // We implicitly start inside a brace-surrounded struct.
        // Constructing a Group allows for more generic handling elsewhere.
        TokenDe::new(&TokenStream::from(TokenTree::from(Group::new(
            Delimiter::Brace,
            input.clone(),
        ))))
    }

    fn new(input: &'de TokenStream) -> Self {
        let t: Box<dyn Iterator<Item = TokenTree>> =
            Box::new(input.clone().into_iter());
        TokenDe {
            input: t.peekable(),
            current: None,
            last: None,
        }
    }

    fn gobble_optional_comma(&mut self) -> InternalResult<()> {
        match self.next() {
            None => Ok(()),
            Some(TokenTree::Punct(punct)) if punct.as_char() == ',' => Ok(()),
            Some(token) => Err(InternalError::Normal(Error::new(
                token.span(),
                format!("expected `,` or nothing, but found `{}`", token),
            ))),
        }
    }

    fn next(&mut self) -> Option<TokenTree> {
        let next = self.input.next();

        self.last = std::mem::replace(
            &mut self.current,
            next.as_ref().map(|t| t.clone()),
        );
        next
    }

    fn last_err<T>(&self, what: &str) -> InternalResult<T> {
        match &self.last {
            Some(token) => Err(InternalError::Normal(Error::new(
                token.span(),
                format!("expected {} following `{}`", what, token),
            ))),
            // It should not be possible to reach this point. Although
            // `self.last` starts as `None`, the first thing we'll try to do
            // is deserialize a structure type based on the `Group` we create
            // in `::from_tokenstream`.
            None => Err(InternalError::Unknown),
        }
    }

    fn deserialize_error<VV>(
        &self,
        next: Option<TokenTree>,
        what: &str,
    ) -> InternalResult<VV> {
        match next {
            Some(token) => Err(InternalError::Normal(Error::new(
                token.span(),
                format!("expected {}, but found `{}`", what, token),
            ))),
            None => self.last_err(what),
        }
    }

    fn non_unit_variant<VV>(&self) -> InternalResult<VV> {
        match &self.current {
            Some(token) => Err(InternalError::Normal(Error::new(
                token.span(),
                "non-unit variants are not currently supported",
            ))),
            // This can't happen; we will need to have read a token at
            // this point.j
            None => Err(InternalError::Unknown),
        }
    }

    fn deserialize_int<T, VV, F>(&mut self, visit: F) -> InternalResult<VV>
    where
        F: FnOnce(T) -> InternalResult<VV>,
        T: std::str::FromStr,
        T::Err: Display,
    {
        let next = self.next();

        if let Some(TokenTree::Literal(literal)) = &next {
            if let Ok(syn::ExprLit {
                lit: syn::Lit::Int(i), ..
            }) = syn::parse_str::<syn::ExprLit>(&literal.to_string())
            {
                if let Ok(value) = i.base10_parse::<T>() {
                    return visit(value);
                }
            }
        }

        self.deserialize_error(next, stringify!(T))
    }

    fn deserialize_float<T, VV, F>(&mut self, visit: F) -> InternalResult<VV>
    where
        F: FnOnce(T) -> InternalResult<VV>,
        T: std::str::FromStr,
        T::Err: Display,
    {
        let next = self.next();

        if let Some(TokenTree::Literal(literal)) = &next {
            if let Ok(syn::ExprLit {
                lit: syn::Lit::Float(f), ..
            }) = syn::parse_str::<syn::ExprLit>(&literal.to_string())
            {
                if let Ok(value) = f.base10_parse::<T>() {
                    return visit(value);
                }
            }
        }

        self.deserialize_error(next, stringify!(T))
    }
}

impl serde::de::Error for InternalError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        InternalError::NoData(format!("{}", msg))
    }
}
impl std::error::Error for InternalError {}

impl Display for InternalError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(format!("{:?}", self).as_str())
    }
}

impl<'de, 'a> MapAccess<'de> for TokenDe {
    type Error = InternalError;

    fn next_key_seed<K>(&mut self, seed: K) -> InternalResult<Option<K::Value>>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        let keytok = match self.input.peek() {
            None => {
                return Ok(None);
            }
            Some(token) => token.clone(),
        };

        let key = seed.deserialize(&mut *self).map(Some);

        // Verify we have an '=' delimiter.
        if key.is_ok() {
            let _eq = match self.next() {
                Some(TokenTree::Punct(punct)) if punct.as_char() == '=' => {
                    punct
                }

                Some(token) => {
                    return Err(InternalError::Normal(Error::new(
                        token.span(),
                        format!("expected `=`, but found `{}`", token),
                    )))
                }
                None => {
                    return Err(InternalError::Normal(Error::new(
                        keytok.span(),
                        format!("expected `=` following `{}`", keytok),
                    )))
                }
            };
        }

        key
    }

    fn next_value_seed<V>(&mut self, seed: V) -> InternalResult<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        let valtok = self.input.peek().map(|tok| tok.span());
        let value = seed.deserialize(&mut *self);

        match &value {
            Ok(_) => {
                self.gobble_optional_comma()?;
            }
            Err(InternalError::NoData(msg)) => match valtok {
                Some(span) => {
                    return Err(InternalError::Normal(Error::new(span, msg)));
                }
                None => (),
            },
            Err(_) => (),
        }

        value
    }
}

impl<'de, 'a> SeqAccess<'de> for TokenDe {
    type Error = InternalError;

    fn next_element_seed<T>(
        &mut self,
        seed: T,
    ) -> InternalResult<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        if self.input.peek().is_none() {
            return Ok(None);
        }
        let value = seed.deserialize(&mut *self).map(Some);
        if value.is_ok() {
            self.gobble_optional_comma()?;
        }
        value
    }
}

impl<'de, 'a> EnumAccess<'de> for &mut TokenDe {
    type Error = InternalError;
    type Variant = Self;

    fn variant_seed<V>(
        self,
        seed: V,
    ) -> InternalResult<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        let val = seed.deserialize(&mut *self);

        match val {
            Ok(v) => Ok((v, self)),
            // If there wan an error from serde, tag it with the current token.
            Err(InternalError::NoData(msg)) => match &self.current {
                Some(token) => {
                    Err(InternalError::Normal(Error::new(token.span(), msg)))
                }
                // This can't happen; we will need to have read a token at
                // this point.j
                None => Err(InternalError::Unknown),
            },
            Err(err) => Err(err),
        }
    }
}

impl<'de, 'a> VariantAccess<'de> for &mut TokenDe {
    type Error = InternalError;

    fn unit_variant(self) -> InternalResult<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, _seed: T) -> InternalResult<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        self.non_unit_variant()
    }

    fn tuple_variant<V>(
        self,
        _len: usize,
        _visitor: V,
    ) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.non_unit_variant()
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.non_unit_variant()
    }
}

/// Stub out Deserializer trait functions we don't want to deal with right now.
macro_rules! de_unimp {
    ($i:ident $(, $p:ident : $t:ty )*) => {
        fn $i<V>(self $(, $p: $t)*, _visitor: V) -> InternalResult<V::Value>
        where
            V: Visitor<'de>,
        {
            unimplemented!(stringify!($i));
        }
    };
    ($i:ident $(, $p:ident : $t:ty )* ,) => {
        de_unimp!($i $(, $p: $t)*);
    };
}

impl<'de, 'a> Deserializer<'de> for &'a mut TokenDe {
    type Error = InternalError;

    fn deserialize_bool<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.next() {
            Some(TokenTree::Ident(ident)) if ident.to_string() == "true" => {
                visitor.visit_bool(true)
            }
            Some(TokenTree::Ident(ident)) if ident.to_string() == "false" => {
                visitor.visit_bool(false)
            }
            other => self.deserialize_error(other, "bool"),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        // None is a missing field, so this must be Some.
        visitor.visit_some(self)
    }

    fn deserialize_string<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        let token = self.next();
        let value = match &token {
            Some(TokenTree::Ident(ident)) => Some(ident.to_string()),
            Some(TokenTree::Literal(lit)) => {
                match syn::parse_str::<syn::ExprLit>(&lit.to_string()) {
                    Ok(syn::ExprLit {
                        lit: syn::Lit::Str(s), ..
                    }) => Some(s.value()),
                    _ => None,
                }
            }
            _ => None,
        };

        value.map_or_else(
            || self.deserialize_error(token, "a string"),
            |v| visitor.visit_string(v),
        )
    }
    fn deserialize_str<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_seq<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        let next = self.next();

        if let Some(token) = &next {
            if let TokenTree::Group(group) = token {
                if let Delimiter::Bracket = group.delimiter() {
                    return match visitor
                        .visit_seq(TokenDe::new(&group.stream()))
                    {
                        Err(InternalError::NoData(msg)) => {
                            Err(InternalError::Normal(Error::new(
                                token.span(),
                                msg,
                            )))
                        }
                        other => other,
                    };
                }
            }
        }

        self.deserialize_error(next, "an array")
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        let next = self.next();

        if let Some(token) = &next {
            if let TokenTree::Group(group) = token {
                if let Delimiter::Brace = group.delimiter() {
                    match visitor.visit_map(TokenDe::new(&group.stream())) {
                        Err(InternalError::NoData(msg)) => {
                            return Err(InternalError::Normal(Error::new(
                                token.span(),
                                msg,
                            )))
                        }
                        other => return other,
                    }
                }
            }
        };

        self.deserialize_error(next, "a struct")
    }

    fn deserialize_map<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        let next = self.next();

        if let Some(TokenTree::Group(group)) = &next {
            if let Delimiter::Brace = group.delimiter() {
                return visitor.visit_map(TokenDe::new(&group.stream()));
            }
        }

        self.deserialize_error(next, "a map")
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        let next = self.next();

        if let Some(ident @ TokenTree::Ident(_)) = next {
            return visitor.visit_string(ident.to_string());
        }

        self.deserialize_error(next, "an identifier")
    }

    fn deserialize_char<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        let next = self.next();

        if let Some(TokenTree::Literal(literal)) = &next {
            if let Ok(syn::ExprLit {
                lit: syn::Lit::Char(ch), ..
            }) = syn::parse_str::<syn::ExprLit>(&literal.to_string())
            {
                return visitor.visit_char(ch.value());
            }
        }
        self.deserialize_error(next, "a char")
    }

    fn deserialize_unit<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        let next = self.next();

        if let Some(token) = &next {
            if let TokenTree::Group(group) = token {
                if let Delimiter::Parenthesis = group.delimiter() {
                    if group.stream().is_empty() {
                        return visitor.visit_unit();
                    }
                }
            }
        }

        self.deserialize_error(next, "a unit")
    }

    de_unimp!(deserialize_bytes);
    de_unimp!(deserialize_byte_buf);

    fn deserialize_tuple<V>(
        self,
        _len: usize,
        visitor: V,
    ) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        let next = self.next();

        if let Some(token) = &next {
            if let TokenTree::Group(group) = token {
                if let Delimiter::Parenthesis = group.delimiter() {
                    return match visitor
                        .visit_seq(TokenDe::new(&group.stream()))
                    {
                        Err(InternalError::NoData(msg)) => {
                            Err(InternalError::Normal(Error::new(
                                token.span(),
                                msg,
                            )))
                        }
                        other => other,
                    };
                }
            }
        }

        self.deserialize_error(next, "a tuple")
    }

    fn deserialize_any<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        let token = self.next();

        match &token {
            None => self.last_err("a value"),
            Some(TokenTree::Group(group)) => match group.delimiter() {
                Delimiter::Brace => {
                    visitor.visit_map(TokenDe::new(&group.stream()))
                }
                Delimiter::Bracket => {
                    visitor.visit_seq(TokenDe::new(&group.stream()))
                }
                Delimiter::Parenthesis => {
                    let stream = &group.stream();
                    if stream.is_empty() {
                        visitor.visit_unit()
                    } else {
                        visitor.visit_seq(TokenDe::new(stream))
                    }
                }
                Delimiter::None => Err(InternalError::Normal(Error::new(
                    group.span(),
                    format!("the null delimiter is not allowed"),
                ))),
            },
            Some(TokenTree::Ident(ident)) if ident.to_string() == "true" => {
                visitor.visit_bool(true)
            }
            Some(TokenTree::Ident(ident)) if ident.to_string() == "false" => {
                visitor.visit_bool(false)
            }
            Some(TokenTree::Ident(ident)) => {
                visitor.visit_string(ident.to_string())
            }
            Some(TokenTree::Literal(lit)) => {
                match syn::parse_str::<ExprLit>(&lit.to_string()) {
                    Ok(ExprLit {
                        lit: Lit::Str(s), ..
                    }) => visitor.visit_string(s.value()),
                    Ok(ExprLit {
                        lit: Lit::ByteStr(_), ..
                    }) => todo!("bytestr"),
                    Ok(ExprLit {
                        lit: Lit::Byte(_), ..
                    }) => todo!("byte"),
                    Ok(ExprLit {
                        lit: Lit::Char(ch), ..
                    }) => visitor.visit_char(ch.value()),
                    Ok(ExprLit {
                        lit: Lit::Int(i), ..
                    }) => visitor.visit_u64(i.base10_parse::<u64>().unwrap()),
                    Ok(ExprLit {
                        lit: Lit::Float(f), ..
                    }) => visitor.visit_f64(f.base10_parse::<f64>().unwrap()),
                    Ok(ExprLit {
                        lit: Lit::Bool(_), ..
                    }) => panic!("can't happen; bool is handled elsewhere"),
                    Ok(ExprLit {
                        lit: Lit::Verbatim(_), ..
                    }) => todo!("verbatim"),
                    Err(err) => panic!(
                        "can't happen; must be parseable: {} {}",
                        lit, err
                    ),
                }
            }
            Some(TokenTree::Punct(_)) => {
                self.deserialize_error(token, "a value")
            }
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_i8(value))
    }
    fn deserialize_i16<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_i16(value))
    }
    fn deserialize_i32<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_i32(value))
    }
    fn deserialize_i64<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_i64(value))
    }
    fn deserialize_u8<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_u8(value))
    }
    fn deserialize_u16<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_u16(value))
    }
    fn deserialize_u32<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_u32(value))
    }
    fn deserialize_u64<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_u64(value))
    }

    fn deserialize_f32<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_float(|value| visitor.visit_f32(value))
    }
    fn deserialize_f64<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_float(|value| visitor.visit_f64(value))
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> InternalResult<V::Value>
    where
        V: Visitor<'de>,
    {
        let mut err = match self.last.as_ref() {
            Some(token) => Error::new(
                token.span(),
                format!("extraneous member `{}`", token),
            ),
            // This can't happen -- we need to have read a token in order for
            // serde to determine that this value will be ignored.
            None => return Err(InternalError::Unknown),
        };

        // We know this is going to be an error, but parse the value anyways
        // to see if that *also* produces an error.
        //
        // TODO it would be slick to have the span cover the full range of the
        // item (i.e. not just the key), but Span::join requires nightly.
        match self.deserialize_any(visitor) {
            Err(InternalError::Normal(e2)) => err.combine(e2),
            _ => (),
        }

        Err(InternalError::Normal(err))
    }

    de_unimp!(deserialize_unit_struct, _name: &'static str);
    de_unimp!(deserialize_newtype_struct, _name: &'static str);
    de_unimp!(deserialize_tuple_struct, _name: &'static str, _len: usize);
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;
    use std::collections::HashMap;

    #[derive(Clone, Debug, Deserialize)]
    #[serde(untagged)]
    enum MapEntry {
        Value(String),
        Struct(MapData),
        Array(Vec<MapEntry>),
    }

    type MapData = HashMap<String, MapEntry>;

    fn compare_kv(k: Option<&MapEntry>, v: &str) {
        match k {
            Some(MapEntry::Value(s)) => assert_eq!(s, v),
            _ => panic!("incorrect value"),
        }
    }

    #[test]
    fn simple_map1() -> Result<()> {
        let data = from_tokenstream::<MapData>(
            &quote! {
                "potato" = potato
            }
            .into(),
        )?;

        compare_kv(data.get("potato"), "potato");

        Ok(())
    }
    #[test]
    fn simple_map2() -> Result<()> {
        let data = from_tokenstream::<MapData>(
            &quote! {
                "potato" = potato,
                lizzie = "lizzie",
                brickley = brickley,
                "bug" = "bug"
            }
            .into(),
        )?;

        compare_kv(data.get("potato"), "potato");
        compare_kv(data.get("lizzie"), "lizzie");
        compare_kv(data.get("brickley"), "brickley");
        compare_kv(data.get("bug"), "bug");

        Ok(())
    }

    #[test]
    fn bad_ident() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            potato: String,
        }
        match from_tokenstream::<Test>(
            &quote! {
                "potato" = potato
            }
            .into(),
        ) {
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    "expected an identifier, but found `\"potato\"`"
                );
            }
            Ok(_) => panic!("unexpected success"),
        }
    }
    #[test]
    fn just_ident() {
        match from_tokenstream::<MapData>(
            &quote! {
                howdy
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(msg.to_string(), "expected `=` following `howdy`")
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn no_equals() {
        match from_tokenstream::<MapData>(
            &quote! {
                hi there
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(msg.to_string(), "expected `=`, but found `there`")
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn paren_grouping() {
        match from_tokenstream::<MapData>(
            &quote! {
                hi = ()
            }
            .into(),
        ) {
            Err(msg) => assert_eq!(
                msg.to_string(),
                "data did not match any variant of untagged enum MapEntry"
            ),
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn no_value() {
        match from_tokenstream::<MapData>(
            &quote! {
                x =
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(msg.to_string(), "expected a value following `=`")
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn no_value2() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            x: String,
        }
        match from_tokenstream::<Test>(
            &quote! {
                x =
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(msg.to_string(), "expected a string following `=`")
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn simple() {
        #[derive(Deserialize)]
        struct Test {
            hi: String,
        }
        let m = from_tokenstream::<Test>(
            &quote! {
                hi = there
            }
            .into(),
        )
        .unwrap();

        assert_eq!(m.hi, "there");
    }

    #[test]
    fn simple2() {
        #[derive(Deserialize)]
        struct Test {
            message: String,
        }
        let m = from_tokenstream::<Test>(
            &quote! {
                message = "hi there"
            }
            .into(),
        )
        .unwrap();
        assert_eq!(m.message, "hi there");
    }
    #[test]
    fn trailing_comma() {
        #[derive(Deserialize)]
        struct Test {
            hi: String,
        }
        let m = from_tokenstream::<Test>(
            &quote! {
                hi = there,
            }
            .into(),
        )
        .unwrap();
        assert_eq!(m.hi, "there");
    }

    #[test]
    fn double_comma() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            hi: String,
        }
        match from_tokenstream::<Test>(
            &quote! {
                hi = there,,
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(
                    msg.to_string(),
                    "expected an identifier, but found `,`"
                );
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn bad_value() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            wat: String,
        }
        match from_tokenstream::<Test>(
            &quote! {
                wat = ?
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(msg.to_string(), "expected a string, but found `?`");
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn bad_value2() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            the_meaning_of_life_the_universe_and_everything: String,
        }
        match from_tokenstream::<Test>(
            &quote! {
                the_meaning_of_life_the_universe_and_everything = 42
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(
                    msg.to_string(),
                    "expected a string, but found `42`"
                );
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn bad_value3() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            a: String,
        }
        match from_tokenstream::<Test>(
            &quote! {
                b = 42,
                a = "howdy",
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(msg.to_string(), "extraneous member `b`");
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn bad_value4() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            a: String,
        }
        match from_tokenstream::<Test>(
            &quote! {
                b = ?,
                a = "howdy",
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(msg.to_string(), "extraneous member `b`");
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn bad_value5() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            a: (),
        }
        match from_tokenstream::<Test>(
            &quote! {
                a = 7,
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(msg.to_string(), "expected a unit, but found `7`");
            }
            Ok(_) => panic!("unexpected success"),
        }
    }
    #[test]
    fn bad_map_value() {
        match from_tokenstream::<MapData>(
            &quote! {
                wtf = [ ?! ]
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(msg.to_string(), "expected a value, but found `?`")
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn simple_array1() {
        #[derive(Deserialize)]
        struct Test {
            array: Vec<u32>,
        }
        let t = from_tokenstream::<Test>(
            &quote! {
                array = []
            }
            .into(),
        )
        .unwrap();
        assert!(t.array.is_empty());
    }
    #[test]
    fn simple_array2() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            array: Vec<u32>,
        }
        match from_tokenstream::<Test>(
            &quote! {
                array = [1, 2, 3]
            }
            .into(),
        ) {
            Ok(t) => assert_eq!(t.array[0], 1),
            Err(err) => panic!("unexpected failure: {:?}", err),
        }
    }

    #[test]
    fn simple_array3() {
        #[derive(Deserialize)]
        struct Test {
            array: Vec<Test2>,
        }
        #[derive(Deserialize)]
        struct Test2 {}
        let t = from_tokenstream::<Test>(
            &quote! {
                array = [{}]
            }
            .into(),
        )
        .unwrap();
        assert_eq!(t.array.len(), 1);
    }

    #[test]
    fn simple_array4() {
        #[derive(Deserialize)]
        struct Test {
            array: Vec<Test2>,
        }
        #[derive(Deserialize)]
        struct Test2 {}
        let t = from_tokenstream::<Test>(
            &quote! {
                array = [{}, {},]
            }
            .into(),
        )
        .unwrap();
        assert_eq!(t.array.len(), 2);
    }

    #[test]
    fn bad_array2() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            array: Vec<Test2>,
        }
        #[derive(Deserialize)]
        struct Test2 {}
        match from_tokenstream::<Test>(
            &quote! {
                array = [{}<-]
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(
                    msg.to_string(),
                    "expected `,` or nothing, but found `<`"
                );
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn bad_array3() {
        match from_tokenstream::<MapData>(
            &quote! {
                array = [{}<-]
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(
                    msg.to_string(),
                    "expected `,` or nothing, but found `<`"
                );
            }
            Ok(_) => panic!("unexpected success"),
        }
    }
    #[test]
    fn bad_array4() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            array: Vec<Test2>,
        }
        #[derive(Deserialize)]
        struct Test2 {}
        match from_tokenstream::<Test>(
            &quote! {
                array = {}
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(
                    msg.to_string(),
                    "expected an array, but found `{}`"
                );
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn bupkis() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            array: String,
        }
        match from_tokenstream::<Test>(&quote! {}.into()) {
            Err(msg) => {
                assert_eq!(msg.to_string(), "missing field `array`");
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn nested_bupkis1() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            array: Test2,
        }
        #[derive(Deserialize)]
        struct Test2 {
            #[allow(dead_code)]
            item: u32,
        }
        match from_tokenstream::<Test>(
            &quote! {
                array = {}
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(msg.to_string(), "missing field `item`");
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn bad_enum() {
        #[derive(Deserialize)]
        enum Foo {
            Foo,
        }
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            foo: Foo,
        }
        match from_tokenstream::<Test>(
            &quote! {
                foo = Foop
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(
                    msg.to_string(),
                    "unknown variant `Foop`, expected `Foo`"
                );
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn bad_enum2() {
        #[derive(Deserialize)]
        enum Foo {
            Foo,
        }
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            foo: Foo,
        }
        match from_tokenstream::<Test>(
            &quote! {
                foo =
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(
                    msg.to_string(),
                    "expected an identifier following `=`"
                );
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn bad_enum3() {
        #[derive(Deserialize)]
        enum Foo {
            Foo(u32),
        }
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            foo: Foo,
        }
        match from_tokenstream::<Test>(
            &quote! {
                foo = Foo
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(
                    msg.to_string(),
                    "non-unit variants are not currently supported"
                );
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn bad_enum4() {
        #[derive(Deserialize)]
        enum Foo {
            #[allow(dead_code)]
            Foo { foo: u32 },
        }
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            foo: Foo,
        }
        match from_tokenstream::<Test>(
            &quote! {
                foo = Foo
            }
            .into(),
        ) {
            Err(msg) => {
                assert_eq!(
                    msg.to_string(),
                    "non-unit variants are not currently supported"
                );
            }
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn tuple() {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            tup: (u32, u32),
        }
        match from_tokenstream::<Test>(
            &quote! {
                tup = (1, 2)
            }
            .into(),
        ) {
            Ok(t) => assert_eq!(t.tup.1, 2),
            Err(err) => panic!("unexpected failure: {:?}", err),
        }
    }
}
