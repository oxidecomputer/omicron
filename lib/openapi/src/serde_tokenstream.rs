use proc_macro2::{Delimiter, Group, TokenStream, TokenTree};

use serde::de::{
    DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
};
use serde::{Deserialize, Deserializer};

use std::collections::HashMap;
use std::fmt::{self, Display};

use core::iter::Peekable;

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

pub type Result<T> = std::result::Result<T, Error>;

struct TokenDe {
    input: Peekable<Box<dyn Iterator<Item = TokenTree>>>,
    current: Option<TokenTree>,
    last: Option<TokenTree>,
}

impl<'de> TokenDe {
    fn from_tokenstream(input: &'de TokenStream) -> Self {
        // We implicitly start inside a brace-surrounded struct; this
        // allows for more generic handling elsewhere.
        TokenDe::new(&TokenStream::from(TokenTree::from(Group::new(
            Delimiter::Brace,
            input.clone(),
        ))))
    }

    fn new(input: &'de TokenStream) -> Self {
        println!("new TokenDe");
        treeify(0, &input.clone());
        let t: Box<dyn Iterator<Item = TokenTree>> =
            Box::new(input.clone().into_iter());
        TokenDe {
            input: t.peekable(),
            current: None,
            last: None,
        }
    }

    fn gobble_optional_comma(&mut self) -> Result<()> {
        match self.next() {
            None => Ok(()),
            Some(TokenTree::Punct(punct)) if punct.as_char() == ',' => Ok(()),
            Some(token) => Err(Error::ExpectedCommaOrNothing(
                token.clone(),
                format!("expected `,` or nothing, but found `{}`", token),
            )),
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

    fn last_err<T>(&self) -> Result<T> {
        match &self.last {
            None => {
                println!("didn't get started");
                Err(Error::Unknown)
            }
            Some(token) => Err(Error::ExpectedValue(
                token.clone(),
                format!("expected a value following `{}`", token),
            )),
        }
    }

    fn deserialize_error<VV>(
        &self,
        next: Option<TokenTree>,
        what: &str,
    ) -> Result<VV> {
        match next {
            Some(token) => Err(Error::ExpectedValue(
                token.clone(),
                format!("expected {}, but found `{}`", what, token),
            )),
            None => self.last_err(),
        }
    }

    fn deserialize_int<T, VV, F>(&mut self, visit: F) -> Result<VV>
    where
        F: FnOnce(T) -> Result<VV>,
        T: std::str::FromStr,
        T::Err: Display,
    {
        let token = self.next();

        if let Some(TokenTree::Literal(literal)) = &token {
            if let Ok(syn::ExprLit {
                lit: syn::Lit::Int(i), ..
            }) = syn::parse_str::<syn::ExprLit>(&literal.to_string())
            {
                if let Ok(value) = i.base10_parse::<T>() {
                    return visit(value);
                }
            }
        }

        self.deserialize_error(token, stringify!(T))
    }

    fn deserialize_float<T, VV, F>(&mut self, visit: F) -> Result<VV>
    where
        F: FnOnce(T) -> Result<VV>,
        T: std::str::FromStr,
        T::Err: Display,
    {
        let token = self.next();

        if let Some(TokenTree::Literal(literal)) = &token {
            if let Ok(syn::ExprLit {
                lit: syn::Lit::Float(f), ..
            }) = syn::parse_str::<syn::ExprLit>(&literal.to_string())
            {
                if let Ok(value) = f.base10_parse::<T>() {
                    return visit(value);
                }
            }
        }

        self.deserialize_error(token, stringify!(T))
    }
}

#[derive(Clone, Debug)]
pub enum Error {
    ExpectedArray(TokenTree, String),
    ExpectedMap(TokenTree, String),
    ExpectedStruct(TokenTree, String),
    ExpectedIdentifier(TokenTree, String),
    ExpectedCommaOrNothing(TokenTree, String),
    ExpectedAssignment(TokenTree, String),
    ExpectedValue(TokenTree, String),
    ExpectedEOF(TokenTree, String),
    Unknown,
    UnexpectedGrouping(TokenTree, String),
    NoData(String),
}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Error::NoData(format!("{}", msg))
    }
}
impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(format!("{:?}", self).as_str())
    }
}

impl<'de, 'a> MapAccess<'de> for TokenDe {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        let keytok = match self.input.peek() {
            None => {
                //panic!("looking but not finding");
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
                    return Err(Error::ExpectedAssignment(
                        token.clone(),
                        format!("expected `=`, but found `{}`", token),
                    ))
                }
                None => {
                    return Err(Error::ExpectedAssignment(
                        keytok.clone(),
                        format!("expected `=` following `{}`", keytok),
                    ))
                }
            };
        }

        key
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        let value = seed.deserialize(&mut *self);
        if value.is_ok() {
            self.gobble_optional_comma()?;
        }
        value
    }
}

impl<'de, 'a> SeqAccess<'de> for TokenDe {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
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
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        let val = seed.deserialize(&mut *self);

        match val {
            Err(err) => panic!("error: {}", err),
            Ok(v) => Ok((v, self)),
        }
    }
}

impl<'de, 'a> VariantAccess<'de> for &mut TokenDe {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        // TODO should not be allowed
        todo!("newtype_variant_seed");
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // TODO should not be allowed
        todo!("tuple_variant");
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!("struct_variant");
    }
}

/// Stub out Deserializer trait functions we don't want to deal with right now.
macro_rules! de_unimp {
    ($i:ident $(, $p:ident : $t:ty )*) => {
        fn $i<V>(self $(, $p: $t)*, _visitor: V) -> Result<V::Value>
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
    type Error = Error;

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
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
            // TODO not quite right
            other => self.deserialize_error(other, "bool"),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // None is a missing field, so this must be Some.
        visitor.visit_some(self)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
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

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let next = self.next();

        if let Some(token) = &next {
            if let TokenTree::Group(group) = token {
                if let Delimiter::Bracket = group.delimiter() {
                    match visitor.visit_seq(TokenDe::new(&group.stream())) {
                        Err(Error::NoData(msg)) => {
                            return Err(Error::ExpectedValue(
                                token.clone(),
                                msg,
                            ))
                        }
                        other => return other,
                    }
                }
            }
        }

        match next {
            Some(token) => Err(Error::ExpectedArray(
                token.clone(),
                format!("expected an array, but found `{}`", token),
            )),
            // TODO this isn't quite right. I should make Error a struct with 3
            // fields: reason, token, msg.
            None => self.last_err(),
        }
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let next = self.next();

        if let Some(token) = &next {
            if let TokenTree::Group(group) = token {
                if let Delimiter::Brace = group.delimiter() {
                    match visitor.visit_map(TokenDe::new(&group.stream())) {
                        Err(Error::NoData(msg)) => {
                            return Err(Error::ExpectedIdentifier(
                                token.clone(),
                                msg,
                            ))
                        }
                        other => return other,
                    }
                }
            }
        };

        match next {
            Some(token) => Err(Error::ExpectedStruct(
                token.clone(),
                format!("expected a struct, but found `{}`", token),
            )),
            // TODO this isn't quite right. I should make Error a struct with 3
            // fields: reason, token, msg.
            None => self.last_err(),
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let next = self.next();

        if let Some(TokenTree::Group(group)) = &next {
            if let Delimiter::Brace = group.delimiter() {
                return visitor.visit_map(TokenDe::new(&group.stream()));
            }
        }

        match next {
            Some(token) => Err(Error::ExpectedMap(
                token.clone(),
                format!("expected a map, but found `{}`", token),
            )),
            // TODO this isn't quite right. I should make Error a struct with 3
            // fields: reason, token, msg.
            None => self.last_err(),
        }
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let id = match self.next() {
            Some(ident @ TokenTree::Ident(_)) => ident,
            Some(token) => {
                println!("{:?}", token);
                return Err(Error::ExpectedIdentifier(
                    token.clone(),
                    format!("expected an identifier, but found `{}`", token),
                ));
            }
            None => return self.last_err(),
        };
        visitor.visit_string(id.to_string())
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let token = self.next();
        println!("any {:?}", token);

        match &token {
            None => self.last_err(),
            Some(TokenTree::Group(group)) => match group.delimiter() {
                Delimiter::Brace => {
                    visitor.visit_map(TokenDe::new(&group.stream()))
                }
                Delimiter::Bracket => {
                    visitor.visit_seq(TokenDe::new(&group.stream()))
                }
                Delimiter::Parenthesis => Err(Error::UnexpectedGrouping(
                    group.clone().into(),
                    format!("parentheses are not allowed"),
                )),
                Delimiter::None => Err(Error::UnexpectedGrouping(
                    group.clone().into(),
                    format!("the null delimiter is not allowed"),
                )),
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
                println!("literal {:?}", lit);
                match syn::parse_str::<syn::ExprLit>(&lit.to_string()) {
                    Ok(syn::ExprLit {
                        lit: syn::Lit::Str(s), ..
                    }) => visitor.visit_string(s.value()),
                    Ok(syn::ExprLit {
                        lit: syn::Lit::Int(i), ..
                    }) => visitor.visit_u64(i.base10_parse::<u64>().unwrap()),
                    Ok(syn::ExprLit {
                        lit: syn::Lit::Float(f), ..
                    }) => visitor.visit_f64(f.base10_parse::<f64>().unwrap()),
                    _ => self.deserialize_error(token, "a value"),
                }
            }
            Some(TokenTree::Punct(_)) => {
                self.deserialize_error(token, "a value")
            }
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_i8(value))
    }
    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_i16(value))
    }
    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_i32(value))
    }
    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_i64(value))
    }
    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_u8(value))
    }
    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_u16(value))
    }
    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_u32(value))
    }
    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_int(|value| visitor.visit_u64(value))
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_float(|value| visitor.visit_f32(value))
    }
    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_float(|value| visitor.visit_f64(value))
    }

    de_unimp!(deserialize_char);
    de_unimp!(deserialize_str);
    de_unimp!(deserialize_bytes);
    de_unimp!(deserialize_byte_buf);
    de_unimp!(deserialize_unit);
    de_unimp!(deserialize_ignored_any);
    de_unimp!(deserialize_tuple, _len: usize);
    de_unimp!(deserialize_unit_struct, _name: &'static str);
    de_unimp!(deserialize_newtype_struct, _name: &'static str);
    de_unimp!(deserialize_tuple_struct, _name: &'static str, _len: usize);
}

pub fn from_tokenstream<'a, T>(tokens: &'a TokenStream) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = TokenDe::from_tokenstream(tokens);
    let result = T::deserialize(&mut deserializer);
    match &result {
        // This can only happen if we were given no input
        Err(Error::NoData(_)) => {
            assert!(deserializer.last.is_none());
            result
        }

        // Pass through all other errors.
        Err(_) => result,

        // On success, check that there isn't extra data.
        Ok(_) => match deserializer.next() {
            None => result,
            Some(token) => Err(Error::ExpectedEOF(
                token.clone(),
                format!("expected EOF but found `{}`", token),
            )),
        },
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum MapEntry {
    Value(String),
    Struct(MapData),
    Array(Vec<MapEntry>),
}

type MapData = HashMap<String, MapEntry>;

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

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
            Err(Error::ExpectedIdentifier(_, msg)) => {
                assert_eq!(
                    msg,
                    "expected an identifier, but found `\"potato\"`"
                );
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
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
            Err(Error::ExpectedAssignment(_, msg)) => {
                assert_eq!(msg, "expected `=` following `howdy`")
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
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
            Err(Error::ExpectedAssignment(_, msg)) => {
                assert_eq!(msg, "expected `=`, but found `there`")
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[test]
    fn paren_grouping() {
        match from_tokenstream::<MapData>(
            &quote! {
                hi = (a, b, c)
            }
            .into(),
        ) {
            Err(Error::UnexpectedGrouping(_, msg)) => {
                assert_eq!(msg, "parentheses are not allowed")
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
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
            Err(Error::ExpectedValue(_, msg)) => {
                assert_eq!(msg, "expected a value following `=`")
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
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
            Err(Error::ExpectedValue(_, msg)) => {
                assert_eq!(msg, "expected a value following `=`")
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
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
            Err(Error::ExpectedIdentifier(_, msg)) => {
                assert_eq!(msg, "expected an identifier, but found `,`");
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
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
            Err(Error::ExpectedValue(_, msg)) => {
                assert_eq!(msg, "expected a string, but found `?`");
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
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
            Err(Error::ExpectedValue(_, msg)) => {
                assert_eq!(msg, "expected a string, but found `42`");
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
            Ok(_) => panic!("unexpected success"),
        }
    }
    #[test]
    fn bad_value3() {
        match from_tokenstream::<MapData>(
            &quote! {
                wtf = [ ?! ]
            }
            .into(),
        ) {
            Err(Error::ExpectedValue(_, msg)) => {
                assert_eq!(msg, "expected a value, but found `?`")
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
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
            Err(Error::ExpectedCommaOrNothing(_, msg)) => {
                assert_eq!(msg, "expected `,` or nothing, but found `<`");
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
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
            Err(Error::ExpectedCommaOrNothing(_, msg)) => {
                assert_eq!(msg, "expected `,` or nothing, but found `<`");
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
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
            Err(Error::ExpectedArray(_, msg)) => {
                assert_eq!(msg, "expected an array, but found `{}`");
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
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
            Err(Error::ExpectedIdentifier(_, msg)) => {
                assert_eq!(msg, "missing field `array`");
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
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
            Err(Error::ExpectedIdentifier(_, msg)) => {
                assert_eq!(msg, "missing field `item`");
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
            Ok(_) => panic!("unexpected success"),
        }
    }
}
