use proc_macro2::{Delimiter, Group, TokenStream, TokenTree};

use serde::de::{
    DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
};
use serde::{Deserialize, Deserializer};

use std::collections::HashMap;
use std::fmt::{self, Display};

use core::iter::Peekable;

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
        treeify(0, &input.clone());
        let t: Box<dyn Iterator<Item = TokenTree>> =
            Box::new(input.clone().into_iter());
        TokenDe {
            input: t.peekable(),
            last: None,
        }
    }

    fn gobble_optional_comma(&mut self) -> Result<()> {
        match self.next() {
            None => Ok(()),
            Some(TokenTree::Punct(punct)) if punct.as_char() == ',' => Ok(()),
            Some(_) => Err(Error::ExpectedCommaOrNothing),
        }
    }

    fn next(&mut self) -> Option<TokenTree> {
        let last = self.input.next();
        self.last = last.as_ref().map(|t| t.clone());
        last
    }

    fn last_err<T>(self) -> Result<T> {
        match self.last {
            None => Err(Error::Unknown),
            Some(token) => Err(Error::ExpectedValue(
                token.clone(),
                format!("expected a value following `{}`", token),
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Error {
    ExpectedMap,
    ExpectedStruct,
    ExpectedIdentifier,
    ExpectedBool,
    ExpectedCommaOrNothing,
    ExpectedAssignment(TokenTree, String),
    ExpectedValue(TokenTree, String),
    Unknown,
    UnexpectedGrouping(TokenTree, String),
    EOF(TokenTree, String),
}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        panic!("{:}", msg);
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
            None => return Ok(None),
            Some(token) => token.clone(),
        };

        let key = seed.deserialize(&mut *self).map(Some);

        // Verify we have an '=' delimiter.
        let _eq = match self.next() {
            Some(TokenTree::Punct(punct)) if punct.as_char() == '=' => punct,

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
        panic!("newtype_variant_seed");
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // TODO should not be allowed
        panic!("tuple_variant");
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // TODO should not be allowed
        panic!("struct_variant");
    }
}

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
            _ => Err(Error::ExpectedBool),
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
        let value = match self.next() {
            Some(TokenTree::Ident(ident)) => ident.to_string(),
            Some(TokenTree::Literal(lit)) => {
                match syn::parse_str::<syn::ExprLit>(&lit.to_string()) {
                    Ok(syn::ExprLit {
                        lit: syn::Lit::Str(s), ..
                    }) => s.value(),
                    _ => abort!(lit, "expected a value, but found `{}`", lit),
                }
            }

            Some(token) => {
                abort!(token, "expected a value, but found `{}`", token)
            }
            None => return &self.last_err(),
        };

        println!("visit_string({})", value);
        visitor.visit_string(value)
    }
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.next() {
            Some(TokenTree::Group(group)) => match group.delimiter() {
                Delimiter::Bracket => {
                    visitor.visit_seq(TokenDe::new(&group.stream()))
                }
                _ => panic!("bad group type"),
            },
            _ => panic!("not a group"),
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
        if let Some(TokenTree::Group(group)) = self.next() {
            if let Delimiter::Brace = group.delimiter() {
                return visitor.visit_map(TokenDe::new(&group.stream()));
            }
        }

        Err(Error::ExpectedStruct)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some(TokenTree::Group(group)) = self.next() {
            if let Delimiter::Brace = group.delimiter() {
                return visitor.visit_map(TokenDe::new(&group.stream()));
            }
        }

        Err(Error::ExpectedMap)
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
            token => {
                println!("{:?}", token);
                return Err(Error::ExpectedIdentifier);
            }
        };
        visitor.visit_string(id.to_string())
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let token = self.next();
        println!("{:?}", token);

        match token {
            None => todo!(),
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
                    _ => abort!(lit, "expected a value, but found `{}`", lit),
                }
            }
            Some(TokenTree::Punct(_)) => Err(Error::Unknown),
        }
    }

    de_unimp!(deserialize_i8);
    de_unimp!(deserialize_i16);
    de_unimp!(deserialize_i32);
    de_unimp!(deserialize_i64);
    de_unimp!(deserialize_u8);
    de_unimp!(deserialize_u16);
    de_unimp!(deserialize_u32);
    de_unimp!(deserialize_u64);
    de_unimp!(deserialize_f32);
    de_unimp!(deserialize_f64);
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
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.next().is_none() {
        Ok(t)
    } else {
        panic!("extra")
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
    fn bad_ident() -> Result<()> {
        #[derive(Deserialize)]
        struct Test {
            #[allow(dead_code)]
            potato: String,
        }
        let t = from_tokenstream::<Test>(
            &quote! {
                "potato" = potato
            }
            .into(),
        );

        match t {
            Err(Error::ExpectedIdentifier) => Ok(()),
            _ => panic!("unexpeted result"),
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
            Err(Error::UnexpectedGrouping(_, msg)) => {
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
            Err(Error::UnexpectedGrouping(_, msg)) => {
                assert_eq!(msg, "expected a value following `=`")
            }
            Err(err) => panic!("unexpected failure: {:?}", err),
            Ok(_) => panic!("unexpected success"),
        }
    }

    /*
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
    };

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
    */
    #[test]
    fn bad_bad_bad() {
        use super::MapEntry;
        use std::collections::HashMap;
        let _: super::Result<HashMap<String, MapEntry>> =
            super::from_tokenstream(
                &quote! {
                    array = [{}<-]
                }
                .into(),
            );
    }
}
