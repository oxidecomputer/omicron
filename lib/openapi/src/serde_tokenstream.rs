use proc_macro2::{Delimiter, TokenStream, TokenTree};

use serde::de::{
    DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
};
use serde::{Deserialize, Deserializer};

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
    start: bool,
}

impl<'de> TokenDe {
    fn from_tokenstream(input: &'de TokenStream) -> Self {
        let mut s = TokenDe::new(input);
        s.start = true;
        s
    }

    fn new(input: &'de TokenStream) -> Self {
        treeify(0, &input.clone());
        let t: Box<dyn Iterator<Item = TokenTree>> =
            Box::new(input.clone().into_iter());
        TokenDe {
            input: t.peekable(),
            start: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    ExpectedStruct,
    ExpectedBool,
    Unknown,
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
        formatter.write_str("error")
    }
}

impl<'de, 'a> MapAccess<'de> for TokenDe {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        if self.input.peek().is_none() {
            return Ok(None);
        }
        let key = seed.deserialize(&mut *self).map(Some);

        // Verify we have an '=' delimiter.
        let _eq = match self.input.next() {
            Some(TokenTree::Punct(punct)) if punct.as_char() == '=' => punct,

            Some(token) => abort!(token, "expected `=`, but found `{}`", token),
            //None => abort!(keytok, "expected `=` following `{}`", ""),
            None => panic!("bad"),
        };

        key
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        let value = seed.deserialize(&mut *self);

        let comma = self.input.next();
        match comma {
            None => (),
            Some(TokenTree::Punct(punct)) if punct.as_char() == ',' => (),
            Some(token) => abort!(token, "expected `,`, but found `{}`", token),
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
        let comma = self.input.next();
        if value.is_ok() {
            match comma {
                None => (),
                Some(TokenTree::Punct(punct)) if punct.as_char() == ',' => (),
                Some(token) => {
                    abort!(token, "expected `,`, but found `{}`", token)
                }
            }
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
            Err(_) => panic!("error"),
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
    ($i:ident) => {
        fn $i<V>(self, _visitor: V) -> Result<V::Value>
        where
            V: Visitor<'de>,
        {
            panic!(stringify!($i));
        }
    };
}

impl<'de, 'a> Deserializer<'de> for &'a mut TokenDe {
    type Error = Error;
    de_unimp!(deserialize_any);
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
    de_unimp!(deserialize_map);
    de_unimp!(deserialize_ignored_any);

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.input.next() {
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

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        panic!("deserialize_tuple")
    }
    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        panic!("deserialize_unit_struct")
    }
    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        panic!("deserialize_newtype_struct")
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = match self.input.next() {
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
            //None => abort!(eq, "expected a value following `{}`", eq),
            None => panic!("expected value"),
        };

        println!("visit_string({})", value);
        visitor.visit_string(value)
    }
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.input.next() {
            Some(TokenTree::Group(group)) => match group.delimiter() {
                Delimiter::Bracket => {
                    visitor.visit_seq(TokenDe::new(&group.stream()))
                }
                _ => panic!("bad group type"),
            },
            _ => panic!("not a group"),
        }
    }
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        panic!("deserialize_tuple_struct")
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
        println!("start: {}", self.start);
        if self.start {
            return visitor.visit_map(self);
        }
        match self.input.next() {
            Some(TokenTree::Group(group)) => match group.delimiter() {
                Delimiter::Brace => {
                    return visitor.visit_map(TokenDe::new(&group.stream()))
                }
                _ => panic!("bad group type"),
            },
            None => panic!("struct EOF"),
            _ => panic!("bad token"),
        }
        //Err(Error::ExpectedStruct)
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
        let id = match self.input.next() {
            None => panic!("id EOF"),
            Some(ident @ TokenTree::Ident(_)) => ident,
            Some(token) => {
                abort!(token, "expected an identifier, but found `{}`", token)
            }
        };
        println!("visit_string({})", id.to_string());
        visitor.visit_string(id.to_string())
    }
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
