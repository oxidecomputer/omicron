//! This crate defines attributes associated with HTTP handlers. These
//! attributes are used both to define an HTTP API and to generate an OpenAPI
//! Spec (OAS) v3 document that describes the API.

extern crate proc_macro;

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use quote::quote;

use std::collections::HashMap;

use serde::de::{
    self, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess,
    VariantAccess, Visitor,
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

#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct Parameter {
    name: String,
    #[serde(rename = "in")]
    iin: InType,
}

#[derive(Deserialize)]
enum MethodType {
    DELETE,
    GET,
    PATCH,
    POST,
    PUT,
}

#[derive(Deserialize)]
struct MetaData {
    method: MethodType,
    path: String,
    parameters: Vec<Parameter>,
}

type Result<T> = std::result::Result<T, Error>;

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
enum Error {
    ExpectedStruct,
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

        if value.is_ok() {
            let comma = self.input.next();
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
    de_unimp!(deserialize_bool);
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
    de_unimp!(deserialize_option);
    de_unimp!(deserialize_unit);
    de_unimp!(deserialize_map);

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        panic!("deserialize_tuple")
    }
    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        panic!("deserialize_unit_struct")
    }
    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
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
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        panic!("unimp")
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
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
        name: &'static str,
        variants: &'static [&'static str],
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
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        panic!("deserialize_ignored_any")
    }
}

fn from_tokenstream<'a, T>(tokens: &'a TokenStream) -> Result<T>
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

/// Attribute to apply to an HTTP endpoint.
/// TODO(doc) explain intended use
#[proc_macro_attribute]
pub fn endpoint(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let attr2 = TokenStream::from(attr);

    // new serde
    let xxxx: Result<MetaData> = from_tokenstream(&attr2);

    match xxxx {
        Err(err) => println!("{:}", err),
        _ => (),
    }

    // old working
    let metadata = to_map(&attr2);

    let method = match metadata.get("method") {
        Some(MapEntry::Value(method)) => match method.as_str() {
            "DELETE" | "GET" | "PATCH" | "POST" | "PUT" => method,
            _ => abort!(attr2, r#"invalid method "{}""#, method),
        },
        None => abort!(
            attr2,
            r#""method" is a required field; valid values are DELETE, GET, PATCH, POST, and PUT"#
        ),
        _ => panic!("not done; needs test"),
    };

    let path = match metadata.get("path") {
        Some(MapEntry::Value(path)) => path,
        _ => panic!("not done; needs test"),
    };

    // ast manipulation
    let ast: syn::ItemFn = syn::parse(item).unwrap();
    let name = ast.sig.ident.clone();

    let args = &ast.sig.inputs;

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

    get_parameters(&metadata);

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
    stream.into()
}

#[derive(Clone)]
enum MapEntry {
    Value(String),
    Struct(HashMap<String, MapEntry>),
    Array(Vec<MapEntry>),
}

// Print out a MapEntry structure.
#[allow(dead_code)]
fn emit(depth: usize, metadata: MapEntry) {
    match metadata {
        MapEntry::Value(s) => println!("{}", s),
        MapEntry::Struct(m) => {
            println!("{}", '{');
            for (key, value) in m {
                print!("{:width$}{} = ", "", key, width = depth * 2 + 2);
                emit(depth + 1, value);
            }
            println!("{:width$}{}", "", '}', width = depth * 2);
        }
        MapEntry::Array(v) => {
            println!("[");
            for value in v {
                print!("{:width$}", "", width = depth * 2 + 2);
                emit(depth + 1, value);
            }
            println!("{:width$}{}", "", ']', width = depth * 2);
        }
    }
}

fn parse_kv(tt: &TokenStream) -> MapEntry {
    MapEntry::Struct(to_map(tt))
}

fn to_map(tt: &TokenStream) -> HashMap<String, MapEntry> {
    let mut map = HashMap::new();
    let mut iter = tt.clone().into_iter();

    loop {
        // Pull out the key; a None here means we've reached the end of input.
        let key = match iter.next() {
            None => break,
            Some(ident @ TokenTree::Ident(_)) => ident,
            Some(token) => {
                abort!(token, "expected an identifier, but found `{}`", token)
            }
        };

        // Verify we have an '=' delimiter.
        let eq = match iter.next() {
            Some(TokenTree::Punct(punct)) if punct.as_char() == '=' => punct,

            Some(token) => abort!(token, "expected `=`, but found `{}`", token),
            None => abort!(key, "expected `=` following `{}`", key),
        };

        let value = match iter.next() {
            Some(TokenTree::Group(group)) => match group.delimiter() {
                Delimiter::Parenthesis => {
                    abort!(group, "parentheses not allowed")
                }
                Delimiter::Brace => parse_kv(&group.stream()),
                Delimiter::Bracket => MapEntry::Array(to_vec(&group.stream())),
                Delimiter::None => abort!(group, "invalid grouping"),
            },
            Some(TokenTree::Ident(ident)) => MapEntry::Value(ident.to_string()),
            Some(TokenTree::Literal(lit)) => {
                match syn::parse_str::<syn::ExprLit>(&lit.to_string()) {
                    Ok(syn::ExprLit {
                        lit: syn::Lit::Str(s), ..
                    }) => MapEntry::Value(s.value()),
                    _ => abort!(lit, "expected a value, but found `{}`", lit),
                }
            }

            Some(token) => {
                abort!(token, "expected a value, but found `{}`", token)
            }
            None => abort!(eq, "expected a value following `{}`", eq),
        };

        map.insert(key.to_string(), value);

        let comma = iter.next();
        match comma {
            None => break,
            Some(TokenTree::Punct(punct)) if punct.as_char() == ',' => continue,
            Some(token) => abort!(token, "expected `,`, but found `{}`", token),
        }
    }

    map
}

fn is_object(group: &proc_macro2::Group) -> bool {
    match group.delimiter() {
        Delimiter::Brace => true,
        _ => false,
    }
}

/// Process a TokenStream into an vector of MapEntry. We expect `tt` to match
///   ({...},)* {...}?
fn to_vec(tt: &TokenStream) -> Vec<MapEntry> {
    let mut vec = Vec::new();

    let mut iter = tt.clone().into_iter();
    loop {
        let item = match iter.next() {
            None => break,
            Some(TokenTree::Group(group)) if is_object(&group) => {
                parse_kv(&group.stream())
            }
            Some(token) => abort!(
                token,
                "expected an object {{...}}, but found `{}`",
                token
            ),
        };

        vec.push(item);

        // Look for a comma; terminate the loop if none is found.
        match iter.next() {
            None => break,
            Some(TokenTree::Punct(punct)) if punct.as_char() == ',' => continue,
            Some(token) => abort!(token, "expected `,`, but found `{}`", token),
        }
    }

    vec
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

fn get_parameters(
    metadata: &HashMap<String, MapEntry>,
) -> std::result::Result<Vec<Parameter>, String> {
    let parameters = match metadata.get("parameters") {
        Some(MapEntry::Array(parameters)) => parameters,
        _ => panic!("not done; needs test"),
    };

    for parameter in parameters.iter() {
        if let MapEntry::Struct(obj) = parameter {
            if let MapEntry::Value(value) = obj.get("name").unwrap() {
                println!("n = {}", value);
            }
        }
    }

    Ok(vec![])
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
}
