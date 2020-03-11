extern crate proc_macro;

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use quote::quote;

use std::collections::HashMap;

macro_rules! abort {
    ($span:expr, $($tts:tt)*) => {
        if cfg!(test) {
            panic!($($tts)*)
        } else {
            proc_macro_error::abort!($span, $($tts)*)
        }
    };
}

#[proc_macro_attribute]
pub fn endpoint(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    //treeify(0, &attr.clone().into());
    let a = attr.into();
    let mm = to_map(&a);
    //emit(0, MapEntry::Struct(mm.clone()));

    let method = match mm.get("method") {
        Some(MapEntry::Value(method)) => match method.as_str() {
            "DELETE" | "GET" | "PATCH" | "POST" | "PUT" => method,
            _ => abort!(a, r#"invlid method "{}""#, method),
        },
        None => abort!(
            a,
            r#""method" is a required field; valid values are DELETE, GET, PATCH, POST, and PUT"#
        ),
        _ => panic!("not done"),
    };

    let path = match mm.get("path") {
        Some(MapEntry::Value(path)) => path,
        _ => panic!("not done"),
    };

    let ast: syn::ItemFn = syn::parse(item).unwrap();
    let name = ast.sig.ident.clone();

    let method_ident = quote::format_ident!("{}", method);

    let stream = quote! {
        #[allow(non_camel_case_types, missing_docs)]
        pub struct #name;
        impl #name {
            fn register(router: &mut HttpRouter) {
                #ast
                router.insert(Method::#method_ident, #path, HttpRouteHandler::new(#name));
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

#[allow(dead_code)]
fn emit(depth: usize, mm: MapEntry) {
    match mm {
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

fn parse(tt: &TokenStream) -> MapEntry {
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
                abort!(token, "expected a identifier, but found `{}`", token)
            }
        };

        // Verify we have an '=' delimiter.
        let eq = match iter.next() {
            Some(TokenTree::Punct(punct)) if punct.as_char() != '=' => punct,

            Some(token) => abort!(token, "expected `=`, but found `{}`", token),
            None => abort!(key, "expected `=` following `{}`", key),
        };

        let value = match iter.next() {
            Some(TokenTree::Group(group)) => match group.delimiter() {
                Delimiter::Parenthesis => {
                    abort!(group, "parentheses not allowed")
                }
                Delimiter::Brace => parse(&group.stream()),
                Delimiter::Bracket => MapEntry::Array(to_vec(&group.stream())),
                Delimiter::None => abort!(group, "invalid grouping"),
            },
            Some(TokenTree::Ident(ident)) => MapEntry::Value(ident.to_string()),
            Some(TokenTree::Literal(lit)) => {
                match syn::parse_str::<syn::ExprLit>(&lit.to_string()) {
                    Ok(syn::ExprLit {
                        lit: syn::Lit::Str(s), ..
                    }) => MapEntry::Value(s.value()),
                    _ => abort!(lit, "expected a string but found `{}`", lit),
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
                parse(&group.stream())
            }
            Some(token) => abort!(token, "expected to find an object {...}"),
        };

        vec.push(item);

        // Look for a comma; terminate the loop if none is found.
        match iter.next() {
            None => break,
            Some(TokenTree::Punct(punct)) if punct.as_char() == ',' => continue,
            Some(token) => abort!(token, "sadface"),
        }
    }

    vec
}

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

#[cfg(test)]
mod tests {
    use quote::quote;

    #[test]
    #[should_panic(expected = r#"expected a identifier, but found `"potato"`"#)]
    fn bad_ident() {
        let _ = super::to_map(
            &quote! {
                "potato"
            }
            .into(),
        );
    }

    #[test]
    #[should_panic(expected = r#"expected `=` following `howdy`"#)]
    fn just_ident() {
        let _ = super::to_map(
            &quote! {
                howdy
            }
            .into(),
        );
    }

    #[test]
    #[should_panic(expected = r#"expected `=`, but found `there`"#)]
    fn no_equals() {
        let _ = super::to_map(
            &quote! {
                hi there
            }
            .into(),
        );
    }

    #[test]
    #[should_panic(expected = r#"expected a value following `=`"#)]
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
            crate::MapEntry::Value(s) => assert_eq!(s, r#""hi there""#),
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
    #[should_panic(expected = r#"expected a identifier, but found `,`"#)]
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
    #[should_panic(expected = r#"expected a value, but found `?`"#)]
    fn bad_value() {
        let _ = super::to_map(
            &quote! {
                wat = ?
            }
            .into(),
        );
    }
}
