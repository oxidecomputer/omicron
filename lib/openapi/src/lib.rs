extern crate proc_macro;

use proc_macro2::{TokenStream, TokenTree};
use quote::quote;

use std::collections::HashMap;

#[proc_macro_attribute]
pub fn endpoint(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    treeify(0, &attr.clone().into());
    let _ = to_map(&attr.into());

    let ast: syn::ItemFn = syn::parse(item).unwrap();
    let name = ast.sig.ident.clone();

    let stream = quote! {
        #[allow(non_camel_case_types, missing_docs)]
        pub struct #name;
        impl #name {
            fn register(router: &mut HttpRouter) {
                #ast
                router.insert(Method::GET, "/projects/2/{project_id}", api_handler_create(#name));
            }
        }
    };
    stream.into()
}

enum MapEntry {
    Value(String),
    Group(HashMap<String, MapEntry>),
}

macro_rules! abort {
    ($span:expr, $($tts:tt)*) => {
        if cfg!(test) {
            panic!($($tts)*)
        } else {
            proc_macro_error::abort!($span, $($tts)*)
        }
    };
}

fn to_map(tt: &TokenStream) -> HashMap<String, MapEntry> {
    let mut map = HashMap::new();
    let mut iter = tt.clone().into_iter();

    loop {
        let key = match iter.next() {
            None => break,
            Some(ident @ TokenTree::Ident(_)) => ident,
            Some(token) => abort!(token, "expected a identifier, but found `{}`", token),
        };

        let eq = match iter.next() {
            Some(TokenTree::Punct(punct)) => {
                if punct.as_char() != '=' {
                    abort!(punct, "thus I die");
                }
                punct
            }

            Some(token) => abort!(token, "expected `=`, but found `{}`", token),
            None => abort!(key, "expected `=` following `{}`", key),
        };

        let value = match iter.next() {
            Some(TokenTree::Group(group)) => MapEntry::Group(to_map(&group.stream())),
            Some(TokenTree::Ident(ident)) => MapEntry::Value(ident.to_string()),
            Some(TokenTree::Literal(lit)) => MapEntry::Value(lit.to_string()),
            Some(token) => abort!(token, "expected a value, but found `{}`", token),
            None => abort!(eq, "expected a value following `{}`", eq),
        };

        map.insert(key.to_string(), value);

        let comma = iter.next();
        match comma {
            None => break,
            Some(TokenTree::Punct(punct)) if punct.as_char() == ',' => continue,
            _ => {
                println!("I'm sad");
                break;
            }
        }
    }

    map
}

fn treeify(depth: usize, tt: &TokenStream) {
    for i in tt.clone().into_iter() {
        match i {
            proc_macro2::TokenTree::Group(child) => treeify(depth + 1, &child.stream()),
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
            crate::MapEntry::Value(s) => assert_eq!(s, "hi there"),
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

    #[test]
    #[should_panic(expected = r#"expected a value, but found `42`"#)]
    fn bad_value2() {
        let _ = super::to_map(
            &quote! {
                the_meaning_of_life_the_universe_and_everything = 42
            }
            .into(),
        );
    }
}
