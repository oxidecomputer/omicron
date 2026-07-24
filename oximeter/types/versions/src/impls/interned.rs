// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A string type deduplicated through a thread-local intern table.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

/// Maximum entries in each thread's intern table.
///
/// The table is cleared when it reaches this size, bounding the memory
/// retained by a long-lived thread if the interned vocabulary churns.
/// Existing `InternedString`s remain valid after a clear; only the
/// deduplication resets.
const MAX_INTERNED_STRINGS: usize = 8192;

// foldhash rather than the std default (SipHash): the table is keyed by
// short strings hashed once per name per sample, where SipHash's fixed
// per-hash overhead dominates. SipHash's flood resistance buys nothing
// here — keys come from Oxide-authored producers on the underlay, and the
// size cap bounds worst-case probing regardless.
type InternTable = HashSet<Arc<str>, foldhash::fast::RandomState>;

thread_local! {
    static INTERN_TABLE: RefCell<InternTable> =
        RefCell::new(InternTable::default());
}

fn intern(s: &str) -> Arc<str> {
    INTERN_TABLE.with(|table| {
        let mut table = table.borrow_mut();
        if let Some(interned) = table.get(s) {
            return Arc::clone(interned);
        }
        if table.len() >= MAX_INTERNED_STRINGS {
            table.clear();
        }
        let interned: Arc<str> = Arc::from(s);
        table.insert(Arc::clone(&interned));
        interned
    })
}

/// An immutable string, deduplicated against other instances created on the
/// same thread.
///
/// Strings like field names recur in nearly every sample, so representing
/// them as owned `String`s allocates the same few hundred distinct strings
/// once per sample, forever. An `InternedString` is a shared handle to a
/// single allocation: constructing one from a string already seen on this
/// thread is a table lookup and a reference-count increment rather than an
/// allocation and a copy.
///
/// The type serializes exactly like a string, so wire formats and the
/// timeseries key derivation are unchanged from `String`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InternedString(Arc<str>);

impl InternedString {
    /// Return the string as a `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for InternedString {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for InternedString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for InternedString {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for InternedString {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl From<&str> for InternedString {
    fn from(s: &str) -> Self {
        Self(intern(s))
    }
}

impl From<String> for InternedString {
    fn from(s: String) -> Self {
        Self(intern(&s))
    }
}

impl From<InternedString> for String {
    fn from(s: InternedString) -> Self {
        s.0.to_string()
    }
}

impl PartialEq<str> for InternedString {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

impl PartialEq<&str> for InternedString {
    fn eq(&self, other: &&str) -> bool {
        &*self.0 == *other
    }
}

impl PartialEq<String> for InternedString {
    fn eq(&self, other: &String) -> bool {
        &*self.0 == other.as_str()
    }
}

impl Serialize for InternedString {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for InternedString {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = InternedString;

            fn expecting(
                &self,
                f: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                f.write_str("a string")
            }

            fn visit_str<E: serde::de::Error>(
                self,
                v: &str,
            ) -> Result<Self::Value, E> {
                Ok(InternedString::from(v))
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}

impl JsonSchema for InternedString {
    fn schema_name() -> String {
        String::schema_name()
    }

    fn json_schema(
        r#gen: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        String::json_schema(r#gen)
    }

    fn is_referenceable() -> bool {
        String::is_referenceable()
    }
}

#[cfg(test)]
mod tests {
    use super::InternedString;

    #[test]
    fn test_interned_strings_share_an_allocation() {
        let a = InternedString::from("some_field_name");
        let b = InternedString::from("some_field_name");
        assert!(std::sync::Arc::ptr_eq(&a.0, &b.0));
    }

    #[test]
    fn test_serializes_like_a_string() {
        let s = InternedString::from("foo:bar");
        assert_eq!(
            serde_json::to_string(&s).unwrap(),
            serde_json::to_string("foo:bar").unwrap(),
        );
        let rt: InternedString =
            serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        assert_eq!(rt, s);
    }
}
