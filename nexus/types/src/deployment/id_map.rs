// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use derive_where::derive_where;
use diffus::edit::Edit;
use diffus::Diffable;
use schemars::JsonSchema;
use serde::de::Error as _;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;

pub trait IdMappable:
    JsonSchema + Serialize + for<'de> Deserialize<'de> + for<'a> Diffable<'a>
{
    type Id: Ord
        + Copy
        + fmt::Display
        + fmt::Debug
        + JsonSchema
        + Serialize
        + for<'de> Deserialize<'de>;

    fn id(&self) -> Self::Id;
}

type Inner<T> = BTreeMap<<T as IdMappable>::Id, T>;

/// A 1:1 map ordered by the entries' IDs.
///
/// `IdMap` is a newtype wrapper around `BTreeMap` that does not allow a
/// mismatch between a key and its associated value (i.e., the keys are
/// guaranteed to be the ID of their associated value). Its implementations of
/// various serialization-related traits all erase the `IdMap` and behave like
/// the inner `BTreeMap` would, although deserialzation performs extra checks to
/// guarantee the key-must-be-its-values-ID property.
#[derive(Clone, Debug, Eq, PartialEq)]
#[derive_where(Default)]
pub struct IdMap<T: IdMappable> {
    inner: Inner<T>,
}

impl<T: IdMappable> IdMap<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, entry: T) -> Option<T> {
        self.inner.insert(entry.id(), entry)
    }
}

impl<T: IdMappable> FromIterator<T> for IdMap<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let inner = iter.into_iter().map(|entry| (entry.id(), entry)).collect();
        Self { inner }
    }
}

impl<T: IdMappable> JsonSchema for IdMap<T> {
    fn schema_name() -> String {
        Inner::<T>::schema_name()
    }

    fn json_schema(
        gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        Inner::<T>::json_schema(gen)
    }
}

impl<T: IdMappable> Serialize for IdMap<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl<'de, T: IdMappable> Deserialize<'de> for IdMap<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IdCheckVisitor<T>(PhantomData<T>);

        impl<'d, T: IdMappable> Visitor<'d> for IdCheckVisitor<T> {
            type Value = IdMap<T>;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a map keyed by ID")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'d>,
            {
                let mut inner = Inner::<T>::new();
                while let Some((key, value)) = map.next_entry::<T::Id, T>()? {
                    if key != value.id() {
                        return Err(A::Error::custom(format!(
                            "invalid map: key {} maps to value with ID {}",
                            key,
                            value.id()
                        )));
                    }
                    if let Some(old) = inner.insert(key, value) {
                        return Err(A::Error::custom(format!(
                            "invalid map: duplicate key {}",
                            old.id()
                        )));
                    }
                }
                Ok(IdMap { inner })
            }
        }

        deserializer.deserialize_map(IdCheckVisitor(PhantomData))
    }
}

impl<'a, T: IdMappable + 'a> Diffable<'a> for IdMap<T> {
    type Diff = BTreeMap<&'a T::Id, diffus::edit::map::Edit<'a, T>>;

    fn diff(&'a self, other: &'a Self) -> Edit<'a, Self> {
        match self.inner.diff(&other.inner) {
            Edit::Copy(_) => Edit::Copy(self),
            Edit::Change(change) => Edit::Change(change),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diffus::Diffus;
    use test_strategy::proptest;
    use test_strategy::Arbitrary;

    #[derive(
        Debug,
        Clone,
        PartialEq,
        Eq,
        Arbitrary,
        JsonSchema,
        Serialize,
        Deserialize,
        Diffus,
    )]
    struct TestEntry {
        id: u8,
        val1: u32,
        val2: u32,
    }

    impl IdMappable for TestEntry {
        type Id = u8;

        fn id(&self) -> Self::Id {
            self.id
        }
    }

    #[proptest]
    fn serialization_roundtrip(values: Vec<TestEntry>) {
        let map: IdMap<_> = values.into_iter().collect();

        let serialized = serde_json::to_string(&map).unwrap();
        let deserialized: IdMap<TestEntry> =
            serde_json::from_str(&serialized).unwrap();

        assert_eq!(map, deserialized);
    }

    #[proptest]
    fn serialization_is_transparent(values: Vec<TestEntry>) {
        let map: IdMap<_> = values.into_iter().collect();

        let serialized = serde_json::to_string(&map).unwrap();
        let serialized_inner = serde_json::to_string(&map.inner).unwrap();

        assert_eq!(serialized, serialized_inner);
    }

    #[test]
    fn deserialize_rejects_mismatched_keys() {
        let mut map = IdMap::<TestEntry>::new();
        map.insert(TestEntry { id: 1, val1: 2, val2: 3 });

        let mut entries = map.inner;
        entries.get_mut(&1).unwrap().id = 2;

        let serialized = serde_json::to_string(&entries).unwrap();
        let err =
            serde_json::from_str::<IdMap<TestEntry>>(&serialized).unwrap_err();
        let err = err.to_string();

        assert!(
            err.contains("key 1 maps to value with ID 2"),
            "unexpected error message: {err:?}"
        );
    }

    #[test]
    fn deserialize_rejects_duplicates() {
        let serialized = r#"
        {
            "1": {"id": 1, "val1": 2, "val2": 3},
            "1": {"id": 1, "val1": 2, "val2": 3}
        }
        "#;

        let err =
            serde_json::from_str::<IdMap<TestEntry>>(&serialized).unwrap_err();
        let err = err.to_string();

        assert!(
            err.contains("duplicate key 1"),
            "unexpected error message: {err:?}"
        );
    }
}
