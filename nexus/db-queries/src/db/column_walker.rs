// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CTE utility for iterating over all columns in a table.

use crate::db::raw_query_builder::TrustedStr;
use diesel::prelude::*;
use std::marker::PhantomData;

/// Used to iterate over a tuple of columns ("T").
///
/// Diesel exposes "AllColumns" as a tuple, which is difficult to iterate over
/// -- after all, all the types are distinct. However, each of these types
/// implements "Column", so we can use a macro to provide a
/// "convertion-to-iterator" implemenation for our expected tuples.
pub(crate) struct ColumnWalker<T> {
    remaining: PhantomData<T>,
}

pub type AllColumnsOf<T> = ColumnWalker<<T as diesel::Table>::AllColumns>;

impl<T> ColumnWalker<T> {
    pub const fn new() -> Self {
        Self { remaining: PhantomData }
    }
}

macro_rules! impl_column_walker {
    ( $len:literal $($column:ident)+ ) => (
        #[allow(dead_code)]
        impl<$($column: Column),+> ColumnWalker<($($column,)+)> {
            pub fn with_prefix(prefix: &'static str) -> TrustedStr {
                // This string is derived from:
                // - The "table" type, with associated columns, which
                // are not controlled by an arbitrary user, and
                // - The "prefix" type, which is a "&'static str" (AKA,
                // hopefully known at compile-time, and not leaked).
                TrustedStr::i_take_responsibility_for_validating_this_string(
                    [$([prefix, $column::NAME].join("."),)+].join(", ")
                )
            }
        }

        impl<$($column: Column),+> IntoIterator for ColumnWalker<($($column,)+)> {
            type Item = &'static str;
            type IntoIter = std::array::IntoIter<Self::Item, $len>;

            fn into_iter(self) -> Self::IntoIter {
                [$($column::NAME,)+].into_iter()
            }
        }
    );
}

// implementations for 1 - 32 columns
impl_column_walker! { 1 A }
impl_column_walker! { 2 A B }
impl_column_walker! { 3 A B C }
impl_column_walker! { 4 A B C D }
impl_column_walker! { 5 A B C D E }
impl_column_walker! { 6 A B C D E F }
impl_column_walker! { 7 A B C D E F G }
impl_column_walker! { 8 A B C D E F G H }
impl_column_walker! { 9 A B C D E F G H I }
impl_column_walker! { 10 A B C D E F G H I J }
impl_column_walker! { 11 A B C D E F G H I J K }
impl_column_walker! { 12 A B C D E F G H I J K L }
impl_column_walker! { 13 A B C D E F G H I J K L M }
impl_column_walker! { 14 A B C D E F G H I J K L M N }
impl_column_walker! { 15 A B C D E F G H I J K L M N O }
impl_column_walker! { 16 A B C D E F G H I J K L M N O P }
impl_column_walker! { 17 A B C D E F G H I J K L M N O P Q }
impl_column_walker! { 18 A B C D E F G H I J K L M N O P Q R }
impl_column_walker! { 19 A B C D E F G H I J K L M N O P Q R S }
impl_column_walker! { 20 A B C D E F G H I J K L M N O P Q R S T }
impl_column_walker! { 21 A B C D E F G H I J K L M N O P Q R S T U }
impl_column_walker! { 22 A B C D E F G H I J K L M N O P Q R S T U V }
impl_column_walker! { 23 A B C D E F G H I J K L M N O P Q R S T U V W }
impl_column_walker! { 24 A B C D E F G H I J K L M N O P Q R S T U V W X }
impl_column_walker! { 25 A B C D E F G H I J K L M N O P Q R S T U V W X Y }
impl_column_walker! { 26 A B C D E F G H I J K L M N O P Q R S T U V W X Y Z }
impl_column_walker! { 27 A B C D E F G H I J K L M N O P Q R S T U V W X Y Z A1 }
impl_column_walker! { 28 A B C D E F G H I J K L M N O P Q R S T U V W X Y Z A1 B1 }
impl_column_walker! { 29 A B C D E F G H I J K L M N O P Q R S T U V W X Y Z A1 B1 C1 }
impl_column_walker! { 30 A B C D E F G H I J K L M N O P Q R S T U V W X Y Z A1 B1 C1 D1 }
impl_column_walker! { 31 A B C D E F G H I J K L M N O P Q R S T U V W X Y Z A1 B1 C1 D1 E1 }
impl_column_walker! { 32 A B C D E F G H I J K L M N O P Q R S T U V W X Y Z A1 B1 C1 D1 E1 F1 }

#[cfg(test)]
mod test {
    use super::*;

    table! {
        test_schema.test_table (id) {
            id -> Uuid,
            value -> Int4,
            time_deleted -> Nullable<Timestamptz>,
        }
    }

    // We can convert all a tables columns into an iteratable format.
    #[test]
    fn test_walk_table() {
        let all_columns =
            ColumnWalker::<<test_table::table as Table>::AllColumns>::new();

        let mut iter = all_columns.into_iter();
        assert_eq!(iter.next(), Some("id"));
        assert_eq!(iter.next(), Some("value"));
        assert_eq!(iter.next(), Some("time_deleted"));
        assert_eq!(iter.next(), None);
    }

    // We can, if we want to, also make a ColumnWalker out of an arbitrary tuple
    // of columns.
    #[test]
    fn test_walk_columns() {
        let all_columns = ColumnWalker::<(
            test_table::columns::id,
            test_table::columns::value,
        )>::new();

        let mut iter = all_columns.into_iter();
        assert_eq!(iter.next(), Some("id"));
        assert_eq!(iter.next(), Some("value"));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_all_columns_with_prefix() {
        assert_eq!(
            AllColumnsOf::<test_table::table>::with_prefix("foo").as_str(),
            "foo.id, foo.value, foo.time_deleted"
        );
    }
}
