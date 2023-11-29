// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CTE utility for iterating over all columns in a table.

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

impl<T> ColumnWalker<T> {
    pub fn new() -> Self {
        Self { remaining: PhantomData }
    }
}

macro_rules! impl_column_walker {
    ( $($column:ident)+ ) => (
        impl<$($column: Column),+> IntoIterator for ColumnWalker<($($column,)+)> {
            type Item = &'static str;
            type IntoIter = std::vec::IntoIter<Self::Item>;

            fn into_iter(self) -> Self::IntoIter {
                // TODO: don't convert to vec? You'll need to figure
                // out how to state the type of IntoIter.
                [$($column::NAME,)+].to_vec().into_iter()
            }
        }
    );
}

// implementations for 1 - 32 columns
impl_column_walker! { A }
impl_column_walker! { A B }
impl_column_walker! { A B C }
impl_column_walker! { A B C D }
impl_column_walker! { A B C D E }
impl_column_walker! { A B C D E F }
impl_column_walker! { A B C D E F G }
impl_column_walker! { A B C D E F G H }
impl_column_walker! { A B C D E F G H I }
impl_column_walker! { A B C D E F G H I J }
impl_column_walker! { A B C D E F G H I J K }
impl_column_walker! { A B C D E F G H I J K L }
impl_column_walker! { A B C D E F G H I J K L M }
impl_column_walker! { A B C D E F G H I J K L M N }
impl_column_walker! { A B C D E F G H I J K L M N O }
impl_column_walker! { A B C D E F G H I J K L M N O P }
impl_column_walker! { A B C D E F G H I J K L M N O P Q }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T U }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T U V }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T U V W }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T U V W X }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T U V W X Y }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T U V W X Y Z }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T U V W X Y Z A1 }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T U V W X Y Z A1 B1 }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T U V W X Y Z A1 B1 C1 }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T U V W X Y Z A1 B1 C1 D1 }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T U V W X Y Z A1 B1 C1 D1 E1 }
impl_column_walker! { A B C D E F G H I J K L M N O P Q R S T U V W X Y Z A1 B1 C1 D1 E1 F1 }

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
}
