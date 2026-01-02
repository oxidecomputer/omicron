// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::borrow::Cow;
use std::mem;

/// Generic support for scalar (primitive) values in the sled editor.
///
/// These values are only compared for equality.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ScalarEditor<T> {
    original: T,
    value: EditValue<T>,
}

impl<T> ScalarEditor<T> {
    pub(crate) fn new(original: T) -> Self {
        ScalarEditor { original, value: EditValue::Original }
    }

    pub(crate) fn value(&self) -> &T {
        match &self.value {
            EditValue::Original => &self.original,
            EditValue::Edited(value) => value,
        }
    }

    /// Set the value to a new one, returning the old value.
    ///
    /// The old value might either be owned or borrowed, depending on
    /// whether `set_value` was ever called before.
    pub(crate) fn set_value(&mut self, value: T) -> Cow<'_, T>
    where
        T: Clone,
    {
        match mem::replace(&mut self.value, EditValue::Edited(value)) {
            EditValue::Original => Cow::Borrowed(&self.original),
            EditValue::Edited(old_value) => Cow::Owned(old_value),
        }
    }

    pub(crate) fn finalize(self) -> T {
        match self.value {
            EditValue::Original => self.original,
            EditValue::Edited(value) => value,
        }
    }
}

impl<T: Eq> ScalarEditor<T> {
    pub(crate) fn is_modified(&self) -> bool {
        match &self.value {
            EditValue::Original => false,
            EditValue::Edited(value) => value != &self.original,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum EditValue<T> {
    Original,
    Edited(T),
}
