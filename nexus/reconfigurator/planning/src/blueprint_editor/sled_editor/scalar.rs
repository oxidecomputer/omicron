// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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

    pub(crate) fn set_value(&mut self, value: T) {
        self.value = EditValue::Edited(value);
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
