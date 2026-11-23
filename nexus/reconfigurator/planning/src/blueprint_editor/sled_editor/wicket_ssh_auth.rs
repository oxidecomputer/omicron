// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::scalar::ScalarEditor;
use omicron_generation_kinds::WicketSshAuthGeneration;
use sled_agent_types::inventory::WicketSshAuth;

#[derive(Debug, thiserror::Error)]
pub enum WicketSshAuthEditError {
    #[error(
        "tried to set older wicket SSH auth generation {older} \
         (current generation is {current})"
    )]
    OlderGeneration {
        current: WicketSshAuthGeneration,
        older: WicketSshAuthGeneration,
    },

    #[error(
        "tried to set wicket SSH auth to same generation \
         with different kind: current = {}, new = {}",
         current.display(), new.display(),
    )]
    SameGenerationDifferentKind { current: WicketSshAuth, new: WicketSshAuth },
}

#[derive(Debug)]
pub(super) struct WicketSshAuthEditor {
    inner: ScalarEditor<WicketSshAuth>,
}

impl WicketSshAuthEditor {
    pub fn new(current: WicketSshAuth) -> Self {
        Self { inner: ScalarEditor::new(current) }
    }

    pub fn set_value(
        &mut self,
        new_value: WicketSshAuth,
    ) -> Result<(), WicketSshAuthEditError> {
        let current_value = self.inner.value();

        // We should never go backwards in generations.
        if current_value.generation > new_value.generation {
            return Err(WicketSshAuthEditError::OlderGeneration {
                current: current_value.generation,
                older: new_value.generation,
            });
        }

        // Generations are immutable: we should never see the same generation
        // with a different `kind`.
        if current_value.generation == new_value.generation
            && current_value.kind != new_value.kind
        {
            return Err(WicketSshAuthEditError::SameGenerationDifferentKind {
                current: current_value.clone(),
                new: new_value,
            });
        }

        self.inner.set_value(new_value);
        Ok(())
    }

    pub fn is_modified(&self) -> bool {
        self.inner.is_modified()
    }

    pub fn finalize(self) -> WicketSshAuth {
        self.inner.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use sled_agent_types::inventory::WicketSshAuthKind;
    use std::collections::BTreeSet;

    fn public_keys(
        generation: WicketSshAuthGeneration,
        key: &str,
    ) -> WicketSshAuth {
        WicketSshAuth {
            generation,
            kind: WicketSshAuthKind::PublicKeys {
                authorized_keys: BTreeSet::from([key.to_string()]),
                allow_support_keys: false,
            },
        }
    }

    fn gen2() -> WicketSshAuthGeneration {
        WicketSshAuthGeneration::new().next()
    }

    #[test]
    fn newer_generation_is_accepted() {
        let new_value = public_keys(gen2(), "fake public key");
        let mut editor = WicketSshAuthEditor::new(WicketSshAuth::initial());
        assert!(!editor.is_modified());

        editor.set_value(new_value.clone()).expect("set newer generation");
        assert!(editor.is_modified());
        assert_eq!(editor.finalize(), new_value);
    }

    #[test]
    fn skipping_generations_is_accepted() {
        let new_value = public_keys(gen2().next().next(), "fake public key");
        let mut editor = WicketSshAuthEditor::new(WicketSshAuth::initial());

        editor.set_value(new_value.clone()).expect("set much newer generation");
        assert!(editor.is_modified());
        assert_eq!(editor.finalize(), new_value);
    }

    #[test]
    fn same_value_is_not_a_modification() {
        let current = public_keys(gen2(), "fake public key");
        let mut editor = WicketSshAuthEditor::new(current.clone());

        editor.set_value(current.clone()).expect("set identical value");
        assert!(!editor.is_modified());
        assert_eq!(editor.finalize(), current);
    }

    #[test]
    fn older_generation_is_rejected() {
        let current = public_keys(gen2(), "fake public key");
        let mut editor = WicketSshAuthEditor::new(current.clone());

        let err = editor
            .set_value(WicketSshAuth::initial())
            .expect_err("older generation should be rejected");
        assert_matches!(
            err,
            WicketSshAuthEditError::OlderGeneration { current, older }
                if current == older.next()
        );

        assert!(!editor.is_modified());
        assert_eq!(editor.finalize(), current);
    }

    #[test]
    fn same_generation_different_kind_is_rejected() {
        let current = public_keys(gen2(), "fake public key");
        let conflicting = public_keys(gen2(), "different fake public key");
        let mut editor = WicketSshAuthEditor::new(current.clone());

        let err = editor
            .set_value(conflicting.clone())
            .expect_err("same generation with different kind is rejected");
        assert_matches!(
            err,
            WicketSshAuthEditError::SameGenerationDifferentKind {
                current: err_current,
                new: err_new,
            } if err_current == current && err_new == conflicting
        );

        assert!(!editor.is_modified());
        assert_eq!(editor.finalize(), current);
    }
}
