// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::scalar::ScalarEditor;
use omicron_generation_kinds::WicketSshAuthGeneration;
use sled_agent_types::inventory::WicketSshAuth;
use sled_agent_types::inventory::WicketSshAuthKind;

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
        "tried to set wicket SSH auth to same generation ({generation}) \
         with different kind (current = {current:?}, new = {new:?})"
    )]
    SameGenerationDifferentKind {
        generation: WicketSshAuthGeneration,
        current: WicketSshAuthKind,
        new: WicketSshAuthKind,
    },
}

#[derive(Debug)]
pub(super) struct WicketSshAuthEditor {
    inner: ScalarEditor<WicketSshAuth>,
}

impl WicketSshAuthEditor {
    pub fn new(current: WicketSshAuth) -> Self {
        Self { inner: ScalarEditor::new(current) }
    }

    // TODO omicron#11821 Reconfigurator should call this as needed.
    #[allow(unused)]
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
                generation: current_value.generation,
                current: current_value.kind.clone(),
                new: new_value.kind,
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
