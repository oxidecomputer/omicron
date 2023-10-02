// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for accessing SMF properties.

pub use crucible_smf::ScfError as InnerScfError;

#[derive(Debug, thiserror::Error)]
pub enum ScfError {
    #[error("failed to create scf handle: {0}")]
    ScfHandle(InnerScfError),
    #[error("failed to get self smf instance: {0}")]
    SelfInstance(InnerScfError),
    #[error("failed to get self running snapshot: {0}")]
    RunningSnapshot(InnerScfError),
    #[error("failed to get propertygroup `{group}`: {err}")]
    GetPg { group: &'static str, err: InnerScfError },
    #[error("missing propertygroup `{group}`")]
    MissingPg { group: &'static str },
    #[error("failed to get property `{group}/{prop}`: {err}")]
    GetProperty { group: &'static str, prop: &'static str, err: InnerScfError },
    #[error("missing property `{group}/{prop}`")]
    MissingProperty { group: &'static str, prop: &'static str },
    #[error("failed to get value for `{group}/{prop}`: {err}")]
    GetValue { group: &'static str, prop: &'static str, err: InnerScfError },
    #[error("failed to get values for `{group}/{prop}`: {err}")]
    GetValues { group: &'static str, prop: &'static str, err: InnerScfError },
    #[error("failed to get value for `{group}/{prop}`")]
    MissingValue { group: &'static str, prop: &'static str },
    #[error("failed to get `{group}/{prop} as a string: {err}")]
    ValueAsString {
        group: &'static str,
        prop: &'static str,
        err: InnerScfError,
    },
}

pub struct ScfHandle {
    inner: crucible_smf::Scf,
}

impl ScfHandle {
    pub fn new() -> Result<Self, ScfError> {
        match crucible_smf::Scf::new() {
            Ok(inner) => Ok(Self { inner }),
            Err(err) => Err(ScfError::ScfHandle(err)),
        }
    }

    pub fn self_instance(&self) -> Result<ScfInstance<'_>, ScfError> {
        match self.inner.get_self_instance() {
            Ok(inner) => Ok(ScfInstance { inner }),
            Err(err) => Err(ScfError::SelfInstance(err)),
        }
    }
}

pub struct ScfInstance<'a> {
    inner: crucible_smf::Instance<'a>,
}

impl ScfInstance<'_> {
    pub fn running_snapshot(&self) -> Result<ScfSnapshot<'_>, ScfError> {
        match self.inner.get_running_snapshot() {
            Ok(inner) => Ok(ScfSnapshot { inner }),
            Err(err) => Err(ScfError::RunningSnapshot(err)),
        }
    }
}

pub struct ScfSnapshot<'a> {
    inner: crucible_smf::Snapshot<'a>,
}

impl ScfSnapshot<'_> {
    pub fn property_group(
        &self,
        group: &'static str,
    ) -> Result<ScfPropertyGroup<'_>, ScfError> {
        match self.inner.get_pg(group) {
            Ok(Some(inner)) => Ok(ScfPropertyGroup { group, inner }),
            Ok(None) => Err(ScfError::MissingPg { group }),
            Err(err) => Err(ScfError::GetPg { group, err }),
        }
    }
}

pub struct ScfPropertyGroup<'a> {
    group: &'static str,
    inner: crucible_smf::PropertyGroup<'a>,
}

impl ScfPropertyGroup<'_> {
    fn property<'a>(
        &'a self,
        prop: &'static str,
    ) -> Result<crucible_smf::Property<'a>, ScfError> {
        match self.inner.get_property(prop) {
            Ok(Some(prop)) => Ok(prop),
            Ok(None) => {
                Err(ScfError::MissingProperty { group: self.group, prop })
            }
            Err(err) => {
                Err(ScfError::GetProperty { group: self.group, prop, err })
            }
        }
    }

    pub fn value_as_string(
        &self,
        prop: &'static str,
    ) -> Result<String, ScfError> {
        let inner = self.property(prop)?;
        let value = inner
            .value()
            .map_err(|err| ScfError::GetValue { group: self.group, prop, err })?
            .ok_or(ScfError::MissingValue { group: self.group, prop })?;
        value.as_string().map_err(|err| ScfError::ValueAsString {
            group: self.group,
            prop,
            err,
        })
    }

    pub fn values_as_strings(
        &self,
        prop: &'static str,
    ) -> Result<Vec<String>, ScfError> {
        let inner = self.property(prop)?;
        let values = inner.values().map_err(|err| ScfError::GetValues {
            group: self.group,
            prop,
            err,
        })?;
        values
            .map(|value| {
                let value = value.map_err(|err| ScfError::GetValue {
                    group: self.group,
                    prop,
                    err,
                })?;
                value.as_string().map_err(|err| ScfError::ValueAsString {
                    group: self.group,
                    prop,
                    err,
                })
            })
            .collect()
    }
}
