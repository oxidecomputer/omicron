// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use omicron_common::api::external;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "instance_state_v2", schema = "public"))]
    pub struct InstanceStateEnum;

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
        Serialize,
        Deserialize,
        strum::VariantArray,
    )]
    #[diesel(sql_type = InstanceStateEnum)]
    pub enum InstanceState;

    // Enum values
    Creating => b"creating"
    NoVmm => b"no_vmm"
    Vmm => b"vmm"
    Failed => b"failed"
    Destroyed => b"destroyed"
);

impl InstanceState {
    pub fn state(&self) -> external::InstanceState {
        external::InstanceState::from(*self)
    }

    pub fn label(&self) -> &'static str {
        match self {
            InstanceState::Creating => "creating",
            InstanceState::NoVmm => "no_VMM",
            InstanceState::Vmm => "VMM",
            InstanceState::Failed => "failed",
            InstanceState::Destroyed => "destroyed",
        }
    }

    pub const ALL_STATES: &'static [Self] =
        <Self as strum::VariantArray>::VARIANTS;
}

impl fmt::Display for InstanceState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.label())
    }
}

impl From<InstanceState> for omicron_common::api::external::InstanceState {
    fn from(value: InstanceState) -> Self {
        use omicron_common::api::external::InstanceState as Output;
        match value {
            InstanceState::Creating => Output::Creating,
            InstanceState::NoVmm => Output::Stopped,
            InstanceState::Vmm => Output::Running,
            InstanceState::Failed => Output::Failed,
            InstanceState::Destroyed => Output::Destroyed,
        }
    }
}

impl std::str::FromStr for InstanceState {
    type Err = InstanceStateParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for &v in Self::ALL_STATES {
            if s.eq_ignore_ascii_case(v.label()) {
                return Ok(v);
            }
        }

        Err(InstanceStateParseError(()))
    }
}

impl diesel::query_builder::QueryId for InstanceStateEnum {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

#[derive(Debug, Eq, PartialEq)]
pub struct InstanceStateParseError(());

impl fmt::Display for InstanceStateParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "expected one of [")?;
        let mut variants = InstanceState::ALL_STATES.iter();
        if let Some(v) = variants.next() {
            write!(f, "{v}")?;
            for v in variants {
                write!(f, ", {v}")?;
            }
        }
        f.write_str("]")
    }
}

impl std::error::Error for InstanceStateParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_roundtrips() {
        for &variant in InstanceState::ALL_STATES {
            assert_eq!(Ok(dbg!(variant)), dbg!(variant.to_string().parse()));
        }
    }
}
