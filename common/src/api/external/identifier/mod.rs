pub mod id;
pub mod name;

use std::{convert::TryFrom, str::FromStr};

pub use id::*;
pub use name::*;
use parse_display::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Display, PartialEq, Serialize, Deserialize, JsonSchema)]
#[display("{0}")]
#[serde(try_from = "String")]
pub enum Identifier {
    Id(Uuid),
    Name(Name),
}

impl TryFrom<String> for Identifier {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if is_uuid(&value) {
            Ok(Identifier::Id(value.parse()?))
        } else {
            Ok(Identifier::Name(value.parse()?))
        }
    }
}

impl FromStr for Identifier {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        Identifier::try_from(String::from(s))
    }
}

impl From<Identifier> for String {
    fn from(value: Identifier) -> Self {
        value.to_string()
    }
}

fn is_uuid(s: &String) -> bool {
    s.parse::<Uuid>().is_ok()
}
