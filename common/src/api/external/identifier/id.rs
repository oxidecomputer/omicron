use std::{convert::TryFrom, str::FromStr};

use parse_display::Display;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum IdError {
    #[error("failed to parse Uuid")]
    ParsingError(#[from] uuid::Error),
}

/// An id used to refer to a resource
#[derive(
    Clone,
    Debug,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Display,
    Serialize,
    Deserialize,
)]
#[display("{0}")]
#[serde(try_from = "String")]
pub struct Id(Uuid);
impl From<Uuid> for Id {
    fn from(uuid: Uuid) -> Self {
        Id(uuid)
    }
}

impl TryFrom<String> for Id {
    type Error = IdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.parse::<Uuid>() {
            Ok(uuid) => Ok(Id(uuid)),
            Err(err) => Err(IdError::ParsingError(err)),
        }
    }
}

impl FromStr for Id {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Id::try_from(String::from(s))
    }
}
