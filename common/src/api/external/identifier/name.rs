use std::{collections::BTreeMap, convert::TryFrom, str::FromStr};

use parse_display::Display;
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// TODO wire up regex
const NAME_PATTERN: &str = r"^[a-z][a-zA-Z0-9-]{1,62}[a-zA-Z0-9]$";

#[derive(Error, Debug)]
pub enum NameError {
    #[error("Name {0} is a UUID which is disallowed to avoid conflicts. Add a prefix or postfix to differentiate it.")]
    UuidConflict(String),
    #[error("Name may contain at most 63 characters, `{0}` has {}", .0.len())]
    TooLong(String),
    #[error("Name requires at least one character")]
    Empty,
    #[error("Expected name `{0}` to begin with an ASCII lowercase character but got `{1}`")]
    InvalidFirstCharacter(String, char),
    #[error("Name `{0}` contains invalid character `{1}` (only lowercase ASCII, digits, and \"-\" are allowed)")]
    InvalidCharacter(String, char),
    #[error("Name `{0}` ends with `{1}` which is invalid")]
    InvalidLastCharacter(String, char),
}

/**
 * A name used in the API
 *
 * Names are generally user-provided unique identifiers, highly constrained as
 * described in RFD 4.  An `Name` can only be constructed with a string
 * that's valid as a name.
 */
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
pub struct Name(String);

/**
 * `Name::try_from(String)` is the primary method for constructing an Name
 * from an input string.  This validates the string according to our
 * requirements for a name.
 * TODO-cleanup why shouldn't callers use TryFrom<&str>?
 */
impl TryFrom<String> for Name {
    type Error = NameError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if super::is_uuid(value) {
            return Err(NameError::UuidConflict(value));
        }

        if value.len() > 63 {
            return Err(NameError::TooLong(value));
        }

        let mut iter = value.chars();

        let first = iter.next().ok_or(NameError::Empty)?;
        if !first.is_ascii_lowercase() {
            return Err(NameError::InvalidFirstCharacter(value, first));
        }

        let mut last = first;
        for c in iter {
            last = c;

            if !c.is_ascii_lowercase() && !c.is_digit(10) && c != '-' {
                return Err(NameError::InvalidCharacter(value, c));
            }
        }

        if last == '-' {
            return Err(NameError::InvalidLastCharacter(value, last));
        }

        Ok(Name(value))
    }
}

impl FromStr for Name {
    type Err = NameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Name::try_from(String::from(value))
    }
}

impl<'a> From<&'a Name> for &'a str {
    fn from(n: &'a Name) -> Self {
        n.as_str()
    }
}

/**
 * `Name` instances are comparable like Strings, primarily so that they can
 * be used as keys in trees.
 */
impl<S> PartialEq<S> for Name
where
    S: AsRef<str>,
{
    fn eq(&self, other: &S) -> bool {
        self.0 == other.as_ref()
    }
}

/**
 * Custom JsonSchema implementation to encode the constraints on Name
 */
/*
 * TODO: 1. make this part of schemars w/ rename and maxlen annotations
 * TODO: 2. integrate the regex with `try_from`
 */
impl JsonSchema for Name {
    fn schema_name() -> String {
        "Name".to_string()
    }
    fn json_schema(
        _gen: &mut schemars::gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                id: None,
                title: Some("A name used in the API".to_string()),
                description: Some(
                    "Names must begin with a lower case ASCII letter, be \
                     composed exclusively of lowercase ASCII, uppercase \
                     ASCII, numbers, and '-', and may not end with a '-'."
                        .to_string(),
                ),
                default: None,
                deprecated: false,
                read_only: false,
                write_only: false,
                examples: vec![],
            })),
            instance_type: Some(schemars::schema::SingleOrVec::Single(
                Box::new(schemars::schema::InstanceType::String),
            )),
            format: None,
            enum_values: None,
            const_value: None,
            subschemas: None,
            number: None,
            string: Some(Box::new(schemars::schema::StringValidation {
                max_length: Some(63),
                min_length: None,
                pattern: Some(NAME_PATTERN.to_string()),
            })),
            array: None,
            object: None,
            reference: None,
            extensions: BTreeMap::new(),
        })
    }
}

impl Name {
    /**
     * Return the `&str` representing the actual name.
     */
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
