// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{fmt, str::FromStr};

use anyhow::{anyhow, Context, Result};
use omicron_common::update::ArtifactHash;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactIdsOpt {
    FromIpcc,
    Manual(ArtifactHashes),
}

impl ArtifactIdsOpt {
    pub(crate) fn resolve(&self) -> Result<ArtifactHashes> {
        match self {
            Self::FromIpcc => {
                todo!("obtain hashes from ipcc");
            }
            Self::Manual(hashes) => Ok(*hashes),
        }
    }
}

impl fmt::Display for ArtifactIdsOpt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArtifactIdsOpt::FromIpcc => f.write_str("from-ipcc"),
            ArtifactIdsOpt::Manual(hashes) => write!(f, "{}", hashes),
        }
    }
}

impl FromStr for ArtifactIdsOpt {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "from-ipcc" {
            return Ok(Self::FromIpcc);
        }

        let (host_phase_2,control_plane) = s.split_once(',').ok_or_else(|| anyhow!(
            "expected data in the format host_phase_2:<hash>,control_plane:<hash>")
        )?;

        fn parse_hash(
            input: &str,
            prefix: &str,
        ) -> Result<ArtifactHash, anyhow::Error> {
            let hash = input.strip_prefix(prefix).ok_or_else(|| {
                anyhow!("input `{input}` did not start with `{prefix}`")
            })?;
            hash.parse()
                .with_context(|| format!("input `{input}` has invalid hash"))
        }

        let host_phase_2 = parse_hash(host_phase_2, "host_phase_2:")?;
        let control_plane = parse_hash(control_plane, "control_plane:")?;

        Ok(Self::Manual(ArtifactHashes { host_phase_2, control_plane }))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct ArtifactHashes {
    pub(crate) host_phase_2: ArtifactHash,
    pub(crate) control_plane: ArtifactHash,
}

impl fmt::Display for ArtifactHashes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "host_phase_2:{},control_plane:{}",
            self.host_phase_2, self.control_plane
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_artifact_ids() -> Result<()> {
        let valid = [
            ("from-ipcc", ArtifactIdsOpt::FromIpcc),
            (
                "host_phase_2:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef,\
                 control_plane:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
                ArtifactIdsOpt::Manual(ArtifactHashes {
                    host_phase_2: ArtifactHash(
                        hex_literal::hex!(
                            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        ),
                    ),
                    control_plane: ArtifactHash(
                        hex_literal::hex!(
                            "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
                        ),
                    ),
                }),
            ),
        ];

        for (input, expected) in valid {
            let actual =
                input.parse::<ArtifactIdsOpt>().with_context(|| {
                    format!("for input `{input}`, parsing failed")
                })?;
            assert_eq!(
                expected, actual,
                "for input `{input}`, expected output matches actual"
            );
        }

        let invalid = [
            "foobar",
            // Control plane hash missing.
            "host_phase_2:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            // Host phase 2 hash missing.
            "control_plane:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
            // The control plane hash is not the correct length.
            "host_phase_2:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef,\
             control_plane:fedcba9876543210fedcba9876543210fedcba9876543210fedcba987654321",
        ];

        for input in invalid {
            let _ = input.parse::<ArtifactIdsOpt>().map(|output| {
                panic!("for input `{input}`, expected an error but parsed output `{output:?}`")
            });
        }

        Ok(())
    }
}
