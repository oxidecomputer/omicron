// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing deployed software and configuration
//!
//! For more on this, see the crate-level documentation for
//! `nexus/reconfigurator/planning`.
//!
//! This lives in nexus/types because it's used by both nexus/db-model and
//! nexus/reconfigurator/planning.  (It could as well just live in
//! nexus/db-model, but nexus/reconfigurator/planning does not currently know
//! about nexus/db-model and it's convenient to separate these concerns.)

use crate::external_api::views::SledState;
use crate::internal_api::params::DnsConfigParams;
use crate::inventory::Collection;
pub use crate::inventory::OmicronZoneConfig;
pub use crate::inventory::OmicronZoneDataset;
pub use crate::inventory::OmicronZoneType;
pub use crate::inventory::OmicronZonesConfig;
pub use crate::inventory::SourceNatConfig;
pub use crate::inventory::ZpoolName;
use newtype_uuid::GenericUuid;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::ExternalIpUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::ZoneKind;
use slog_error_chain::SlogInlineError;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::net::AddrParseError;
use std::net::Ipv6Addr;
use strum::EnumIter;
use strum::IntoEnumIterator;
use thiserror::Error;
use uuid::Uuid;

mod planning_input;
mod zone_type;

pub use planning_input::DiskFilter;
pub use planning_input::OmicronZoneExternalFloatingAddr;
pub use planning_input::OmicronZoneExternalFloatingIp;
pub use planning_input::OmicronZoneExternalIp;
pub use planning_input::OmicronZoneExternalSnatIp;
pub use planning_input::OmicronZoneNic;
pub use planning_input::PlanningInput;
pub use planning_input::PlanningInputBuildError;
pub use planning_input::PlanningInputBuilder;
pub use planning_input::Policy;
pub use planning_input::SledDetails;
pub use planning_input::SledDisk;
pub use planning_input::SledFilter;
pub use planning_input::SledResources;
pub use planning_input::ZpoolFilter;
pub use zone_type::blueprint_zone_type;
pub use zone_type::BlueprintZoneType;

/// Describes a complete set of software and configuration for the system
// Blueprints are a fundamental part of how the system modifies itself.  Each
// blueprint completely describes all of the software and configuration
// that the control plane manages.  See the nexus/reconfigurator/planning
// crate-level documentation for details.
//
// Blueprints are different from policy.  Policy describes the things that an
// operator would generally want to control.  The blueprint describes the
// details of implementing that policy that an operator shouldn't have to deal
// with.  For example, the operator might write policy that says "I want
// 5 external DNS zones".  The system could then generate a blueprint that
// _has_ 5 external DNS zones on 5 specific sleds.  The blueprint includes all
// the details needed to achieve that, including which image these zones should
// run, which zpools their persistent data should be stored on, their public and
// private IP addresses, their internal DNS names, etc.
//
// It must be possible for multiple Nexus instances to execute the same
// blueprint concurrently and converge to the same thing.  Thus, these _cannot_
// be how a blueprint works:
//
// - "add a Nexus zone" -- two Nexus instances operating concurrently would
//   add _two_ Nexus zones (which is wrong)
// - "ensure that there is a Nexus zone on this sled with this id" -- the IP
//   addresses and images are left unspecified.  Two Nexus instances could pick
//   different IPs or images for the zone.
//
// This is why blueprints must be so detailed.  The key principle here is that
// **all the work of ensuring that the system do the right thing happens in one
// process (the update planner in one Nexus instance).  Once a blueprint has
// been committed, everyone is on the same page about how to execute it.**  The
// intent is that this makes both planning and executing a lot easier.  In
// particular, by the time we get to execution, all the hard choices have
// already been made.
//
// Currently, blueprints are limited to describing only the set of Omicron
// zones deployed on each host and some supporting configuration (e.g., DNS).
// This is aimed at supporting add/remove sleds.  The plan is to grow this to
// include more of the system as we support more use cases.
#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct Blueprint {
    /// unique identifier for this blueprint
    pub id: Uuid,

    /// A map of sled id -> desired state of the sled.
    ///
    /// A sled is considered part of the control plane cluster iff it has an
    /// entry in this map.
    pub sled_state: BTreeMap<SledUuid, SledState>,

    /// A map of sled id -> zones deployed on each sled, along with the
    /// [`BlueprintZoneDisposition`] for each zone.
    ///
    /// Unlike `sled_state`, this map may contain entries for sleds that are no
    /// longer a part of the control plane cluster (e.g., sleds that have been
    /// decommissioned, but still have expunged zones where cleanup has not yet
    /// completed).
    pub blueprint_zones: BTreeMap<SledUuid, BlueprintZonesConfig>,

    /// A map of sled id -> disks in use on each sled.
    pub blueprint_disks: BTreeMap<SledUuid, BlueprintPhysicalDisksConfig>,

    /// which blueprint this blueprint is based on
    pub parent_blueprint_id: Option<Uuid>,

    /// internal DNS version when this blueprint was created
    // See blueprint execution for more on this.
    pub internal_dns_version: Generation,

    /// external DNS version when thi blueprint was created
    // See blueprint execution for more on this.
    pub external_dns_version: Generation,

    /// when this blueprint was generated (for debugging)
    pub time_created: chrono::DateTime<chrono::Utc>,
    /// identity of the component that generated the blueprint (for debugging)
    /// This would generally be the Uuid of a Nexus instance.
    pub creator: String,
    /// human-readable string describing why this blueprint was created
    /// (for debugging)
    pub comment: String,
}

impl Blueprint {
    /// Return metadata for this blueprint.
    pub fn metadata(&self) -> BlueprintMetadata {
        BlueprintMetadata {
            id: self.id,
            parent_blueprint_id: self.parent_blueprint_id,
            internal_dns_version: self.internal_dns_version,
            external_dns_version: self.external_dns_version,
            time_created: self.time_created,
            creator: self.creator.clone(),
            comment: self.comment.clone(),
        }
    }

    /// Iterate over the [`BlueprintZoneConfig`] instances in the blueprint
    /// that match the provided filter, along with the associated sled id.
    pub fn all_omicron_zones(
        &self,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = (SledUuid, &BlueprintZoneConfig)> {
        self.blueprint_zones.iter().flat_map(move |(sled_id, z)| {
            z.zones
                .iter()
                .filter(move |z| z.disposition.matches(filter))
                .map(|z| (*sled_id, z))
        })
    }

    /// Iterate over the [`BlueprintZoneConfig`] instances in the blueprint
    /// that do not match the provided filter, along with the associated sled
    /// id.
    pub fn all_omicron_zones_not_in(
        &self,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = (SledUuid, &BlueprintZoneConfig)> {
        self.blueprint_zones.iter().flat_map(move |(sled_id, z)| {
            z.zones
                .iter()
                .filter(move |z| !z.disposition.matches(filter))
                .map(|z| (*sled_id, z))
        })
    }

    /// Iterate over the ids of all sleds in the blueprint
    pub fn sleds(&self) -> impl Iterator<Item = SledUuid> + '_ {
        self.blueprint_zones.keys().copied()
    }

    /// Summarize the difference between sleds and zones between two
    /// blueprints.
    ///
    /// The argument provided is the "before" side, and `self` is the "after"
    /// side. This matches the order of arguments to
    /// [`Blueprint::diff_since_collection`].
    pub fn diff_since_blueprint(
        &self,
        before: &Blueprint,
    ) -> Result<BlueprintDiff, BlueprintDiffError> {
        BlueprintDiff::new(
            DiffBeforeMetadata::Blueprint(Box::new(before.metadata())),
            before
                .blueprint_zones
                .iter()
                .map(|(sled_id, zones)| (*sled_id, zones.clone().into()))
                .collect(),
            self.metadata(),
            self.blueprint_zones.clone(),
        )
    }

    /// Summarize the differences in sleds and zones between a collection and a
    /// blueprint.
    ///
    /// This gives an idea about what would change about a running system if
    /// one were to execute the blueprint.
    ///
    /// Note that collections do not include information about zone
    /// disposition, so it is assumed that all zones in the collection have the
    /// [`InService`](BlueprintZoneDisposition::InService) disposition.
    pub fn diff_since_collection(
        &self,
        before: &Collection,
    ) -> Result<BlueprintDiff, BlueprintDiffError> {
        let before_zones = before
            .omicron_zones
            .iter()
            .map(|(sled_id, zones_found)| {
                (*sled_id, zones_found.zones.clone().into())
            })
            .collect();

        BlueprintDiff::new(
            DiffBeforeMetadata::Collection { id: before.id },
            before_zones,
            self.metadata(),
            self.blueprint_zones.clone(),
        )
    }

    /// Return a struct that can be displayed to present information about the
    /// blueprint.
    pub fn display(&self) -> BlueprintDisplay<'_> {
        BlueprintDisplay { blueprint: self }
    }
}

/// Wrapper to allow a [`Blueprint`] to be displayed with information.
///
/// Returned by [`Blueprint::display()`].
#[derive(Clone, Debug)]
#[must_use = "this struct does nothing unless displayed"]
pub struct BlueprintDisplay<'a> {
    blueprint: &'a Blueprint,
    // TODO: add colorization with a stylesheet
}

impl<'a> fmt::Display for BlueprintDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = self.blueprint;
        writeln!(f, "blueprint  {}", b.id)?;
        writeln!(
            f,
            "parent:    {}",
            b.parent_blueprint_id
                .map(|u| u.to_string())
                .unwrap_or_else(|| String::from("<none>"))
        )?;

        writeln!(f, "\n{}", self.make_zone_table())?;

        writeln!(f, "\n{}", table_display::metadata_heading())?;
        writeln!(f, "{}", self.make_metadata_table())?;

        Ok(())
    }
}

/// Information about an Omicron zone as recorded in a blueprint.
///
/// Currently, this is similar to [`OmicronZonesConfig`], but also contains a
/// per-zone [`BlueprintZoneDisposition`].
///
/// Part of [`Blueprint`].
#[derive(Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct BlueprintZonesConfig {
    /// Generation number of this configuration.
    ///
    /// This generation number is owned by the control plane. See
    /// [`OmicronZonesConfig::generation`] for more details.
    pub generation: Generation,

    /// The list of running zones.
    pub zones: Vec<BlueprintZoneConfig>,
}

impl From<BlueprintZonesConfig> for OmicronZonesConfig {
    fn from(config: BlueprintZonesConfig) -> Self {
        Self {
            generation: config.generation,
            zones: config.zones.into_iter().map(From::from).collect(),
        }
    }
}

impl BlueprintZonesConfig {
    /// Sorts the list of zones stored in this configuration.
    ///
    /// This is not strictly necessary. But for testing (particularly snapshot
    /// testing), it's helpful for zones to be in sorted order.
    pub fn sort(&mut self) {
        self.zones.sort_unstable_by_key(zone_sort_key);
    }

    /// Converts self to an [`OmicronZonesConfig`], applying the provided
    /// [`BlueprintZoneFilter`].
    ///
    /// The filter controls which zones should be exported into the resulting
    /// [`OmicronZonesConfig`].
    pub fn to_omicron_zones_config(
        &self,
        filter: BlueprintZoneFilter,
    ) -> OmicronZonesConfig {
        OmicronZonesConfig {
            generation: self.generation,
            zones: self
                .zones
                .iter()
                .filter(|z| z.disposition.matches(filter))
                .cloned()
                .map(OmicronZoneConfig::from)
                .collect(),
        }
    }

    /// Returns true if all zones in the blueprint have a disposition of
    // `Expunged`, false otherwise.
    pub fn are_all_zones_expunged(&self) -> bool {
        self.zones
            .iter()
            .all(|c| c.disposition == BlueprintZoneDisposition::Expunged)
    }
}

trait ZoneSortKey {
    fn kind(&self) -> ZoneKind;
    fn id(&self) -> OmicronZoneUuid;
}

impl ZoneSortKey for BlueprintZoneConfig {
    fn kind(&self) -> ZoneKind {
        self.zone_type.kind()
    }

    fn id(&self) -> OmicronZoneUuid {
        self.id
    }
}

impl ZoneSortKey for OmicronZoneConfig {
    fn kind(&self) -> ZoneKind {
        self.zone_type.kind()
    }

    fn id(&self) -> OmicronZoneUuid {
        OmicronZoneUuid::from_untyped_uuid(self.id)
    }
}

impl ZoneSortKey for BlueprintOrCollectionZoneConfig {
    fn kind(&self) -> ZoneKind {
        BlueprintOrCollectionZoneConfig::kind(self)
    }

    fn id(&self) -> OmicronZoneUuid {
        BlueprintOrCollectionZoneConfig::id(self)
    }
}

fn zone_sort_key<T: ZoneSortKey>(z: &T) -> impl Ord {
    // First sort by kind, then by ID. This makes it so that zones of the same
    // kind (e.g. Crucible zones) are grouped together.
    (z.kind(), z.id())
}

/// "Should never happen" errors from converting an [`OmicronZoneType`] into a
/// [`BlueprintZoneType`].
// Removing this error type would be a side effect of fixing
// https://github.com/oxidecomputer/omicron/issues/4988.
#[derive(Debug, Clone, Error, SlogInlineError)]
pub enum InvalidOmicronZoneType {
    #[error("invalid socket address for Omicron zone {kind} ({addr})")]
    ParseSocketAddr {
        kind: ZoneKind,
        addr: String,
        #[source]
        err: AddrParseError,
    },
    #[error("Omicron zone {kind} requires an external IP ID")]
    ExternalIpIdRequired { kind: ZoneKind },
}

/// Describes one Omicron-managed zone in a blueprint.
///
/// Part of [`BlueprintZonesConfig`].
#[derive(Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct BlueprintZoneConfig {
    /// The disposition (desired state) of this zone recorded in the blueprint.
    pub disposition: BlueprintZoneDisposition,

    pub id: OmicronZoneUuid,
    pub underlay_address: Ipv6Addr,
    pub zone_type: BlueprintZoneType,
}

impl BlueprintZoneConfig {
    /// Convert from an [`OmicronZoneConfig`].
    ///
    /// This method is annoying to call correctly and will become more so over
    /// time. Ideally we'd remove all callers and then remove this method, but
    /// for now we keep it.
    ///
    /// # Errors
    ///
    /// If `config.zone_type` is a zone that has an external IP address (Nexus,
    /// boundary NTP, external DNS), `external_ip_id` must be `Some(_)` or this
    /// method will return an error.
    pub fn from_omicron_zone_config(
        config: OmicronZoneConfig,
        disposition: BlueprintZoneDisposition,
        external_ip_id: Option<ExternalIpUuid>,
    ) -> Result<Self, InvalidOmicronZoneType> {
        let kind = config.zone_type.kind();
        let zone_type = match config.zone_type {
            OmicronZoneType::BoundaryNtp {
                address,
                dns_servers,
                domain,
                nic,
                ntp_servers,
                snat_cfg,
            } => {
                let external_ip_id = external_ip_id.ok_or(
                    InvalidOmicronZoneType::ExternalIpIdRequired { kind },
                )?;
                let address = address.parse().map_err(|err| {
                    InvalidOmicronZoneType::ParseSocketAddr {
                        kind,
                        addr: address.clone(),
                        err,
                    }
                })?;
                BlueprintZoneType::BoundaryNtp(
                    blueprint_zone_type::BoundaryNtp {
                        address,
                        ntp_servers,
                        dns_servers,
                        domain,
                        nic,
                        external_ip: OmicronZoneExternalSnatIp {
                            id: external_ip_id,
                            snat_cfg,
                        },
                    },
                )
            }
            OmicronZoneType::Clickhouse { address, dataset } => {
                let address = address.parse().map_err(|err| {
                    InvalidOmicronZoneType::ParseSocketAddr {
                        kind,
                        addr: address.clone(),
                        err,
                    }
                })?;
                BlueprintZoneType::Clickhouse(blueprint_zone_type::Clickhouse {
                    address,
                    dataset,
                })
            }
            OmicronZoneType::ClickhouseKeeper { address, dataset } => {
                let address = address.parse().map_err(|err| {
                    InvalidOmicronZoneType::ParseSocketAddr {
                        kind,
                        addr: address.clone(),
                        err,
                    }
                })?;
                BlueprintZoneType::ClickhouseKeeper(
                    blueprint_zone_type::ClickhouseKeeper { address, dataset },
                )
            }
            OmicronZoneType::CockroachDb { address, dataset } => {
                let address = address.parse().map_err(|err| {
                    InvalidOmicronZoneType::ParseSocketAddr {
                        kind,
                        addr: address.clone(),
                        err,
                    }
                })?;
                BlueprintZoneType::CockroachDb(
                    blueprint_zone_type::CockroachDb { address, dataset },
                )
            }
            OmicronZoneType::Crucible { address, dataset } => {
                let address = address.parse().map_err(|err| {
                    InvalidOmicronZoneType::ParseSocketAddr {
                        kind,
                        addr: address.clone(),
                        err,
                    }
                })?;
                BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                    address,
                    dataset,
                })
            }
            OmicronZoneType::CruciblePantry { address } => {
                let address = address.parse().map_err(|err| {
                    InvalidOmicronZoneType::ParseSocketAddr {
                        kind,
                        addr: address.clone(),
                        err,
                    }
                })?;
                BlueprintZoneType::CruciblePantry(
                    blueprint_zone_type::CruciblePantry { address },
                )
            }
            OmicronZoneType::ExternalDns {
                dataset,
                dns_address,
                http_address,
                nic,
            } => {
                let external_ip_id = external_ip_id.ok_or(
                    InvalidOmicronZoneType::ExternalIpIdRequired { kind },
                )?;
                let dns_address = dns_address.parse().map_err(|err| {
                    InvalidOmicronZoneType::ParseSocketAddr {
                        kind,
                        addr: dns_address.clone(),
                        err,
                    }
                })?;
                let http_address = http_address.parse().map_err(|err| {
                    InvalidOmicronZoneType::ParseSocketAddr {
                        kind,
                        addr: http_address.clone(),
                        err,
                    }
                })?;
                BlueprintZoneType::ExternalDns(
                    blueprint_zone_type::ExternalDns {
                        dataset,
                        http_address,
                        dns_address: OmicronZoneExternalFloatingAddr {
                            id: external_ip_id,
                            addr: dns_address,
                        },
                        nic,
                    },
                )
            }
            OmicronZoneType::InternalDns {
                dataset,
                dns_address,
                gz_address,
                gz_address_index,
                http_address,
            } => {
                let dns_address = dns_address.parse().map_err(|err| {
                    InvalidOmicronZoneType::ParseSocketAddr {
                        kind,
                        addr: dns_address.clone(),
                        err,
                    }
                })?;
                let http_address = http_address.parse().map_err(|err| {
                    InvalidOmicronZoneType::ParseSocketAddr {
                        kind,
                        addr: http_address.clone(),
                        err,
                    }
                })?;
                BlueprintZoneType::InternalDns(
                    blueprint_zone_type::InternalDns {
                        dataset,
                        http_address,
                        dns_address,
                        gz_address,
                        gz_address_index,
                    },
                )
            }
            OmicronZoneType::InternalNtp {
                address,
                dns_servers,
                domain,
                ntp_servers,
            } => {
                let address = address.parse().map_err(|err| {
                    InvalidOmicronZoneType::ParseSocketAddr {
                        kind,
                        addr: address.clone(),
                        err,
                    }
                })?;
                BlueprintZoneType::InternalNtp(
                    blueprint_zone_type::InternalNtp {
                        address,
                        ntp_servers,
                        dns_servers,
                        domain,
                    },
                )
            }
            OmicronZoneType::Nexus {
                external_dns_servers,
                external_ip,
                external_tls,
                internal_address,
                nic,
            } => {
                let external_ip_id = external_ip_id.ok_or(
                    InvalidOmicronZoneType::ExternalIpIdRequired { kind },
                )?;
                let internal_address =
                    internal_address.parse().map_err(|err| {
                        InvalidOmicronZoneType::ParseSocketAddr {
                            kind,
                            addr: internal_address.clone(),
                            err,
                        }
                    })?;
                BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    internal_address,
                    external_ip: OmicronZoneExternalFloatingIp {
                        id: external_ip_id,
                        ip: external_ip,
                    },
                    nic,
                    external_tls,
                    external_dns_servers,
                })
            }
            OmicronZoneType::Oximeter { address } => {
                let address = address.parse().map_err(|err| {
                    InvalidOmicronZoneType::ParseSocketAddr {
                        kind,
                        addr: address.clone(),
                        err,
                    }
                })?;
                BlueprintZoneType::Oximeter(blueprint_zone_type::Oximeter {
                    address,
                })
            }
        };
        Ok(Self {
            disposition,
            id: OmicronZoneUuid::from_untyped_uuid(config.id),
            underlay_address: config.underlay_address,
            zone_type,
        })
    }
}

impl From<BlueprintZoneConfig> for OmicronZoneConfig {
    fn from(z: BlueprintZoneConfig) -> Self {
        Self {
            id: z.id.into_untyped_uuid(),
            underlay_address: z.underlay_address,
            zone_type: z.zone_type.into(),
        }
    }
}

/// The desired state of an Omicron-managed zone in a blueprint.
///
/// Part of [`BlueprintZoneConfig`].
#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    JsonSchema,
    Deserialize,
    Serialize,
    EnumIter,
)]
#[serde(rename_all = "snake_case")]
pub enum BlueprintZoneDisposition {
    /// The zone is in-service.
    InService,

    /// The zone is not in service.
    Quiesced,

    /// The zone is permanently gone.
    Expunged,
}

impl BlueprintZoneDisposition {
    /// Returns true if the zone disposition matches this filter.
    pub fn matches(self, filter: BlueprintZoneFilter) -> bool {
        // This code could be written in three ways:
        //
        // 1. match self { match filter { ... } }
        // 2. match filter { match self { ... } }
        // 3. match (self, filter) { ... }
        //
        // We choose 1 here because we expect many filters and just a few
        // dispositions, and 1 is the easiest form to represent that.
        match self {
            Self::InService => match filter {
                BlueprintZoneFilter::All => true,
                BlueprintZoneFilter::ShouldBeRunning => true,
                BlueprintZoneFilter::ShouldBeExternallyReachable => true,
                BlueprintZoneFilter::ShouldBeInInternalDns => true,
                BlueprintZoneFilter::ShouldDeployVpcFirewallRules => true,
            },
            Self::Quiesced => match filter {
                BlueprintZoneFilter::All => true,

                // Quiesced zones are still running.
                BlueprintZoneFilter::ShouldBeRunning => true,

                // Quiesced zones should not have external resources -- we do
                // not want traffic to be directed to them.
                BlueprintZoneFilter::ShouldBeExternallyReachable => false,

                // Quiesced zones should not be exposed in DNS.
                BlueprintZoneFilter::ShouldBeInInternalDns => false,

                // Quiesced zones should get firewall rules.
                BlueprintZoneFilter::ShouldDeployVpcFirewallRules => true,
            },
            Self::Expunged => match filter {
                BlueprintZoneFilter::All => true,
                BlueprintZoneFilter::ShouldBeRunning => false,
                BlueprintZoneFilter::ShouldBeExternallyReachable => false,
                BlueprintZoneFilter::ShouldBeInInternalDns => false,
                BlueprintZoneFilter::ShouldDeployVpcFirewallRules => false,
            },
        }
    }

    /// Returns all zone dispositions that match the given filter.
    pub fn all_matching(
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = Self> {
        BlueprintZoneDisposition::iter().filter(move |&d| d.matches(filter))
    }
}

impl fmt::Display for BlueprintZoneDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Neither `write!(f, "...")` nor `f.write_str("...")` obey fill
            // and alignment (used above), but this does.
            BlueprintZoneDisposition::InService => "in service".fmt(f),
            BlueprintZoneDisposition::Quiesced => "quiesced".fmt(f),
            BlueprintZoneDisposition::Expunged => "expunged".fmt(f),
        }
    }
}

/// Filters that apply to blueprint zones.
///
/// This logic lives here rather than within the individual components making
/// decisions, so that this is easier to read.
///
/// The meaning of a particular filter should not be overloaded -- each time a
/// new use case wants to make a decision based on the zone disposition, a new
/// variant should be added to this enum.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum BlueprintZoneFilter {
    // ---
    // Prefer to keep this list in alphabetical order.
    // ---
    /// All zones.
    All,

    /// Zones that are desired to be in the RUNNING state
    ShouldBeRunning,

    /// Filter by zones that should have external IP and DNS resources.
    ShouldBeExternallyReachable,

    /// Filter by zones that should be in internal DNS.
    ShouldBeInInternalDns,

    /// Filter by zones that should be sent VPC firewall rules.
    ShouldDeployVpcFirewallRules,
}

/// Information about an Omicron physical disk as recorded in a blueprint.
///
/// Part of [`Blueprint`].
pub type BlueprintPhysicalDisksConfig =
    sled_agent_client::types::OmicronPhysicalDisksConfig;

pub type BlueprintPhysicalDiskConfig =
    sled_agent_client::types::OmicronPhysicalDiskConfig;

/// Describe high-level metadata about a blueprint
// These fields are a subset of [`Blueprint`], and include only the data we can
// quickly fetch from the main blueprint table (e.g., when listing all
// blueprints).
#[derive(Debug, Clone, Eq, PartialEq, JsonSchema, Serialize)]
pub struct BlueprintMetadata {
    /// unique identifier for this blueprint
    pub id: Uuid,

    /// which blueprint this blueprint is based on
    pub parent_blueprint_id: Option<Uuid>,
    /// internal DNS version when this blueprint was created
    pub internal_dns_version: Generation,
    /// external DNS version when this blueprint was created
    pub external_dns_version: Generation,

    /// when this blueprint was generated (for debugging)
    pub time_created: chrono::DateTime<chrono::Utc>,
    /// identity of the component that generated the blueprint (for debugging)
    /// This would generally be the Uuid of a Nexus instance.
    pub creator: String,
    /// human-readable string describing why this blueprint was created
    /// (for debugging)
    pub comment: String,
}

impl BlueprintMetadata {
    pub fn display_id(&self) -> String {
        format!("blueprint {}", self.id)
    }
}

/// Describes what blueprint, if any, the system is currently working toward
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
pub struct BlueprintTarget {
    /// id of the blueprint that the system is trying to make real
    pub target_id: Uuid,
    /// policy: should the system actively work towards this blueprint
    ///
    /// This should generally be left enabled.
    pub enabled: bool,
    /// when this blueprint was made the target
    pub time_made_target: chrono::DateTime<chrono::Utc>,
}

/// Specifies what blueprint, if any, the system should be working toward
#[derive(Deserialize, JsonSchema)]
pub struct BlueprintTargetSet {
    pub target_id: Uuid,
    pub enabled: bool,
}

/// Summarizes the differences between two blueprints
#[derive(Debug)]
pub struct BlueprintDiff {
    before_meta: DiffBeforeMetadata,
    after_meta: BlueprintMetadata,
    sleds: DiffSleds,
}

impl BlueprintDiff {
    /// Build a diff with the provided contents, verifying that the provided
    /// data is valid.
    fn new(
        before_meta: DiffBeforeMetadata,
        before_zones: BTreeMap<SledUuid, BlueprintOrCollectionZonesConfig>,
        after_meta: BlueprintMetadata,
        after_zones: BTreeMap<SledUuid, BlueprintZonesConfig>,
    ) -> Result<Self, BlueprintDiffError> {
        let mut errors = Vec::new();

        let sleds = DiffSleds::new(before_zones, after_zones, &mut errors);

        if errors.is_empty() {
            Ok(Self { before_meta, after_meta, sleds })
        } else {
            Err(BlueprintDiffError {
                before_meta,
                after_meta: Box::new(after_meta),
                errors,
            })
        }
    }

    /// Returns metadata about the source of the "before" data.
    pub fn before_meta(&self) -> &DiffBeforeMetadata {
        &self.before_meta
    }

    /// Returns metadata about the source of the "after" data.
    pub fn after_meta(&self) -> &BlueprintMetadata {
        &self.after_meta
    }

    /// Iterate over sleds only present in the second blueprint of a diff
    pub fn sleds_added(
        &self,
    ) -> impl ExactSizeIterator<Item = (SledUuid, &BlueprintZonesConfig)> + '_
    {
        self.sleds.added.iter().map(|(sled_id, zones)| (*sled_id, zones))
    }

    /// Iterate over sleds only present in the first blueprint of a diff
    pub fn sleds_removed(
        &self,
    ) -> impl ExactSizeIterator<
        Item = (SledUuid, &BlueprintOrCollectionZonesConfig),
    > + '_ {
        self.sleds.removed.iter().map(|(sled_id, zones)| (*sled_id, zones))
    }

    /// Iterate over sleds present in both blueprints in a diff that have
    /// changes.
    pub fn sleds_modified(
        &self,
    ) -> impl ExactSizeIterator<Item = (SledUuid, &DiffSledModified)> + '_ {
        self.sleds.modified.iter().map(|(sled_id, sled)| (*sled_id, sled))
    }

    /// Iterate over sleds present in both blueprints in a diff that have no
    /// changes.
    pub fn sleds_unchanged(
        &self,
    ) -> impl Iterator<Item = (SledUuid, &BlueprintZonesConfig)> + '_ {
        self.sleds.unchanged.iter().map(|(sled_id, zones)| (*sled_id, zones))
    }

    /// Return a struct that can be used to display the diff.
    pub fn display(&self) -> BlueprintDiffDisplay<'_> {
        BlueprintDiffDisplay::new(self)
    }
}

#[derive(Debug)]
struct DiffSleds {
    added: BTreeMap<SledUuid, BlueprintZonesConfig>,
    removed: BTreeMap<SledUuid, BlueprintOrCollectionZonesConfig>,
    modified: BTreeMap<SledUuid, DiffSledModified>,
    unchanged: BTreeMap<SledUuid, BlueprintZonesConfig>,
}

impl DiffSleds {
    /// Builds added, removed and common maps, verifying that the provided data
    /// is valid.
    ///
    /// The return value only contains the sleds that are present in both
    /// blueprints.
    fn new(
        before: BTreeMap<SledUuid, BlueprintOrCollectionZonesConfig>,
        mut after: BTreeMap<SledUuid, BlueprintZonesConfig>,
        errors: &mut Vec<BlueprintDiffSingleError>,
    ) -> Self {
        let mut removed = BTreeMap::new();
        let mut modified = BTreeMap::new();
        let mut unchanged = BTreeMap::new();

        for (sled_id, mut before_z) in before {
            if let Some(mut after_z) = after.remove(&sled_id) {
                // Sort before_z and after_z so they can be compared directly.
                before_z.sort();
                after_z.sort();

                if before_z == after_z {
                    unchanged.insert(sled_id, after_z);
                } else {
                    let sled_modified = DiffSledModified::new(
                        sled_id, before_z, after_z, errors,
                    );
                    modified.insert(sled_id, sled_modified);
                }
            } else {
                removed.insert(sled_id, before_z);
            }
        }

        // We removed everything common from `after` above, so anything left is
        // an added sled.
        Self { added: after, removed, modified, unchanged }
    }
}

/// Wrapper to allow a [`BlueprintDiff`] to be displayed.
///
/// Returned by [`BlueprintDiff::display()`].
#[derive(Clone, Debug)]
#[must_use = "this struct does nothing unless displayed"]
pub struct BlueprintDiffDisplay<'diff> {
    diff: &'diff BlueprintDiff,
    // TODO: add colorization with a stylesheet
}

impl<'diff> BlueprintDiffDisplay<'diff> {
    #[inline]
    fn new(diff: &'diff BlueprintDiff) -> Self {
        Self { diff }
    }
}

impl<'diff> fmt::Display for BlueprintDiffDisplay<'diff> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let diff = self.diff;

        // Print things differently based on whether the diff is between a
        // collection and a blueprint, or a blueprint and a blueprint.
        match &diff.before_meta {
            DiffBeforeMetadata::Collection { id } => {
                writeln!(
                    f,
                    "from: collection {}\n\
                     to:   blueprint  {}",
                    id, diff.after_meta.id,
                )?;
            }
            DiffBeforeMetadata::Blueprint(before) => {
                writeln!(
                    f,
                    "from: blueprint {}\n\
                     to:   blueprint {}",
                    before.id, diff.after_meta.id
                )?;
            }
        }

        writeln!(f, "\n{}", self.make_zone_diff_table())?;

        writeln!(f, "\n{}", table_display::metadata_diff_heading())?;
        writeln!(f, "{}", self.make_metadata_diff_table())?;

        Ok(())
    }
}

#[derive(Clone, Debug, Error)]
pub struct BlueprintDiffError {
    pub before_meta: DiffBeforeMetadata,
    pub after_meta: Box<BlueprintMetadata>,
    pub errors: Vec<BlueprintDiffSingleError>,
}

impl fmt::Display for BlueprintDiffError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "errors in diff between {} and {}:",
            self.before_meta.display_id(),
            self.after_meta.display_id()
        )?;
        for e in &self.errors {
            writeln!(f, "  - {}", e)?;
        }
        Ok(())
    }
}

/// An individual error within a [`BlueprintDiffError`].
#[derive(Clone, Debug)]
pub enum BlueprintDiffSingleError {
    /// The [`OmicronZoneType`] of a particular zone changed between the before
    /// and after blueprints.
    ///
    /// For a particular zone, the type should never change.
    ZoneTypeChanged {
        sled_id: SledUuid,
        zone_id: Uuid,
        before: ZoneKind,
        after: ZoneKind,
    },
    InvalidOmicronZoneType(InvalidOmicronZoneType),
}

impl fmt::Display for BlueprintDiffSingleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlueprintDiffSingleError::ZoneTypeChanged {
                sled_id,
                zone_id,
                before,
                after,
            } => write!(
                f,
                "on sled {sled_id}, zone {zone_id} changed type \
                 from {before} to {after}",
            ),
            BlueprintDiffSingleError::InvalidOmicronZoneType(err) => {
                write!(f, "invalid OmicronZoneType in collection: {err}")
            }
        }
    }
}

/// Data about the "before" version within a [`BlueprintDiff`].
#[derive(Clone, Debug)]
pub enum DiffBeforeMetadata {
    /// The diff was made from a collection.
    Collection { id: CollectionUuid },
    /// The diff was made from a blueprint.
    Blueprint(Box<BlueprintMetadata>),
}

impl DiffBeforeMetadata {
    pub fn display_id(&self) -> String {
        match self {
            DiffBeforeMetadata::Collection { id } => format!("collection {id}"),
            DiffBeforeMetadata::Blueprint(b) => b.display_id(),
        }
    }
}

/// Single sled's zones config for "before" version within a [`BlueprintDiff`].
#[derive(Clone, Debug)]
pub enum BlueprintOrCollectionZonesConfig {
    /// The diff was made from a collection.
    Collection(OmicronZonesConfig),
    /// The diff was made from a blueprint.
    Blueprint(BlueprintZonesConfig),
}

impl BlueprintOrCollectionZonesConfig {
    pub fn sort(&mut self) {
        match self {
            BlueprintOrCollectionZonesConfig::Collection(z) => {
                z.zones.sort_unstable_by_key(zone_sort_key)
            }
            BlueprintOrCollectionZonesConfig::Blueprint(z) => z.sort(),
        }
    }

    pub fn generation(&self) -> Generation {
        match self {
            BlueprintOrCollectionZonesConfig::Collection(z) => z.generation,
            BlueprintOrCollectionZonesConfig::Blueprint(z) => z.generation,
        }
    }

    pub fn zones(
        &self,
    ) -> Box<dyn Iterator<Item = BlueprintOrCollectionZoneConfig> + '_> {
        match self {
            BlueprintOrCollectionZonesConfig::Collection(zc) => {
                Box::new(zc.zones.iter().map(|z| z.clone().into()))
            }
            BlueprintOrCollectionZonesConfig::Blueprint(zc) => {
                Box::new(zc.zones.iter().map(|z| z.clone().into()))
            }
        }
    }
}

impl From<OmicronZonesConfig> for BlueprintOrCollectionZonesConfig {
    fn from(zc: OmicronZonesConfig) -> Self {
        Self::Collection(zc)
    }
}

impl From<BlueprintZonesConfig> for BlueprintOrCollectionZonesConfig {
    fn from(zc: BlueprintZonesConfig) -> Self {
        Self::Blueprint(zc)
    }
}

impl PartialEq<BlueprintZonesConfig> for BlueprintOrCollectionZonesConfig {
    fn eq(&self, other: &BlueprintZonesConfig) -> bool {
        match self {
            BlueprintOrCollectionZonesConfig::Collection(z) => {
                // BlueprintZonesConfig contains more information than
                // OmicronZonesConfig. We compare them by lowering the
                // BlueprintZonesConfig into an OmicronZonesConfig.
                let lowered = OmicronZonesConfig::from(other.clone());
                z.eq(&lowered)
            }
            BlueprintOrCollectionZonesConfig::Blueprint(z) => z.eq(other),
        }
    }
}

/// Single zone config for "before" version within a [`BlueprintDiff`].
#[derive(Clone, Debug)]
pub enum BlueprintOrCollectionZoneConfig {
    /// The diff was made from a collection.
    Collection(OmicronZoneConfig),
    /// The diff was made from a blueprint.
    Blueprint(BlueprintZoneConfig),
}

impl From<OmicronZoneConfig> for BlueprintOrCollectionZoneConfig {
    fn from(zc: OmicronZoneConfig) -> Self {
        Self::Collection(zc)
    }
}

impl From<BlueprintZoneConfig> for BlueprintOrCollectionZoneConfig {
    fn from(zc: BlueprintZoneConfig) -> Self {
        Self::Blueprint(zc)
    }
}

impl BlueprintOrCollectionZoneConfig {
    pub fn id(&self) -> OmicronZoneUuid {
        match self {
            BlueprintOrCollectionZoneConfig::Collection(z) => z.id(),
            BlueprintOrCollectionZoneConfig::Blueprint(z) => z.id(),
        }
    }

    pub fn kind(&self) -> ZoneKind {
        match self {
            BlueprintOrCollectionZoneConfig::Collection(z) => z.kind(),
            BlueprintOrCollectionZoneConfig::Blueprint(z) => z.kind(),
        }
    }

    pub fn disposition(&self) -> BlueprintZoneDisposition {
        match self {
            // All zones from inventory collection are assumed to be in-service.
            BlueprintOrCollectionZoneConfig::Collection(_) => {
                BlueprintZoneDisposition::InService
            }
            BlueprintOrCollectionZoneConfig::Blueprint(z) => z.disposition,
        }
    }

    pub fn underlay_address(&self) -> Ipv6Addr {
        match self {
            BlueprintOrCollectionZoneConfig::Collection(z) => {
                z.underlay_address
            }
            BlueprintOrCollectionZoneConfig::Blueprint(z) => z.underlay_address,
        }
    }

    pub fn is_zone_type_equal(&self, other: &BlueprintZoneType) -> bool {
        match self {
            BlueprintOrCollectionZoneConfig::Collection(z) => {
                // BlueprintZoneType contains more information than
                // OmicronZoneType. We compare them by lowering the
                // BlueprintZoneType into an OmicronZoneType.
                let lowered = OmicronZoneType::from(other.clone());
                z.zone_type == lowered
            }
            BlueprintOrCollectionZoneConfig::Blueprint(z) => {
                z.zone_type == *other
            }
        }
    }
}

/// Describes a sled that appeared on both sides of a diff and is changed.
#[derive(Clone, Debug)]
pub struct DiffSledModified {
    /// id of the sled
    pub sled_id: SledUuid,
    /// generation of the "zones" configuration on the left side
    pub generation_before: Generation,
    /// generation of the "zones" configuration on the right side
    pub generation_after: Generation,
    zones_added: Vec<BlueprintZoneConfig>,
    zones_removed: Vec<BlueprintOrCollectionZoneConfig>,
    zones_common: Vec<DiffZoneCommon>,
}

impl DiffSledModified {
    fn new(
        sled_id: SledUuid,
        before: BlueprintOrCollectionZonesConfig,
        after: BlueprintZonesConfig,
        errors: &mut Vec<BlueprintDiffSingleError>,
    ) -> Self {
        // Assemble separate summaries of the zones, indexed by zone id.
        let before_by_id: HashMap<_, _> =
            before.zones().map(|zone| (zone.id(), zone)).collect();
        let mut after_by_id: HashMap<_, _> =
            after.zones.into_iter().map(|zone| (zone.id, zone)).collect();

        let mut zones_removed = Vec::new();
        let mut zones_common = Vec::new();

        // Now go through each zone and compare them.
        for (zone_id, zone_before) in before_by_id {
            if let Some(zone_after) = after_by_id.remove(&zone_id) {
                let before_kind = zone_before.kind();
                let after_kind = zone_after.zone_type.kind();

                if before_kind != after_kind {
                    errors.push(BlueprintDiffSingleError::ZoneTypeChanged {
                        sled_id,
                        zone_id: zone_id.into_untyped_uuid(),
                        before: before_kind,
                        after: after_kind,
                    });
                } else {
                    let common = DiffZoneCommon { zone_before, zone_after };
                    zones_common.push(common);
                }
            } else {
                zones_removed.push(zone_before);
            }
        }

        // Since we removed common zones above, anything else exists only in
        // before and was therefore added.
        let mut zones_added: Vec<_> = after_by_id.into_values().collect();

        // Sort for test reproducibility.
        zones_added.sort_unstable_by_key(zone_sort_key);
        zones_removed.sort_unstable_by_key(zone_sort_key);
        zones_common.sort_unstable_by_key(|common| {
            // The ID is common by definition, and the zone type was already
            // verified to be the same above. So just sort by the sort key for
            // the before zone. (In case of errors, the result will be thrown
            // away anyway, so this is harmless.)
            zone_sort_key(&common.zone_before)
        });

        Self {
            sled_id,
            generation_before: before.generation(),
            generation_after: after.generation,
            zones_added,
            zones_removed,
            zones_common,
        }
    }

    /// Iterate over zones added between the blueprints
    pub fn zones_added(
        &self,
    ) -> impl ExactSizeIterator<Item = &BlueprintZoneConfig> + '_ {
        self.zones_added.iter()
    }

    /// Iterate over zones removed between the blueprints
    pub fn zones_removed(
        &self,
    ) -> impl ExactSizeIterator<Item = &BlueprintOrCollectionZoneConfig> + '_
    {
        self.zones_removed.iter()
    }

    /// Iterate over zones that are common to both blueprints
    pub fn zones_in_common(
        &self,
    ) -> impl ExactSizeIterator<Item = &DiffZoneCommon> + '_ {
        self.zones_common.iter()
    }

    /// Iterate over zones that changed between the blueprints
    pub fn zones_modified(&self) -> impl Iterator<Item = &DiffZoneCommon> + '_ {
        self.zones_in_common().filter(|z| z.is_modified())
    }

    /// Iterate over zones that did not change between the blueprints
    pub fn zones_unchanged(
        &self,
    ) -> impl Iterator<Item = &DiffZoneCommon> + '_ {
        self.zones_in_common().filter(|z| !z.is_modified())
    }
}

/// Describes a zone that was common to both sides of a diff
#[derive(Debug, Clone)]
pub struct DiffZoneCommon {
    /// full zone configuration before
    pub zone_before: BlueprintOrCollectionZoneConfig,
    /// full zone configuration after
    pub zone_after: BlueprintZoneConfig,
}

impl DiffZoneCommon {
    /// Returns true if there are any differences between `zone_before` and
    /// `zone_after`.
    ///
    /// This is equivalent to `config_changed() || disposition_changed()`.
    #[inline]
    pub fn is_modified(&self) -> bool {
        // state is smaller and easier to compare than config.
        self.disposition_changed() || self.config_changed()
    }

    /// Returns true if the zone configuration (excluding the disposition)
    /// changed.
    #[inline]
    pub fn config_changed(&self) -> bool {
        self.zone_before.underlay_address() != self.zone_after.underlay_address
            || !self.zone_before.is_zone_type_equal(&self.zone_after.zone_type)
    }

    /// Returns true if the [`BlueprintZoneDisposition`] for the zone changed.
    #[inline]
    pub fn disposition_changed(&self) -> bool {
        self.zone_before.disposition() != self.zone_after.disposition
    }
}

/// Encapsulates Reconfigurator state
///
/// This serialized from is intended for saving state from hand-constructed or
/// real, deployed systems and loading it back into a simulator or test suite
///
/// **This format is not stable.  It may change at any time without
/// backwards-compatibility guarantees.**
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnstableReconfiguratorState {
    pub planning_input: PlanningInput,
    pub collections: Vec<Collection>,
    pub blueprints: Vec<Blueprint>,
    pub internal_dns: BTreeMap<Generation, DnsConfigParams>,
    pub external_dns: BTreeMap<Generation, DnsConfigParams>,
    pub silo_names: Vec<omicron_common::api::external::Name>,
    pub external_dns_zone_names: Vec<String>,
}

/// Code to generate tables.
///
/// This is here because `tabled` has a number of generically-named types, and
/// we'd like to avoid name collisions with other types.
mod table_display {
    use super::*;
    use crate::sectioned_table::SectionSpacing;
    use crate::sectioned_table::StBuilder;
    use crate::sectioned_table::StSectionBuilder;
    use tabled::builder::Builder;
    use tabled::settings::object::Columns;
    use tabled::settings::Modify;
    use tabled::settings::Padding;
    use tabled::settings::Style;
    use tabled::Table;

    impl<'a> super::BlueprintDisplay<'a> {
        pub(super) fn make_zone_table(&self) -> Table {
            let blueprint_zones = &self.blueprint.blueprint_zones;
            let mut builder = StBuilder::new();
            builder.push_header_row(header_row());

            for (sled_id, sled_zones) in blueprint_zones {
                let heading = format!(
                    "{SLED_INDENT}sled {sled_id}: blueprint zones at generation {}",
                    sled_zones.generation
                );
                builder.make_section(
                    SectionSpacing::Always,
                    heading,
                    |section| {
                        for zone in &sled_zones.zones {
                            add_zone_record(
                                ZONE_INDENT.to_string(),
                                &zone.clone().into(),
                                section,
                            );
                        }

                        if section.is_empty() {
                            section.push_nested_heading(
                                SectionSpacing::IfNotFirst,
                                format!("{ZONE_HEAD_INDENT}{NO_ZONES_PARENS}"),
                            );
                        }
                    },
                );
            }

            builder.build()
        }

        pub(super) fn make_metadata_table(&self) -> Table {
            let mut builder = Builder::new();

            // Metadata is presented as a linear (top-to-bottom) table with a
            // small indent.

            builder.push_record(vec![
                METADATA_INDENT.to_string(),
                linear_table_label(&CREATED_BY),
                self.blueprint.creator.clone(),
            ]);

            builder.push_record(vec![
                METADATA_INDENT.to_string(),
                linear_table_label(&CREATED_AT),
                humantime::format_rfc3339_millis(
                    self.blueprint.time_created.into(),
                )
                .to_string(),
            ]);

            let comment = if self.blueprint.comment.is_empty() {
                NONE_PARENS.to_string()
            } else {
                self.blueprint.comment.clone()
            };

            builder.push_record(vec![
                METADATA_INDENT.to_string(),
                linear_table_label(&COMMENT),
                comment,
            ]);

            builder.push_record(vec![
                METADATA_INDENT.to_string(),
                linear_table_label(&INTERNAL_DNS_VERSION),
                self.blueprint.internal_dns_version.to_string(),
            ]);

            builder.push_record(vec![
                METADATA_INDENT.to_string(),
                linear_table_label(&EXTERNAL_DNS_VERSION),
                self.blueprint.external_dns_version.to_string(),
            ]);

            let mut table = builder.build();
            apply_linear_table_settings(&mut table);
            table
        }
    }

    impl<'diff> BlueprintDiffDisplay<'diff> {
        pub(super) fn make_zone_diff_table(&self) -> Table {
            let diff = self.diff;

            // Add the unchanged prefix to the zone indent since the first
            // column will be used as the prefix.
            let mut builder = StBuilder::new();
            builder.push_header_row(diff_header_row());

            // The order is:
            //
            // 1. Unchanged
            // 2. Removed
            // 3. Modified
            // 4. Added
            //
            // The idea behind the order is to (a) group all changes together
            // and (b) put changes towards the bottom, so people have to scroll
            // back less.
            //
            // Zones within a modified sled follow the same order. If you're
            // changing the order here, make sure to keep that in sync.

            // First, unchanged sleds.
            builder.make_section(
                SectionSpacing::Always,
                unchanged_sleds_heading(),
                |section| {
                    for (sled_id, sled_zones) in diff.sleds_unchanged() {
                        add_whole_sled_records(
                            sled_id,
                            &sled_zones.clone().into(),
                            WholeSledKind::Unchanged,
                            section,
                        );
                    }
                },
            );

            // Then, removed sleds.
            builder.make_section(
                SectionSpacing::Always,
                removed_sleds_heading(),
                |section| {
                    for (sled_id, sled_zones) in diff.sleds_removed() {
                        add_whole_sled_records(
                            sled_id,
                            sled_zones,
                            WholeSledKind::Removed,
                            section,
                        );
                    }
                },
            );

            // Then, modified sleds.
            builder.make_section(
                SectionSpacing::Always,
                modified_sleds_heading(),
                |section| {
                    // For sleds that are in common:
                    for (sled_id, modified) in diff.sleds_modified() {
                        add_modified_sled_records(sled_id, modified, section);
                    }
                },
            );

            // Finally, added sleds.
            builder.make_section(
                SectionSpacing::Always,
                added_sleds_heading(),
                |section| {
                    for (sled_id, sled_zones) in diff.sleds_added() {
                        add_whole_sled_records(
                            sled_id,
                            &sled_zones.clone().into(),
                            WholeSledKind::Added,
                            section,
                        );
                    }
                },
            );

            builder.build()
        }

        pub(super) fn make_metadata_diff_table(&self) -> Table {
            let diff = self.diff;
            let mut builder = Builder::new();

            // Metadata is presented as a linear (top-to-bottom) table with a
            // small indent.

            match &diff.before_meta {
                DiffBeforeMetadata::Collection { .. } => {
                    // Collections don't have DNS versions, so this is new.
                    builder.push_record(vec![
                        format!("{ADDED_PREFIX}{METADATA_DIFF_INDENT}"),
                        metadata_table_internal_dns(),
                        linear_table_modified(
                            &NOT_PRESENT_IN_COLLECTION_PARENS,
                            &diff.after_meta.internal_dns_version,
                        ),
                    ]);

                    builder.push_record(vec![
                        format!("{ADDED_PREFIX}{METADATA_DIFF_INDENT}"),
                        metadata_table_external_dns(),
                        linear_table_modified(
                            &NOT_PRESENT_IN_COLLECTION_PARENS,
                            &diff.after_meta.external_dns_version,
                        ),
                    ]);
                }
                DiffBeforeMetadata::Blueprint(before) => {
                    if before.internal_dns_version
                        != diff.after_meta.internal_dns_version
                    {
                        builder.push_record(vec![
                            format!("{MODIFIED_PREFIX}{METADATA_DIFF_INDENT}"),
                            metadata_table_internal_dns(),
                            linear_table_modified(
                                &before.internal_dns_version,
                                &diff.after_meta.internal_dns_version,
                            ),
                        ]);
                    } else {
                        builder.push_record(vec![
                            format!("{UNCHANGED_PREFIX}{METADATA_DIFF_INDENT}"),
                            metadata_table_internal_dns(),
                            linear_table_unchanged(
                                &before.internal_dns_version,
                            ),
                        ]);
                    };

                    if before.external_dns_version
                        != diff.after_meta.external_dns_version
                    {
                        builder.push_record(vec![
                            format!("{MODIFIED_PREFIX}{METADATA_DIFF_INDENT}"),
                            metadata_table_external_dns(),
                            linear_table_modified(
                                &before.external_dns_version,
                                &diff.after_meta.external_dns_version,
                            ),
                        ]);
                    } else {
                        builder.push_record(vec![
                            format!("{UNCHANGED_PREFIX}{METADATA_DIFF_INDENT}"),
                            metadata_table_external_dns(),
                            linear_table_unchanged(
                                &before.external_dns_version,
                            ),
                        ]);
                    };
                }
            }

            let mut table = builder.build();
            apply_linear_table_settings(&mut table);
            table
        }
    }

    fn add_whole_sled_records(
        sled_id: SledUuid,
        sled_zones: &BlueprintOrCollectionZonesConfig,
        kind: WholeSledKind,
        section: &mut StSectionBuilder,
    ) {
        let heading = format!(
            "{}{SLED_INDENT}sled {sled_id}: blueprint zones at generation {}",
            kind.prefix(),
            sled_zones.generation(),
        );
        let prefix = kind.prefix();
        let status = kind.status();
        section.make_subsection(SectionSpacing::Always, heading, |s2| {
            // Also add another section for zones.
            for zone in sled_zones.zones() {
                match status {
                    Some(status) => {
                        add_zone_record_with_status(
                            format!("{prefix}{ZONE_INDENT}"),
                            &zone,
                            status,
                            s2,
                        );
                    }
                    None => {
                        add_zone_record(
                            format!("{prefix}{ZONE_INDENT}"),
                            &zone,
                            s2,
                        );
                    }
                }
            }
        });
    }

    fn add_modified_sled_records(
        sled_id: SledUuid,
        modified: &DiffSledModified,
        section: &mut StSectionBuilder,
    ) {
        let (generation_heading, warning) =
            if modified.generation_before != modified.generation_after {
                (
                    format!(
                        "blueprint zones at generation: {} -> {}",
                        modified.generation_before, modified.generation_after,
                    ),
                    None,
                )
            } else {
                // Modified sleds should always see a generation bump.
                (
                    format!(
                        "blueprint zones at generation: {}",
                        modified.generation_before
                    ),
                    Some(format!(
                        "{WARNING_PREFIX}{ZONE_HEAD_INDENT}\
                     warning: generation should have changed"
                    )),
                )
            };

        let sled_heading =
            format!("{MODIFIED_PREFIX}{SLED_INDENT}sled {sled_id}: {generation_heading}");

        section.make_subsection(SectionSpacing::Always, sled_heading, |s2| {
            if let Some(warning) = warning {
                s2.push_nested_heading(SectionSpacing::Never, warning);
            }

            // The order is:
            //
            // 1. Unchanged
            // 2. Removed
            // 3. Modified
            // 4. Added
            //
            // The idea behind the order is to (a) group all changes together
            // and (b) put changes towards the bottom, so people have to scroll
            // back less.
            //
            // Sleds follow the same order. If you're changing the order here,
            // make sure to keep that in sync.

            // First, unchanged zones.
            for zone_unchanged in modified.zones_unchanged() {
                add_zone_record(
                    format!("{UNCHANGED_PREFIX}{ZONE_INDENT}"),
                    &zone_unchanged.zone_before,
                    s2,
                );
            }

            // Then, removed zones.
            for zone in modified.zones_removed() {
                add_zone_record_with_status(
                    format!("{REMOVED_PREFIX}{ZONE_INDENT}"),
                    zone,
                    REMOVED,
                    s2,
                );
            }

            // Then, modified zones.
            for zone_modified in modified.zones_modified() {
                add_modified_zone_records(zone_modified, s2);
            }

            // Finally, added zones.
            for zone in modified.zones_added() {
                add_zone_record_with_status(
                    format!("{ADDED_PREFIX}{ZONE_INDENT}"),
                    &zone.clone().into(),
                    ADDED,
                    s2,
                );
            }

            // If no rows were pushed, add a row indicating that for this sled.
            if s2.is_empty() {
                s2.push_nested_heading(
                    SectionSpacing::Never,
                    format!(
                        "{UNCHANGED_PREFIX}{ZONE_HEAD_INDENT}\
                             {NO_ZONES_PARENS}"
                    ),
                );
            }
        });
    }

    /// Add a zone record to this section.
    ///
    /// This is the meat-and-potatoes of the diff display.
    fn add_zone_record(
        first_column: String,
        zone: &BlueprintOrCollectionZoneConfig,
        section: &mut StSectionBuilder,
    ) {
        section.push_record(vec![
            first_column,
            zone.kind().to_string(),
            zone.id().to_string(),
            zone.disposition().to_string(),
            zone.underlay_address().to_string(),
        ]);
    }

    fn add_zone_record_with_status(
        first_column: String,
        zone: &BlueprintOrCollectionZoneConfig,
        status: &str,
        section: &mut StSectionBuilder,
    ) {
        section.push_record(vec![
            first_column,
            zone.kind().to_string(),
            zone.id().to_string(),
            zone.disposition().to_string(),
            zone.underlay_address().to_string(),
            status.to_string(),
        ]);
    }

    /// Add a change table for the zone to the section.
    ///
    /// For diffs, this contains a table of changes between two zone
    /// records.
    fn add_modified_zone_records(
        modified: &DiffZoneCommon,
        section: &mut StSectionBuilder,
    ) {
        // Negative record for the before.
        let before = &modified.zone_before;
        let after = &modified.zone_after;

        // Before record.
        add_zone_record_with_status(
            format!("{REMOVED_PREFIX}{ZONE_INDENT}"),
            &before,
            MODIFIED,
            section,
        );

        let mut what_changed = Vec::new();
        if !before.is_zone_type_equal(&after.zone_type) {
            what_changed.push(ZONE_TYPE_CONFIG);
        }
        if before.disposition() != after.disposition {
            what_changed.push(DISPOSITION);
        }
        if before.underlay_address() != after.underlay_address {
            what_changed.push(UNDERLAY_IP);
        }
        debug_assert!(
            !what_changed.is_empty(),
            "at least something should have changed:\n\
             before = {before:#?}\n\
             after = {after:#?}"
        );

        let record = vec![
            format!("{ADDED_PREFIX}{ZONE_INDENT}"),
            // First two columns of data are skipped over since they're
            // always the same (verified at diff construction time).
            format!(" {SUB_NOT_LAST}"),
            "".to_string(),
            after.disposition.to_string(),
            after.underlay_address.to_string(),
        ];
        section.push_record(record);

        section.push_spanned_row(format!(
            "{MODIFIED_PREFIX}{ZONE_INDENT}  \
                 {SUB_LAST} changed: {}",
            what_changed.join(", "),
        ));
    }

    #[derive(Copy, Clone, Debug)]
    enum WholeSledKind {
        Removed,
        Added,
        Unchanged,
    }

    impl WholeSledKind {
        fn prefix(self) -> char {
            match self {
                WholeSledKind::Removed => REMOVED_PREFIX,
                WholeSledKind::Added => ADDED_PREFIX,
                WholeSledKind::Unchanged => UNCHANGED_PREFIX,
            }
        }

        fn status(self) -> Option<&'static str> {
            match self {
                WholeSledKind::Removed => Some(REMOVED),
                WholeSledKind::Added => Some(ADDED),
                WholeSledKind::Unchanged => None,
            }
        }
    }

    // Apply settings for a table which has top-to-bottom rows, and a first
    // column with indents.
    fn apply_linear_table_settings(table: &mut Table) {
        table.with(Style::empty()).with(Padding::zero()).with(
            Modify::new(Columns::single(1))
                // Add an padding on the right of the label column to make the
                // table visually distinctive.
                .with(Padding::new(0, 2, 0, 0)),
        );
    }

    // ---
    // Heading and other definitions
    // ---

    // This aligns the heading with the first column of actual text.
    const H1_INDENT: &str = "  ";
    const SLED_HEAD_INDENT: &str = " ";
    const SLED_INDENT: &str = "  ";
    const ZONE_HEAD_INDENT: &str = "   ";
    // Due to somewhat mysterious reasons with how padding works with tabled,
    // this needs to be 3 columns wide rather than 4.
    const ZONE_INDENT: &str = "   ";
    const METADATA_INDENT: &str = "  ";
    const METADATA_DIFF_INDENT: &str = "   ";

    const ADDED_PREFIX: char = '+';
    const REMOVED_PREFIX: char = '-';
    const MODIFIED_PREFIX: char = '*';
    const UNCHANGED_PREFIX: char = ' ';
    const WARNING_PREFIX: char = '!';

    const ARROW: &str = "->";
    const SUB_NOT_LAST: &str = "├─";
    const SUB_LAST: &str = "└─";

    const ZONE_TYPE: &str = "zone type";
    const ZONE_ID: &str = "zone ID";
    const DISPOSITION: &str = "disposition";
    const UNDERLAY_IP: &str = "underlay IP";
    const ZONE_TYPE_CONFIG: &str = "zone type config";
    const STATUS: &str = "status";
    const REMOVED_SLEDS_HEADING: &str = "REMOVED SLEDS";
    const MODIFIED_SLEDS_HEADING: &str = "MODIFIED SLEDS";
    const UNCHANGED_SLEDS_HEADING: &str = "UNCHANGED SLEDS";
    const ADDED_SLEDS_HEADING: &str = "ADDED SLEDS";
    const REMOVED: &str = "removed";
    const ADDED: &str = "added";
    const MODIFIED: &str = "modified";

    const METADATA_HEADING: &str = "METADATA";
    const CREATED_BY: &str = "created by";
    const CREATED_AT: &str = "created at";
    const INTERNAL_DNS_VERSION: &str = "internal DNS version";
    const EXTERNAL_DNS_VERSION: &str = "external DNS version";
    const COMMENT: &str = "comment";

    const UNCHANGED_PARENS: &str = "(unchanged)";
    const NO_ZONES_PARENS: &str = "(no zones)";
    const NONE_PARENS: &str = "(none)";
    const NOT_PRESENT_IN_COLLECTION_PARENS: &str =
        "(not present in collection)";

    fn header_row() -> Vec<String> {
        vec![
            // First column is so that the header border aligns with the ZONE
            // TABLE section header.
            SLED_INDENT.to_string(),
            ZONE_TYPE.to_string(),
            ZONE_ID.to_string(),
            DISPOSITION.to_string(),
            UNDERLAY_IP.to_string(),
        ]
    }

    fn diff_header_row() -> Vec<String> {
        vec![
            // First column is so that the header border aligns with the ZONE
            // TABLE section header.
            SLED_HEAD_INDENT.to_string(),
            ZONE_TYPE.to_string(),
            ZONE_ID.to_string(),
            DISPOSITION.to_string(),
            UNDERLAY_IP.to_string(),
            STATUS.to_string(),
        ]
    }

    pub(super) fn metadata_heading() -> String {
        format!("{METADATA_HEADING}:")
    }

    pub(super) fn metadata_diff_heading() -> String {
        format!("{H1_INDENT}{METADATA_HEADING}:")
    }

    fn sleds_heading(prefix: char, heading: &'static str) -> String {
        format!("{prefix}{SLED_HEAD_INDENT}{heading}:")
    }

    fn removed_sleds_heading() -> String {
        sleds_heading(UNCHANGED_PREFIX, REMOVED_SLEDS_HEADING)
    }

    fn added_sleds_heading() -> String {
        sleds_heading(UNCHANGED_PREFIX, ADDED_SLEDS_HEADING)
    }

    fn modified_sleds_heading() -> String {
        sleds_heading(UNCHANGED_PREFIX, MODIFIED_SLEDS_HEADING)
    }

    fn unchanged_sleds_heading() -> String {
        sleds_heading(UNCHANGED_PREFIX, UNCHANGED_SLEDS_HEADING)
    }

    fn metadata_table_internal_dns() -> String {
        linear_table_label(&INTERNAL_DNS_VERSION)
    }

    fn metadata_table_external_dns() -> String {
        linear_table_label(&EXTERNAL_DNS_VERSION)
    }

    fn linear_table_label(value: &dyn fmt::Display) -> String {
        format!("{value}:")
    }

    fn linear_table_modified(
        before: &dyn fmt::Display,
        after: &dyn fmt::Display,
    ) -> String {
        format!("{before} {ARROW} {after}")
    }

    fn linear_table_unchanged(value: &dyn fmt::Display) -> String {
        format!("{value} {UNCHANGED_PARENS}")
    }
}
