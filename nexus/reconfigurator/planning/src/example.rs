// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example blueprints

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::net::IpAddr;
use std::net::Ipv4Addr;

use crate::blueprint_builder::BlueprintBuilder;
use crate::blueprint_editor::ExternalNetworkingAllocator;
use crate::planner::rng::PlannerRng;
use crate::system::RotStateOverrides;
use crate::system::SledBuilder;
use crate::system::SystemDescription;
use anyhow::Context;
use anyhow::bail;
use camino::Utf8Path;
use camino_tempfile::Utf8TempDir;
use clap::Parser;
use nexus_inventory::CollectionBuilderRng;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;
use nexus_types::deployment::BlueprintSource;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::OmicronZoneNic;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::TargetReleaseDescription;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::inventory::Collection;
use omicron_common::address::Ipv4Range;
use omicron_common::api::external::TufRepoDescription;
use omicron_common::policy::CRUCIBLE_PANTRY_REDUNDANCY;
use omicron_common::policy::INTERNAL_DNS_REDUNDANCY;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::VnicUuid;
use tufaceous_artifact::ArtifactHash;
use tufaceous_artifact::ArtifactKind;
use tufaceous_artifact::KnownArtifactKind;
use typed_rng::TypedUuidRng;
use update_common::artifacts::ArtifactsWithPlan;
use update_common::artifacts::ControlPlaneZonesMode;
use update_common::artifacts::VerificationMode;

/// Stateful PRNG for generating simulated systems.
///
/// When generating a succession of simulated systems, this stateful PRNG allows
/// for reproducible generation of those systems after setting an initial seed.
/// The PRNGs are structured in tree form as much as possible, so that (for
/// example) if one part of the system decides to change how many sleds are in
/// the system, it can do so without affecting other UUIDs.
///
/// We have a number of existing tests that manually set seeds for individual
/// RNG instances. The old-style seeds have been kept around for backwards
/// compatibility. Newer tests should use this struct to generate their RNGs
/// instead, since it conveniently tracks generation numbers for each seed.
#[derive(Clone, Debug)]
pub struct SimRngState {
    seed: String,
    // Generation numbers for each RNG type.
    system_rng_gen: u64,
    collection_rng_gen: u64,
    planner_rng_gen: u64,
    // TODO: Currently, sled IDs are used to generate UUIDs to mutate the
    // system. This should be replaced in the future with a richer simulation
    // framework.
    sled_id_rng_gen: u64,
}

impl SimRngState {
    pub fn from_seed(seed: &str) -> Self {
        Self {
            seed: seed.to_string(),
            system_rng_gen: 0,
            collection_rng_gen: 0,
            planner_rng_gen: 0,
            sled_id_rng_gen: 0,
        }
    }

    pub fn seed(&self) -> &str {
        &self.seed
    }

    pub fn next_system_rng(&mut self) -> ExampleSystemRng {
        // Different behavior for the first system_rng_gen is a bit weird, but
        // it retains backwards compatibility with existing tests -- it means
        // that generated UUIDs particularly in fixtures don't change.
        self.system_rng_gen += 1;
        if self.system_rng_gen == 1 {
            ExampleSystemRng::from_seed(self.seed.as_str())
        } else {
            ExampleSystemRng::from_seed((
                self.seed.as_str(),
                self.system_rng_gen,
            ))
        }
    }

    pub fn next_collection_rng(&mut self) -> CollectionBuilderRng {
        self.collection_rng_gen += 1;
        // We don't need to pass in extra bits unique to collections, because
        // `CollectionBuilderRng` adds its own.
        let seed = (self.seed.as_str(), self.collection_rng_gen);
        CollectionBuilderRng::from_seed(seed)
    }

    pub fn next_planner_rng(&mut self) -> PlannerRng {
        self.planner_rng_gen += 1;
        // We don't need to pass in extra bits unique to the planner, because
        // `PlannerRng` adds its own.
        PlannerRng::from_seed((self.seed.as_str(), self.planner_rng_gen))
    }

    pub fn next_sled_id_rng(&mut self) -> TypedUuidRng<SledKind> {
        self.sled_id_rng_gen += 1;
        TypedUuidRng::from_seed(
            self.seed.as_str(),
            ("sled-id-rng", self.sled_id_rng_gen),
        )
    }
}

#[derive(Debug, Clone)]
pub struct ExampleSystemRng {
    seed: String,
    sled_rng: TypedUuidRng<SledKind>,
    collection_rng: CollectionBuilderRng,
    // ExampleSystem instances generate two blueprints: create RNGs for both.
    blueprint1_rng: PlannerRng,
    blueprint2_rng: PlannerRng,
}

impl ExampleSystemRng {
    pub fn from_seed<H: Hash + fmt::Debug>(seed: H) -> Self {
        // This is merely "ExampleSystem" for backwards compatibility with
        // existing test fixtures.
        let sled_rng = TypedUuidRng::from_seed(&seed, "ExampleSystem");
        // We choose to make our own collection and blueprint RNGs rather than
        // passing them in via `SimRngState`. This means that `SimRngState` is
        // influenced as little as possible by the specifics of how
        // `ExampleSystem` instances are generated, and RNG stability is
        // maintained.
        let collection_rng = CollectionBuilderRng::from_seed((
            &seed,
            "ExampleSystem collection",
        ));
        let blueprint1_rng =
            PlannerRng::from_seed((&seed, "ExampleSystem initial"));
        let blueprint2_rng =
            PlannerRng::from_seed((&seed, "ExampleSystem make_zones"));
        Self {
            seed: format!("{:?}", seed),
            sled_rng,
            collection_rng,
            blueprint1_rng,
            blueprint2_rng,
        }
    }
}

/// An example generated system, along with a consistent planning input and
/// collection.
///
/// The components of this struct are generated together and match each other.
/// The planning input and collection represent database input and inventory
/// that would be collected from a system matching the system description.
#[derive(Clone, Debug)]
pub struct ExampleSystem {
    pub system: SystemDescription,
    pub input: PlanningInput,
    pub collection: Collection,
    /// The initial blueprint that was used to describe the system.
    pub initial_blueprint: Blueprint,
}

/// Returns a collection, planning input, and blueprint describing a pretty
/// simple system.
///
/// The test name is used as the RNG seed.
pub fn example(
    log: &slog::Logger,
    test_name: &str,
) -> (Collection, PlanningInput, Blueprint) {
    let (example, blueprint) =
        ExampleSystemBuilder::new(log, test_name).build();
    (example.collection, example.input, blueprint)
}

/// A builder for the example system.
#[derive(Debug, Clone)]
pub struct ExampleSystemBuilder {
    log: slog::Logger,
    rng: ExampleSystemRng,
    sled_settings: Vec<BuilderSledSettings>,
    // TODO: Store a Policy struct instead of these fields:
    // https://github.com/oxidecomputer/omicron/issues/6803
    ndisks_per_sled: u8,
    // None means nsleds
    nexus_count: Option<ZoneCount>,
    internal_dns_count: ZoneCount,
    external_dns_count: ZoneCount,
    crucible_pantry_count: ZoneCount,
    create_zones: bool,
    create_disks_in_blueprint: bool,
    target_release: TargetReleaseDescription,
}

impl ExampleSystemBuilder {
    /// The default number of sleds in the example system.
    pub const DEFAULT_N_SLEDS: usize = 3;

    /// The default number of external DNS instances in the example system.
    ///
    /// The default value is picked for backwards compatibility -- we may wish
    /// to revisit it in the future.
    pub const DEFAULT_EXTERNAL_DNS_COUNT: usize = 0;

    pub fn new(log: &slog::Logger, test_name: &str) -> Self {
        let rng = ExampleSystemRng::from_seed(test_name);
        Self::new_with_rng(log, rng)
    }

    pub fn new_with_rng(log: &slog::Logger, rng: ExampleSystemRng) -> Self {
        Self {
            log: log.new(slog::o!(
                "component" => "ExampleSystem",
                "rng_seed" => rng.seed.clone(),
            )),
            rng,
            sled_settings: vec![
                BuilderSledSettings::default();
                Self::DEFAULT_N_SLEDS
            ],
            ndisks_per_sled: SledBuilder::DEFAULT_NPOOLS,
            nexus_count: None,
            internal_dns_count: ZoneCount(INTERNAL_DNS_REDUNDANCY),
            external_dns_count: ZoneCount(Self::DEFAULT_EXTERNAL_DNS_COUNT),
            crucible_pantry_count: ZoneCount(CRUCIBLE_PANTRY_REDUNDANCY),
            create_zones: true,
            create_disks_in_blueprint: true,
            target_release: TargetReleaseDescription::Initial,
        }
    }

    /// Set the number of sleds in the example system.
    ///
    /// Currently, this value can be anywhere between 0 and 5. (More can be
    /// added in the future if necessary.)
    pub fn nsleds(mut self, nsleds: usize) -> Self {
        self.sled_settings.resize(nsleds, BuilderSledSettings::default());
        self
    }

    /// Set the number of disks per sled in the example system.
    ///
    /// The default value is [`SledBuilder::DEFAULT_NPOOLS`]. A value of 0 is
    /// permitted.
    ///
    /// If [`Self::create_zones`] is set to `false`, this is ignored.
    pub fn ndisks_per_sled(mut self, ndisks_per_sled: u8) -> Self {
        self.ndisks_per_sled = ndisks_per_sled;
        self
    }

    /// Set the number of Nexus instances in the example system.
    ///
    /// The default value is the same as the number of sleds (i.e. one Nexus
    /// instance per sled). A value of 0 is permitted.
    ///
    /// If [`Self::create_zones`] is set to `false`, this is ignored.
    pub fn nexus_count(mut self, nexus_count: usize) -> Self {
        self.nexus_count = Some(ZoneCount(nexus_count));
        self
    }

    /// Set the number of internal DNS instances in the example system.
    ///
    /// The default value is [`INTERNAL_DNS_REDUNDANCY`]. A value anywhere
    /// between 0 and [`INTERNAL_DNS_REDUNDANCY`], inclusive, is permitted.
    ///
    /// If [`Self::create_zones`] is set to `false`, this is ignored.
    pub fn internal_dns_count(
        mut self,
        internal_dns_count: usize,
    ) -> anyhow::Result<Self> {
        if internal_dns_count > INTERNAL_DNS_REDUNDANCY {
            anyhow::bail!(
                "internal_dns_count {} is greater than INTERNAL_DNS_REDUNDANCY {}",
                internal_dns_count,
                INTERNAL_DNS_REDUNDANCY,
            );
        }
        self.internal_dns_count = ZoneCount(internal_dns_count);
        Ok(self)
    }

    /// Set the number of external DNS instances in the example system.
    ///
    /// The default value is [`Self::DEFAULT_EXTERNAL_DNS_COUNT`]. A value
    /// anywhere between 0 and 30, inclusive, is permitted. (The limit of 30 is
    /// primarily to simplify the implementation.)
    ///
    /// Each DNS server is assigned an address in the 198.51.100.x range.
    pub fn external_dns_count(
        mut self,
        external_dns_count: usize,
    ) -> anyhow::Result<Self> {
        if external_dns_count > 30 {
            anyhow::bail!(
                "external_dns_count {} is greater than 30",
                external_dns_count,
            );
        }
        self.external_dns_count = ZoneCount(external_dns_count);
        Ok(self)
    }

    /// Set the number of Crucible pantry instances in the example system.
    ///
    /// If [`Self::create_zones`] is set to `false`, this is ignored.
    pub fn crucible_pantry_count(
        mut self,
        crucible_pantry_count: usize,
    ) -> Self {
        self.crucible_pantry_count = ZoneCount(crucible_pantry_count);
        self
    }

    /// Create zones in the example system.
    ///
    /// The default is `true`.
    pub fn create_zones(mut self, create_zones: bool) -> Self {
        self.create_zones = create_zones;
        self
    }

    /// Create disks in the blueprint.
    ///
    /// The default is `true`.
    ///
    /// If [`Self::ndisks_per_sled`] is set to 0, then this is implied: if no
    /// disks are created, then the blueprint won't have any disks.
    pub fn create_disks_in_blueprint(mut self, create: bool) -> Self {
        self.create_disks_in_blueprint = create;
        self
    }

    /// Set the policy for a sled in the example system by index.
    ///
    /// Returns an error if `index >= nsleds`.
    pub fn with_sled_policy(
        mut self,
        index: usize,
        policy: SledPolicy,
    ) -> anyhow::Result<Self> {
        let Some(settings) = self.sled_settings.get_mut(index) else {
            bail!(
                "sled index {} out of range (0..{})",
                index,
                self.sled_settings.len(),
            );
        };
        settings.policy = policy;
        Ok(self)
    }

    /// Set the target release to an initial `0.0.1` version, and image sources to
    /// Artifact corresponding to the release.
    pub fn with_target_release_0_0_1(self) -> anyhow::Result<Self> {
        // Find the 0.0.1 release relative to this crate's root directory.
        let root_dir = Utf8Path::new(env!("CARGO_MANIFEST_DIR"));
        let manifest_path = root_dir
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("update-common/manifests/fake-0.0.1.toml");
        self.with_target_release_manifest(
            &manifest_path,
            // allow_non_semver is false because fake-0.0.1.toml doesn't contain
            // non-semver artifacts.
            false,
        )
    }

    pub fn with_target_release_manifest(
        mut self,
        manifest_path: &Utf8Path,
        allow_non_semver: bool,
    ) -> anyhow::Result<Self> {
        let dir = Utf8TempDir::with_prefix("reconfigurator-planning-example")
            .context("failed to create temp dir")?;
        let zip_path = dir.path().join("repo.zip");
        tuf_assemble(&self.log, manifest_path, &zip_path, allow_non_semver)
            .context("failed to assemble TUF repo")?;

        let target_release = extract_tuf_repo_description(&self.log, &zip_path)
            .context("failed to extract TUF repo description")?;

        self.target_release = TargetReleaseDescription::TufRepo(target_release);

        Ok(self)
    }

    fn get_nexus_zones(&self) -> ZoneCount {
        self.nexus_count.unwrap_or(ZoneCount(self.sled_settings.len()))
    }

    pub fn get_internal_dns_zones(&self) -> usize {
        self.internal_dns_count.0
    }

    pub fn get_external_dns_zones(&self) -> usize {
        self.external_dns_count.0
    }

    /// Create a new example system with the given modifications.
    ///
    /// Return the system, and the initial blueprint that matches it.
    pub fn build(&self) -> (ExampleSystem, Blueprint) {
        let nexus_count = self.get_nexus_zones();

        slog::debug!(
            &self.log,
            "Creating example system";
            "nsleds" => self.sled_settings.len(),
            "ndisks_per_sled" => self.ndisks_per_sled,
            "nexus_count" => nexus_count.0,
            "internal_dns_count" => self.internal_dns_count.0,
            "external_dns_count" => self.external_dns_count.0,
            "crucible_pantry_count" => self.crucible_pantry_count.0,
            "create_zones" => self.create_zones,
            "create_disks_in_blueprint" => self.create_disks_in_blueprint,
        );

        let mut rng = self.rng.clone();

        let mut system = SystemDescription::new();
        // Update the system's target counts with the counts. (Note that
        // there's no external DNS count.)
        system
            .set_target_nexus_zone_count(nexus_count.0)
            .set_target_internal_dns_zone_count(self.internal_dns_count.0)
            .set_target_crucible_pantry_zone_count(
                self.crucible_pantry_count.0,
            );

        // Set the target release if one is available. We don't do this
        // unconditionally because we don't want the target release generation
        // number to be incremented if self.target_release is
        // TargetReleaseDescription::Initial.
        if let TargetReleaseDescription::TufRepo(_) = &self.target_release {
            system.set_target_release(self.target_release.clone());
        }

        let sled_ids_with_settings: Vec<_> = self
            .sled_settings
            .iter()
            .map(|settings| (rng.sled_rng.next(), settings))
            .collect();

        let artifacts_by_kind = if let TargetReleaseDescription::TufRepo(repo) =
            &self.target_release
        {
            // Build a map of artifact versions by kind. For the artifacts we
            // care about, we currently only expect a single version, so this
            // map is expected to be unique.
            //
            // TODO-correctness Need to choose gimlet vs cosmo here! Can we use
            // update-common's UpdatePlan instead of this jank?
            // https://github.com/oxidecomputer/omicron/issues/8777
            let mut artifacts_by_kind = HashMap::new();
            for artifact in &repo.artifacts {
                artifacts_by_kind.insert(&artifact.id.kind, artifact);
            }
            Some(artifacts_by_kind)
        } else {
            None
        };

        for (sled_id, settings) in &sled_ids_with_settings {
            let _ = system
                .sled(
                    SledBuilder::new()
                        .id(*sled_id)
                        .npools(self.ndisks_per_sled)
                        .policy(settings.policy),
                )
                .unwrap();
        }

        // Add as many external IPs as is necessary for external DNS zones. We
        // pick addresses in the TEST-NET-2 (RFC 5737) range.
        if self.external_dns_count.0 > 0 {
            let mut builder =
                system.external_ip_policy().clone().into_builder();
            builder
                .push_service_pool_ipv4_range(
                    Ipv4Range::new(
                        "198.51.100.1".parse::<Ipv4Addr>().unwrap(),
                        "198.51.100.30".parse::<Ipv4Addr>().unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap();
            for i in 0..self.external_dns_count.0 {
                let lo = (i + 1)
                    .try_into()
                    .expect("external_dns_count is always <= 30");
                builder
                    .add_external_dns_ip(IpAddr::V4(Ipv4Addr::new(
                        198, 51, 100, lo,
                    )))
                    .expect("test IPs are valid service IPs");
            }
            system.set_external_ip_policy(builder.build());
        }

        let mut input_builder = system
            .to_planning_input_builder()
            .expect("failed to make planning input builder");

        let base_input = input_builder.clone().build();

        // Start with an empty blueprint containing only our sleds, no zones.
        let initial_blueprint = BlueprintBuilder::build_empty_with_sleds_seeded(
            base_input.all_sled_ids(SledFilter::Commissioned),
            "test suite",
            rng.blueprint1_rng,
        );

        // Now make a blueprint and collection with some zones on each sled.
        let mut builder = BlueprintBuilder::new_based_on(
            &self.log,
            &initial_blueprint,
            &base_input,
            "test suite",
            rng.blueprint2_rng,
        )
        .unwrap();

        let discretionary_sled_count =
            base_input.all_sled_ids(SledFilter::Discretionary).count();
        let mut external_networking_alloc =
            ExternalNetworkingAllocator::from_current_zones(
                &builder,
                base_input.external_ip_policy(),
            )
            .expect("constructed ExternalNetworkingAllocator");

        // * Create disks and non-discretionary zones on all sleds.
        // * Only create discretionary zones on discretionary sleds.
        let mut discretionary_ix = 0;
        for (sled_id, sled_details) in
            base_input.all_sleds(SledFilter::Commissioned)
        {
            if self.create_disks_in_blueprint {
                let _ = builder
                    .sled_add_disks(sled_id, &sled_details.resources)
                    .unwrap();
            }
            if self.create_zones {
                let _ = builder
                    .sled_ensure_zone_ntp(
                        sled_id,
                        self.target_release
                            .zone_image_source(ZoneKind::BoundaryNtp)
                            .expect("obtained BoundaryNtp image source"),
                    )
                    .unwrap();

                // Create discretionary zones if allowed.
                if sled_details.policy.matches(SledFilter::Discretionary) {
                    for _ in 0..nexus_count
                        .on(discretionary_ix, discretionary_sled_count)
                    {
                        let external_ip = external_networking_alloc
                            .for_new_nexus()
                            .expect("should have an external IP for Nexus");
                        builder
                            .sled_add_zone_nexus_with_config(
                                sled_id,
                                false,
                                vec![],
                                self.target_release
                                    .zone_image_source(ZoneKind::Nexus)
                                    .expect("obtained Nexus image source"),
                                external_ip,
                                initial_blueprint.nexus_generation,
                            )
                            .unwrap();
                    }
                    if discretionary_ix == 0 {
                        builder
                            .sled_add_zone_clickhouse(
                                sled_id,
                                self.target_release
                                    .zone_image_source(ZoneKind::Clickhouse)
                                    .expect("obtained Clickhouse image source"),
                            )
                            .unwrap();
                    }
                    let mut internal_dns_subnets =
                        builder.available_internal_dns_subnets().unwrap();
                    for _ in 0..self
                        .internal_dns_count
                        .on(discretionary_ix, discretionary_sled_count)
                    {
                        builder
                            .sled_add_zone_internal_dns(
                                sled_id,
                                self.target_release
                                    .zone_image_source(ZoneKind::InternalDns)
                                    .expect(
                                        "obtained InternalDNS image source",
                                    ),
                                internal_dns_subnets.next().expect(
                                    "sufficient available internal DNS subnets",
                                ),
                            )
                            .unwrap();
                    }
                    for _ in 0..self
                        .external_dns_count
                        .on(discretionary_ix, discretionary_sled_count)
                    {
                        let external_ip = external_networking_alloc
                            .for_new_external_dns()
                            .expect(
                                "should have an external IP for external DNS",
                            );
                        builder
                            .sled_add_zone_external_dns(
                                sled_id,
                                self.target_release
                                    .zone_image_source(ZoneKind::ExternalDns)
                                    .expect(
                                        "obtained ExternalDNS image source",
                                    ),
                                external_ip,
                            )
                            .unwrap();
                    }
                    for _ in 0..self
                        .crucible_pantry_count
                        .on(discretionary_ix, discretionary_sled_count)
                    {
                        builder
                            .sled_add_zone_crucible_pantry(
                                sled_id,
                                self.target_release
                                    .zone_image_source(ZoneKind::CruciblePantry)
                                    .expect(
                                        "obtained CruciblePantry image source",
                                    ),
                            )
                            .unwrap();
                    }
                    discretionary_ix += 1;
                }

                for pool_name in sled_details.resources.zpools.keys() {
                    let _ = builder
                        .sled_ensure_zone_crucible(
                            sled_id,
                            *pool_name,
                            self.target_release
                                .zone_image_source(ZoneKind::Crucible)
                                .expect("obtained Crucible image source"),
                        )
                        .unwrap();
                }
            }
            builder.sled_ensure_zone_datasets(sled_id).unwrap();

            if let Some(artifacts_by_kind) = &artifacts_by_kind {
                // Set the host phase 2 artifact version to Artifact to avoid a
                // noop conversion in the first planning run.
                let host_phase_2_artifact =
                    artifacts_by_kind.get(&ArtifactKind::HOST_PHASE_2).unwrap();

                builder
                    .sled_set_host_phase_2(
                        sled_id,
                        BlueprintHostPhase2DesiredSlots {
                            slot_a:
                                BlueprintHostPhase2DesiredContents::Artifact {
                                    version:
                                        BlueprintArtifactVersion::Available {
                                            version: host_phase_2_artifact
                                                .id
                                                .version
                                                .clone(),
                                        },
                                    hash: host_phase_2_artifact.hash,
                                },
                            slot_b:
                                BlueprintHostPhase2DesiredContents::Artifact {
                                    version:
                                        BlueprintArtifactVersion::Available {
                                            version: host_phase_2_artifact
                                                .id
                                                .version
                                                .clone(),
                                        },
                                    hash: host_phase_2_artifact.hash,
                                },
                        },
                    )
                    .expect("sled is present in blueprint");
            };
        }

        let blueprint = builder.build(BlueprintSource::Test);

        // Find and set the set of active Nexuses
        let active_nexus_zone_ids: BTreeSet<_> = blueprint
            .all_nexus_zones(BlueprintZoneDisposition::is_in_service)
            .filter_map(|(_, zone, nexus_zone)| {
                if nexus_zone.nexus_generation == blueprint.nexus_generation {
                    Some(zone.id)
                } else {
                    None
                }
            })
            .collect();
        input_builder.set_active_nexus_zones(active_nexus_zone_ids.clone());
        system.set_active_nexus_zones(active_nexus_zone_ids);

        for sled_cfg in blueprint.sleds.values() {
            for zone in sled_cfg.zones.iter() {
                let service_id = zone.id;
                if let Some((external_ip, nic)) =
                    zone.zone_type.external_networking()
                {
                    input_builder
                        .add_omicron_zone_external_ip(service_id, external_ip)
                        .expect("failed to add Omicron zone external IP");
                    input_builder
                        .add_omicron_zone_nic(
                            service_id,
                            OmicronZoneNic {
                                // TODO-cleanup use `TypedUuid` everywhere
                                id: VnicUuid::from_untyped_uuid(nic.id),
                                mac: nic.mac,
                                ip: nic.ip,
                                slot: nic.slot,
                                primary: nic.primary,
                            },
                        )
                        .expect("failed to add Omicron zone NIC");
                }
            }
        }

        for (sled_id, sled_cfg) in &blueprint.sleds {
            let sled_cfg = sled_cfg.clone().into_in_service_sled_config();
            system.sled_set_omicron_config(*sled_id, sled_cfg).unwrap();
        }

        // We just ensured that a handful of datasets should exist in
        // the blueprint, but they don't yet exist in the SystemDescription.
        //
        // Go back and add them so that the blueprint is consistent with
        // inventory.
        for (sled_id, sled_cfg) in &blueprint.sleds {
            let sled = system.get_sled_mut(*sled_id).unwrap();

            for dataset_config in sled_cfg.datasets.iter() {
                let config = dataset_config.clone().try_into().unwrap();
                sled.add_synthetic_dataset(config);
            }
        }

        if let Some(artifacts_by_kind) = &artifacts_by_kind {
            // Set all MGS and host phase 2 versions out of the TUF repo to
            // ensure that the planner is quiesced at the time the initial
            // system is returned.
            let sp_version = artifacts_by_kind
                .get(&ArtifactKind::from(KnownArtifactKind::GimletSp))
                .unwrap()
                .id
                .version
                .clone();
            let rot_a_version = artifacts_by_kind
                .get(&ArtifactKind::GIMLET_ROT_IMAGE_A)
                .unwrap()
                .id
                .version
                .clone();
            let rot_b_version = artifacts_by_kind
                .get(&ArtifactKind::GIMLET_ROT_IMAGE_B)
                .unwrap()
                .id
                .version
                .clone();
            let host_phase_1_hash = artifacts_by_kind
                .get(&ArtifactKind::GIMLET_HOST_PHASE_1)
                .unwrap()
                .hash;
            let host_phase_2_hash = artifacts_by_kind
                .get(&ArtifactKind::HOST_PHASE_2)
                .unwrap()
                .hash;

            for sled_id in blueprint.sleds.keys() {
                system
                    .sled_update_sp_versions(
                        *sled_id,
                        Some(sp_version.clone()),
                        Some(ExpectedVersion::Version(sp_version.clone())),
                    )
                    .expect("sled was just added to the system");
                system
                    .sled_update_rot_versions(
                        *sled_id,
                        RotStateOverrides {
                            active_slot_override: None,
                            slot_a_version_override: Some(
                                ExpectedVersion::Version(rot_a_version.clone()),
                            ),
                            slot_b_version_override: Some(
                                ExpectedVersion::Version(rot_b_version.clone()),
                            ),
                            persistent_boot_preference_override: None,
                            pending_persistent_boot_preference_override: None,
                            transient_boot_preference_override: None,
                        },
                    )
                    .expect("sled was just added to the system");

                // TODO-correctness Need to choose gimlet vs cosmo here! Need help
                // from tufaceous to tell us which is which.
                // https://github.com/oxidecomputer/omicron/issues/8777
                system
                    .sled_update_host_phase_1_artifacts(
                        *sled_id,
                        None,
                        Some(host_phase_1_hash),
                        Some(host_phase_1_hash),
                    )
                    .expect("sled was just added to the system");

                // We must set host phase 2 artifacts after sled_set_omicron_config
                // is called, because sled_set_omicron_config resets
                // last_reconciliation state and wipes away host phase 2 artifact
                // hashes.
                system
                    .sled_update_host_phase_2_artifacts(
                        *sled_id,
                        None,
                        Some(host_phase_2_hash),
                        Some(host_phase_2_hash),
                    )
                    .expect("sled was just added to the system");
            }
        }

        let mut builder =
            system.to_collection_builder().expect("failed to build collection");
        builder.set_rng(rng.collection_rng);

        // The blueprint evolves separately from the system -- so it's returned
        // as a separate value.
        let example = ExampleSystem {
            system,
            input: input_builder.build(),
            collection: builder.build(),
            initial_blueprint,
        };
        (example, blueprint)
    }
}

/// Per-sled state.
#[derive(Clone, Debug)]
struct BuilderSledSettings {
    policy: SledPolicy,
}

impl Default for BuilderSledSettings {
    fn default() -> Self {
        Self { policy: SledPolicy::provisionable() }
    }
}

// A little wrapper to try and avoid having an `on` function which takes 3
// usize parameters.
#[derive(Clone, Copy, Debug)]
struct ZoneCount(pub usize);

impl ZoneCount {
    fn on(self, sled_id: usize, total_sleds: usize) -> usize {
        // Spread instances out as evenly as possible. If there are 5 sleds and 3
        // instances, we want to spread them out as 2, 2, 1.
        let div = self.0 / total_sleds;
        let rem = self.0 % total_sleds;
        div + if sled_id < rem { 1 } else { 0 }
    }
}

/// The default key for TUF repository generation.
///
/// This was randomly generated through a tufaceous invocation.
pub static DEFAULT_TUFACEOUS_KEY: &str = "ed25519:\
MFECAQEwBQYDK2VwBCIEIJ9CnAhwk8PPt1x8icu\
z9c12PdfCRHJpoUkuqJmIZ8GbgSEAbNGMpsHK5_w32\
qwYdZH_BeVssmKzQlFsnPuaiHx2hy0=";

/// Construct a TUF repository zip file from a manifest, writing the TUF zip
/// file out to `output_path`.
pub fn tuf_assemble(
    log: &slog::Logger,
    manifest_path: &Utf8Path,
    output_path: &Utf8Path,
    allow_non_semver: bool,
) -> anyhow::Result<()> {
    if output_path.exists() {
        bail!("output path `{output_path}` already exists");
    }

    // Just use a fixed key for now.
    //
    // In the future we may want to test changing the TUF key.
    let mut tufaceous_args = vec![
        "tufaceous",
        "--key",
        DEFAULT_TUFACEOUS_KEY,
        "assemble",
        manifest_path.as_str(),
        output_path.as_str(),
    ];

    if allow_non_semver {
        tufaceous_args.push("--allow-non-semver");
    }
    let args = tufaceous::Args::try_parse_from(tufaceous_args)
        .expect("args are valid so this shouldn't fail");
    let rt =
        tokio::runtime::Runtime::new().context("creating tokio runtime")?;
    rt.block_on(async move { args.exec(log).await })
        .context("error executing tufaceous assemble")?;

    Ok(())
}

pub fn extract_tuf_repo_description(
    log: &slog::Logger,
    zip_path: &Utf8Path,
) -> anyhow::Result<TufRepoDescription> {
    let file = std::fs::File::open(zip_path)
        .with_context(|| format!("open {:?}", zip_path))?;
    let buf = std::io::BufReader::new(file);
    let rt =
        tokio::runtime::Runtime::new().context("creating tokio runtime")?;
    let repo_hash = ArtifactHash([0; 32]);
    let artifacts_with_plan = rt.block_on(async {
        ArtifactsWithPlan::from_zip(
            buf,
            None,
            repo_hash,
            ControlPlaneZonesMode::Split,
            VerificationMode::BlindlyTrustAnything,
            log,
        )
        .await
        .with_context(|| format!("unpacking {:?}", zip_path))
    })?;
    let description = artifacts_with_plan.description().clone();
    Ok(description)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use nexus_sled_agent_shared::inventory::{OmicronZoneConfig, ZoneKind};
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use omicron_test_utils::dev::test_setup_log;

    use super::*;

    #[test]
    fn instances_on_examples() {
        assert_eq!(ZoneCount(3).on(0, 5), 1);
        assert_eq!(ZoneCount(3).on(1, 5), 1);
        assert_eq!(ZoneCount(3).on(2, 5), 1);
        assert_eq!(ZoneCount(3).on(3, 5), 0);
        assert_eq!(ZoneCount(3).on(4, 5), 0);

        assert_eq!(ZoneCount(5).on(0, 5), 1);
        assert_eq!(ZoneCount(5).on(1, 5), 1);
        assert_eq!(ZoneCount(5).on(2, 5), 1);
        assert_eq!(ZoneCount(5).on(3, 5), 1);
        assert_eq!(ZoneCount(5).on(4, 5), 1);

        assert_eq!(ZoneCount(7).on(0, 5), 2);
        assert_eq!(ZoneCount(7).on(1, 5), 2);
        assert_eq!(ZoneCount(7).on(2, 5), 1);
        assert_eq!(ZoneCount(6).on(3, 5), 1);
        assert_eq!(ZoneCount(6).on(4, 5), 1);
    }

    #[test]
    fn builder_zone_counts() {
        static TEST_NAME: &str = "example_builder_zone_counts";
        let logctx = test_setup_log(TEST_NAME);

        let (example, mut blueprint) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME)
                .nsleds(5)
                .nexus_count(6)
                .crucible_pantry_count(5)
                .internal_dns_count(2)
                .unwrap()
                .external_dns_count(10)
                .unwrap()
                .build();

        // Define a time_created for consistent output across runs.
        blueprint.time_created = DateTime::<Utc>::UNIX_EPOCH;

        expectorate::assert_contents(
            "tests/output/example_builder_zone_counts_blueprint.txt",
            &blueprint.display().to_string(),
        );

        // Check that the system's target counts are set correctly.
        assert_eq!(example.system.target_nexus_zone_count(), 6);
        assert_eq!(example.system.target_internal_dns_zone_count(), 2);
        assert_eq!(example.system.target_crucible_pantry_zone_count(), 5);

        // Check that the right number of zones are present in both the
        // blueprint and in the collection.
        let nexus_zones = blueprint_zones_of_kind(&blueprint, ZoneKind::Nexus);
        assert_eq!(
            nexus_zones.len(),
            6,
            "expected 6 Nexus zones in blueprint, got {}: {:#?}",
            nexus_zones.len(),
            nexus_zones,
        );
        let nexus_zones = collection_ledgered_zones_of_kind(
            &example.collection,
            ZoneKind::Nexus,
        );
        assert_eq!(
            nexus_zones.len(),
            6,
            "expected 6 Nexus zones in collection, got {}: {:#?}",
            nexus_zones.len(),
            nexus_zones,
        );

        let internal_dns_zones =
            blueprint_zones_of_kind(&blueprint, ZoneKind::InternalDns);
        assert_eq!(
            internal_dns_zones.len(),
            2,
            "expected 2 internal DNS zones in blueprint, got {}: {:#?}",
            internal_dns_zones.len(),
            internal_dns_zones,
        );
        let internal_dns_zones = collection_ledgered_zones_of_kind(
            &example.collection,
            ZoneKind::InternalDns,
        );
        assert_eq!(
            internal_dns_zones.len(),
            2,
            "expected 2 internal DNS zones in collection, got {}: {:#?}",
            internal_dns_zones.len(),
            internal_dns_zones,
        );

        let external_dns_zones =
            blueprint_zones_of_kind(&blueprint, ZoneKind::ExternalDns);
        assert_eq!(
            external_dns_zones.len(),
            10,
            "expected 10 external DNS zones in blueprint, got {}: {:#?}",
            external_dns_zones.len(),
            external_dns_zones,
        );
        let external_dns_zones = collection_ledgered_zones_of_kind(
            &example.collection,
            ZoneKind::ExternalDns,
        );
        assert_eq!(
            external_dns_zones.len(),
            10,
            "expected 10 external DNS zones in collection, got {}: {:#?}",
            external_dns_zones.len(),
            external_dns_zones,
        );

        let crucible_pantry_zones =
            blueprint_zones_of_kind(&blueprint, ZoneKind::CruciblePantry);
        assert_eq!(
            crucible_pantry_zones.len(),
            5,
            "expected 5 Crucible pantry zones in blueprint, got {}: {:#?}",
            crucible_pantry_zones.len(),
            crucible_pantry_zones,
        );
        let crucible_pantry_zones = collection_ledgered_zones_of_kind(
            &example.collection,
            ZoneKind::CruciblePantry,
        );
        assert_eq!(
            crucible_pantry_zones.len(),
            5,
            "expected 5 Crucible pantry zones in collection, got {}: {:#?}",
            crucible_pantry_zones.len(),
            crucible_pantry_zones,
        );

        logctx.cleanup_successful();
    }

    fn blueprint_zones_of_kind(
        blueprint: &Blueprint,
        kind: ZoneKind,
    ) -> Vec<&BlueprintZoneConfig> {
        blueprint
            .all_omicron_zones(BlueprintZoneDisposition::any)
            .filter_map(|(_, zone)| {
                (zone.zone_type.kind() == kind).then_some(zone)
            })
            .collect()
    }

    fn collection_ledgered_zones_of_kind(
        collection: &Collection,
        kind: ZoneKind,
    ) -> Vec<&OmicronZoneConfig> {
        collection
            .all_ledgered_omicron_zones()
            .filter(|zone| zone.zone_type.kind() == kind)
            .collect()
    }
}
