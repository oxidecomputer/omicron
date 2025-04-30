// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_client::SpComponent;
use gateway_client::types::GetRotBootInfoParams;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpState;
use gateway_client::types::SpType;
use gateway_messages::RotBootInfo;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::inventory::BaseboardId;
use tufaceous_artifact::ArtifactVersion;

pub type GatewayClientError =
    gateway_client::Error<gateway_client::types::Error>;

pub struct SpTestState {
    pub caboose_sp_active: SpComponentCaboose,
    pub caboose_sp_inactive: Result<SpComponentCaboose, GatewayClientError>,
    pub caboose_rot_a: Result<SpComponentCaboose, GatewayClientError>,
    pub caboose_rot_b: Result<SpComponentCaboose, GatewayClientError>,
    pub sp_state: SpState,
    pub sp_boot_info: RotState,
}

impl SpTestState {
    pub async fn load(
        mgs_client: &gateway_client::Client,
        sp_type: SpType,
        sp_slot: u32,
    ) -> Result<SpTestState, GatewayClientError> {
        let caboose_sp_active = mgs_client
            .sp_component_caboose_get(
                sp_type,
                sp_slot,
                SpComponent::SP_ITSELF.const_as_str(),
                0,
            )
            .await?
            .into_inner();
        let caboose_sp_inactive = mgs_client
            .sp_component_caboose_get(
                sp_type,
                sp_slot,
                SpComponent::SP_ITSELF.const_as_str(),
                1,
            )
            .await
            .map(|c| c.into_inner());
        let caboose_rot_a = mgs_client
            .sp_component_caboose_get(
                sp_type,
                sp_slot,
                SpComponent::ROT.const_as_str(),
                0,
            )
            .await
            .map(|c| c.into_inner());
        let caboose_rot_b = mgs_client
            .sp_component_caboose_get(
                sp_type,
                sp_slot,
                SpComponent::ROT.const_as_str(),
                1,
            )
            .await
            .map(|c| c.into_inner());
        let sp_info = mgs_client.sp_get(sp_type, sp_slot).await?.into_inner();
        let sp_boot_info = mgs_client
            .sp_rot_boot_info(
                sp_type,
                sp_slot,
                SpComponent::ROT.const_as_str(),
                &GetRotBootInfoParams {
                    version: RotBootInfo::HIGHEST_KNOWN_VERSION,
                },
            )
            .await?
            .into_inner();
        Ok(SpTestState {
            caboose_sp_active,
            caboose_sp_inactive,
            caboose_rot_a,
            caboose_rot_b,
            sp_state: sp_info,
            sp_boot_info,
        })
    }

    pub fn expect_caboose_sp_active(&self) -> &SpComponentCaboose {
        &self.caboose_sp_active
    }

    pub fn expect_caboose_sp_inactive(&self) -> &SpComponentCaboose {
        self.caboose_sp_inactive.as_ref().expect("inactive SP caboose")
    }

    pub fn expect_caboose_rot_a(&self) -> &SpComponentCaboose {
        self.caboose_rot_a.as_ref().expect("ROT slot A caboose")
    }

    pub fn expect_caboose_rot_b(&self) -> &SpComponentCaboose {
        self.caboose_rot_b.as_ref().expect("ROT slot B caboose")
    }

    pub fn baseboard_id(&self) -> BaseboardId {
        BaseboardId {
            part_number: self.sp_state.model.clone(),
            serial_number: self.sp_state.serial_number.clone(),
        }
    }

    pub fn expect_sp_active_version(&self) -> ArtifactVersion {
        self.expect_caboose_sp_active()
            .version
            .parse()
            .expect("valid artifact version")
    }

    pub fn expect_sp_inactive_version(&self) -> ExpectedVersion {
        match &self.caboose_sp_inactive {
            Ok(v) => ExpectedVersion::Version(
                v.version.parse().expect("valid SP inactive slot version"),
            ),
            // XXX-dap filter on error message
            Err(_) => ExpectedVersion::NoValidVersion,
        }
    }
}
