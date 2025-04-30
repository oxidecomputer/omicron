// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests driver-based updates of the SP

// XXX-dap
// test cases:
//  - successful update: updated SP
//  - successful update: no changes needed
//  - successful update: watched another finish
//  - successful update: took over
//  - failure: when initial conditions don't match
//  - failure: failed to fetch artifact
//  - failure: MGS failure
//  - failure: reset in the middle
//  - failure: stuck?

use gateway_client::SpComponent;
use gateway_client::types::GetRotBootInfoParams;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentCaboose;
use gateway_client::types::SpType;
use gateway_messages::RotBootInfo;
use gateway_messages::SpPort;

mod step_through;

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

struct SpTestState {
    caboose_sp_active: SpComponentCaboose,
    caboose_sp_inactive: Result<SpComponentCaboose, GatewayClientError>,
    caboose_rot_a: Result<SpComponentCaboose, GatewayClientError>,
    caboose_rot_b: Result<SpComponentCaboose, GatewayClientError>,
    sp_boot_info: RotState,
}

impl SpTestState {
    async fn load(
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
            sp_boot_info,
        })
    }

    fn expect_caboose_sp_active(&self) -> &SpComponentCaboose {
        &self.caboose_sp_active
    }

    fn expect_caboose_sp_inactive(&self) -> &SpComponentCaboose {
        self.caboose_sp_inactive.as_ref().expect("inactive SP caboose")
    }

    fn expect_caboose_rot_a(&self) -> &SpComponentCaboose {
        self.caboose_rot_a.as_ref().expect("ROT slot A caboose")
    }

    fn expect_caboose_rot_b(&self) -> &SpComponentCaboose {
        self.caboose_rot_b.as_ref().expect("ROT slot B caboose")
    }
}

#[tokio::test]
async fn test_sp_update_basic() {
    let gwtestctx = gateway_test_utils::setup::test_setup(
        "test_sp_update_basic",
        SpPort::One,
    )
    .await;
    let log = &gwtestctx.logctx.log;

    // XXX-dap next goes here

    gwtestctx.teardown().await;
}
