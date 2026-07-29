// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests BGP unnumbered status APIs.

use nexus_test_utils::http_testing::{AuthnMode, NexusRequest};
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::networking::{
    SwitchResult, SwitchResults, SwitchUnavailableReason, UnnumberedInterfaces,
    UnnumberedManagerState,
};

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_bgp_unnumbered_status_by_switch(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let manager = NexusRequest::object_get(
        client,
        "/v1/system/networking/bgp-unnumbered-manager",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SwitchResults<UnnumberedManagerState>>()
    .await;
    match manager.switch0 {
        SwitchResult::Available { value } => {
            assert!(value.interfaces.is_empty());
        }
        SwitchResult::Unavailable {
            reason: SwitchUnavailableReason::MgdUnresolved,
        } => panic!("switch0 MGD was unexpectedly unresolved"),
        SwitchResult::Unavailable {
            reason: SwitchUnavailableReason::QueryFailed,
        } => panic!("switch0 manager-state query unexpectedly failed"),
    }
    assert_mgd_unresolved(manager.switch1);

    let interfaces = NexusRequest::object_get(
        client,
        "/v1/system/networking/bgp-unnumbered-interfaces",
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute_and_parse_unwrap::<SwitchResults<UnnumberedInterfaces>>()
    .await;
    match interfaces.switch0 {
        SwitchResult::Available { value: UnnumberedInterfaces(interfaces) } => {
            assert!(interfaces.is_empty());
        }
        SwitchResult::Unavailable {
            reason: SwitchUnavailableReason::MgdUnresolved,
        } => panic!("switch0 MGD was unexpectedly unresolved"),
        SwitchResult::Unavailable {
            reason: SwitchUnavailableReason::QueryFailed,
        } => panic!("switch0 interface query unexpectedly failed"),
    }
    assert_mgd_unresolved(interfaces.switch1);
}

fn assert_mgd_unresolved<T>(result: SwitchResult<T>) {
    match result {
        SwitchResult::Unavailable {
            reason: SwitchUnavailableReason::MgdUnresolved,
        } => {}
        SwitchResult::Available { .. } => {
            panic!("switch1 was unexpectedly available");
        }
        SwitchResult::Unavailable {
            reason: SwitchUnavailableReason::QueryFailed,
        } => panic!("switch1 query unexpectedly failed"),
    }
}
