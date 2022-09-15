// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Property based test for bootstore behavior

use assert_matches::assert_matches;
use bootstore::messages::{NodeOp, NodeOpResult, NodeRequest, NodeResponse};
use bootstore::{Config, Coordinator, Node};
use omicron_test_utils::dev::test_setup_log;
use proptest::prelude::*;
use sprockets_common::certificates::Ed25519Certificate;
use sprockets_common::certificates::KeyType;
use sprockets_common::{Ed25519PublicKey, Ed25519Signature};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// A generator to produce an arbitrary Ed25519 Public Key.
/// These aren't used for crypto, so a random bag of bytes is fine here.
fn arb_pub_key() -> impl Strategy<Value = Ed25519PublicKey> {
    proptest::array::uniform32(0u8..).prop_map(|v| Ed25519PublicKey(v))
}

/// A generator to produce an arbitrary Ed25519Signature
/// These aren't used for crypto, so a random bag of bytes is fine here.
fn arb_sig() -> impl Strategy<Value = Ed25519Signature> {
    proptest::array::uniform32(0u8..).prop_map(|v| {
        // Max array size we can generate is 32 bytes. Just use the same array for
        // the first and second half of the signature.
        let mut out = [0u8; 64];
        out[..32].copy_from_slice(&v[..]);
        out[32..].copy_from_slice(&v[..]);
        Ed25519Signature(out)
    })
}

/// A generator to produce an arbitrary Ed25519Certificate for DeviceIds
fn arb_device_id_cert() -> impl Strategy<Value = Ed25519Certificate> {
    (arb_pub_key(), arb_sig()).prop_map(|(key, sig)| Ed25519Certificate {
        subject_key_type: KeyType::DeviceId,
        subject_public_key: key,
        signer_key_type: KeyType::Manufacturing,
        signature: sig,
    })
}

// A generator to produce a vector of Ed25519 DeviceId certs use as trust quroum membership
// We require uniqueness so we generate a BTreeSet and map it to a Vec
fn arb_members() -> impl Strategy<Value = BTreeSet<Ed25519Certificate>> {
    proptest::collection::btree_set(arb_device_id_cert(), 3..8)
}

// There should be a `NodeOp::Initialize` request for each member  when a
// new Initializing Coordinator is created and `Coordinator::next_requests`
// is called.
fn prop_initialize_for_all_members(
    rack_uuid: &Uuid,
    members: &BTreeSet<Ed25519Certificate>,
    requests: &BTreeMap<Ed25519Certificate, NodeRequest>,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(requests.len(), members.len());
    let destinations: BTreeSet<Ed25519Certificate> =
        requests.keys().cloned().into_iter().collect();
    prop_assert_eq!(&destinations, members);
    for request in requests.values() {
        prop_assert_eq!(1, request.version);
        prop_assert_eq!(0, request.coordinator_id);
        assert_matches!(request.op, NodeOp::Initialize { rack_uuid: msg_rack_uuid, .. } => {
            prop_assert_eq!(*rack_uuid, msg_rack_uuid);
        });
    }
    Ok(())
}

// There should be a `NodeOp::KeyShareCommit` request for each member when the
// prepare phase completes.
fn prop_key_share_commit_for_all_members(
    coordinator_id: u64,
    epoch: i32,
    rack_uuid: &Uuid,
    members: &BTreeSet<Ed25519Certificate>,
    requests: &BTreeMap<Ed25519Certificate, NodeRequest>,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(requests.len(), members.len());
    let destinations: BTreeSet<Ed25519Certificate> =
        requests.keys().cloned().into_iter().collect();
    prop_assert_eq!(&destinations, members);
    for request in requests.values() {
        prop_assert_eq!(1, request.version);
        prop_assert_eq!(coordinator_id, request.coordinator_id);
        assert_matches!(request.op, NodeOp::KeyShareCommit{ rack_uuid: msg_rack_uuid, epoch: msg_epoch, .. } => {
            prop_assert_eq!(*rack_uuid, msg_rack_uuid);
            prop_assert_eq!(epoch, msg_epoch);
        });
    }
    Ok(())
}

// Initialize requests sent to all Nodes should have valid responses
fn prop_initialize_responses_are_all_valid(
    rack_uuid: &Uuid,
    members: &BTreeSet<Ed25519Certificate>,
    responses: &BTreeMap<Ed25519Certificate, NodeResponse>,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(responses.len(), members.len());
    let sources: BTreeSet<Ed25519Certificate> =
        responses.keys().cloned().into_iter().collect();
    prop_assert_eq!(&sources, members);
    for response in responses.values() {
        prop_assert_eq!(1, response.version);
        prop_assert_eq!(0, response.coordinator_id);
        assert_matches!(response.result, Ok(NodeOpResult::PrepareOk{rack_uuid: msg_rack_uuid, epoch: msg_epoch}) => {
                prop_assert_eq!(*rack_uuid, msg_rack_uuid);
                prop_assert_eq!(msg_epoch, 0);
        });
    }
    Ok(())
}

// KeyShareCommit requests sent to `members` should have valid responses
fn prop_key_share_commit_responses_are_all_valid(
    coordinator_id: u64,
    epoch: i32,
    rack_uuid: &Uuid,
    members: &BTreeSet<Ed25519Certificate>,
    responses: &BTreeMap<Ed25519Certificate, NodeResponse>,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(responses.len(), members.len());
    let sources: BTreeSet<Ed25519Certificate> =
        responses.keys().cloned().into_iter().collect();
    prop_assert_eq!(&sources, members);
    for response in responses.values() {
        prop_assert_eq!(1, response.version);
        prop_assert_eq!(coordinator_id, response.coordinator_id);
        assert_matches!(response.result, Ok(NodeOpResult::CommitOk{rack_uuid: msg_rack_uuid, epoch: msg_epoch}) => {
                prop_assert_eq!(*rack_uuid, msg_rack_uuid);
                prop_assert_eq!(msg_epoch, epoch);
        });
    }
    Ok(())
}

// Ensure that all nodes have KeyShares prepared for epoch 0
fn prop_initialize_prepared(
    rack_uuid: &Uuid,
    nodes: &mut BTreeMap<Ed25519Certificate, Node>,
) -> Result<(), TestCaseError> {
    let epoch = 0;
    for node in nodes.values_mut() {
        prop_assert!(node.has_key_share_prepare(rack_uuid, epoch)?);
    }
    Ok(())
}

proptest! {
    // Generate a trust quorum and intialize a set of nodes with a coordinator.
    // The members of the trust quorum are generated by proptest
    //
    // Don't drop any messages and assume everything works perfectly in this test
    #[test]
    fn successful_initialization(members in arb_members()) {
        let logctx = test_setup_log("proptest");
        let rack_uuid = Uuid::new_v4();
        // Create a coordinator for rack initialization
        let mut coordinator = Coordinator::new_initialize(
            logctx.log.clone(),
            rack_uuid.clone(),
            members.clone(),
        )?;

        // Create a `Node` per member
        let mut nodes: BTreeMap<Ed25519Certificate, Node> =
            members.iter().cloned().into_iter().map(|cert| {
                let config = Config {
                    log: logctx.log.clone(),
                    db_path: ":memory:".to_string(),
                };
                (cert, Node::new(config))
            }).collect();

        // There should be a `NodeOp::Initialize` request for each member
        let requests = coordinator.next_requests()?;
        prop_initialize_for_all_members(&rack_uuid, &members, &requests)?;

        // Pass an `Initialize` message to each node and gather the response
        let responses: BTreeMap<_, _> = requests.into_iter().map(|(cert, req)| {
            (cert, nodes.get_mut(&cert).unwrap().handle(req))
        }).collect();

        // Verify that all responses are correct
        prop_initialize_responses_are_all_valid(&rack_uuid, &members, &responses)?;

        // Verify the state of all Nodes to ensure they have the KeySharePrepare
        prop_initialize_prepared(&rack_uuid, &mut nodes)?;

        // Pass the responses back to the coordinator
        for (from, response) in responses {
            // `false` indicates that the transaction is not complete. This is
            // true because we haven't committed yet.
            prop_assert_eq!(false, coordinator.handle(from, response)?);
        }

        // Get the next set of requests from the coordinator which should
        // be a `KeyShareCommit` for each member
        let requests = coordinator.next_requests()?;
        let coordinator_id = 0;
        let epoch = 0;
        prop_key_share_commit_for_all_members(
            coordinator_id, epoch,  &rack_uuid, &members, &requests)?;

        // Handle the commits at the Nodes and get the responses
        let responses: BTreeMap<_, _> = requests.into_iter().map(|(cert, req)| {
            (cert, nodes.get_mut(&cert).unwrap().handle(req))
        }).collect();


        // Verify that the responses are correct
        prop_key_share_commit_responses_are_all_valid(
            coordinator_id, epoch, &rack_uuid, &members, &responses)?;

        // Verify that initialization is complete at all Nodes
        for node in nodes.values_mut() {
            prop_assert!(node.is_initialized(&rack_uuid)?);
        }

        // Handle the responses at the coordinator
        // The transaction is only complete after the last response
        for (i, (from, response)) in responses.into_iter().enumerate() {
            if i != members.len() -1 {
                prop_assert_eq!(false, coordinator.handle(from, response)?);
            } else {
                prop_assert_eq!(true, coordinator.handle(from, response)?);
            }
        }

        // Ensure the coordinator has no more requests to send, since the
        // transaction is complete.
        prop_assert!(coordinator.next_requests()?.is_empty());

        logctx.cleanup_successful();
    }
}
