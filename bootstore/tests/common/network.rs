// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Network model for property based tests

use bootstore::schemes::v0::{Envelope, Msg};
use sled_hardware::Baseboard;
use std::collections::{BTreeMap, BTreeSet};

// Named endpoints on a network
pub type Source = Baseboard;
pub type Destination = Baseboard;

/// A flow of messages from a source to a destination
pub type FlowId = (Source, Destination);

/// A simulation of a test network
///
/// Every time a peer handles a message it may send messages. We add these
/// messages to the network and distribute them during the test as a result
/// of actions. We allow dropping of messages and interleaving of messages in
/// different flows (source/dest pairs). However, since we are modeling a single
/// TCP connection for each flow we do not interleave messages within the same flow.
#[derive(Debug, Default)]
pub struct Network {
    // All connected flows.
    // There are two flows per connection, one in each direction.
    connected: BTreeSet<FlowId>,

    // Messages sent and "floating" in the network
    //
    // TODO: We probably want to add a "start time" to each message so that
    // we can model transmit delays as defined in `actions::Delays`. By adding
    // start time instead of delivery time, changes in delay rates will prevent
    // existing packets from getting delivered out of order.
    sent: BTreeMap<FlowId, Vec<Msg>>,

    // Messages that have been delivered on an endpoint and are ready to be
    // handled by the peer. They can no longer be dropped at this point and must
    // be delivered.
    delivered: BTreeMap<Destination, Vec<(Source, Msg)>>,
}

impl Network {
    pub fn connected(&mut self, peer1: Baseboard, peer2: Baseboard) {
        self.connected.insert((peer1.clone(), peer2.clone()));
        self.connected.insert((peer2, peer1));
    }

    pub fn disconnected(&mut self, peer1: Baseboard, peer2: Baseboard) {
        let flow1 = (peer1.clone(), peer2.clone());
        let flow2 = (peer2, peer1);
        self.connected.remove(&flow1);
        self.connected.remove(&flow2);

        // We drop all messages that are sent but not delivered for the given flow
        self.sent.remove(&flow1);
        self.sent.remove(&flow2);
    }

    pub fn send(&mut self, source: &Source, envelopes: Vec<Envelope>) {
        for envelope in envelopes {
            let flow_id = (source.clone(), envelope.to);
            // Only send if the peers are connected
            if self.connected.contains(&flow_id) {
                self.sent.entry(flow_id).or_default().push(envelope.msg);
            }
        }
    }

    // Round robin through each flow in `self.sent` delivering one message per
    // flow until all flows in `self.sent` are empty and all messages reside in
    // `self.delivered`
    pub fn deliver_all(&mut self) {
        while !self.sent.is_empty() {
            self.sent.retain(|(source, dest), msgs| {
                if let Some(msg) = msgs.pop() {
                    let value = (source.clone(), msg);
                    self.delivered.entry(dest.clone()).or_default().push(value);
                    true
                } else {
                    false
                }
            });
        }
    }

    // Provide access to messages that are ready to be handled by their
    // peer.
    pub fn delivered(
        &mut self,
    ) -> &mut BTreeMap<Destination, Vec<(Source, Msg)>> {
        &mut self.delivered
    }
}
