// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Network model for property based tests

use bootstore::schemes::v0::{Envelope, Msg};
use sled_hardware::Baseboard;
use std::collections::BTreeMap;

// Named endpoints on a network
type Source = Baseboard;
type Destination = Baseboard;

/// A flow of messages from a source to a destination
type FlowId = (Source, Destination);

/// A simulation of a test network
///
/// Every time a peer handles a message it may send messages. We add these
/// messages to the network and distribute them during the test as a result
/// of actions. We allow dropping of messages and interleaving of messages in
/// different flows (source/dest pairs in one direction). However, since we are modeling a single
/// TCP connection for each flow we do not interleave messages within the same flow.
#[derive(Debug, Default)]
pub struct Network {
    // Messages sent and "floating" in the network
    sent: BTreeMap<FlowId, Vec<Msg>>,

    // Messages that have been delivered on an endpoint and are ready to be
    // handled by the peer. They can no longer be dropped at this point and must
    // be delivered.
    delivered: BTreeMap<Destination, Vec<(Source, Msg)>>,
}

impl Network {
    pub fn send(&mut self, source: &Source, envelopes: Vec<Envelope>) {
        for envelope in envelopes {
            let flow_id = (source.clone(), envelope.to);
            self.sent.entry(flow_id).or_default().push(envelope.msg);
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
