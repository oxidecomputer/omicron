// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Two-phase commit (2PC) layer for the bootstore

use crate::db::DbId;

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv6Addr;
use std::sync::Arc;

use sprockets_common::certificates::Ed25519Certificate;
use sprockets_common::Sha3_256Digest;
use sprockets_host::Identity;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

pub const PORT: u16 = 2121;

/// The current state of a replica for a given transaction
pub enum TransactionState {
    Init,
    Prepared,
    Committed,
    Aborted,
}
/// A two phase commit (2PC) transaction identifier. Transactions either commit or abort.
///
/// Users submit transactions to a coordinator and retry indefinitely for them
/// to commit or tell them to abort. By giving users explicit control over
/// abort we move the policy decision out of the protocol and keep it simple.
///
/// It is expected that transactions will be driven by Steno, with their IDs
/// stored in CockroachDB. Before issuing transactions, users should record
/// them in CockroachDb so they can be restarted in the case of failure
/// or aborted.
pub struct TransactionId {
    // Database structures are specifically adapted to append-only log style
    // blog storage.
    db_id: DbId,

    // The monotonically increasing generation number of the data value
    gen: u64,
}

/// A sprockets server endpoint. We don't know the identity of the endpoint
/// until the session is established.
pub struct StorageNode<Chan>
where
    Chan: AsyncRead + AsyncWrite,
{
    addr: Ipv6Addr,
    session: sprockets_host::Session<Chan>,
    id: Identity,
}

pub enum TransactionOp {
    // This is an instruction from RSS to generate a rack secret
    //
    // The coordinator will generate a rack secret and distribute shares at
    // database generation 0.
    InitializeTrustQuorum {
        rack_secret_threshold: usize,
        member_device_id_certs: Vec<Ed25519Certificate>,
    },
}

pub struct Transaction {
    id: TransactionId,
    op: TransactionOp,
    addrs: BTreeSet<Ipv6Addr>,
}

/// The coordinator of the 2PC protocol. It establishes connections
/// to [`StorageNode`]]s and transfers data via 2PC.
pub struct Coordinator {
    // We don't know which address maps to which identity, but we know which
    // identity maps to which data. We use an Arc for data, because in some
    // cases the data is shared and can be somewhat large (a few MiB) and we want to
    // eliminate copies.
    //
    // The data is serialized before we get it. The handlers
    // on the nodes know how to deserialize and interpret it.
    data: BTreeMap<Identity, Arc<Vec<u8>>>,
}
