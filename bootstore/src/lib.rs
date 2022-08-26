//! The two communication paths for the bootstore:
//!
//! RSS -> Sled Agent -> Coordinator -> Storage Nodes
//! Nexus -> Steno -> Sled Agent -> Coordinator -> Storage Nodes
//!
//!
//! Since some trust quorum membership information that is input via RSS must
//! make its way into CockroachDb so that reconfiguration works, we will load
//! that information from the trust quorum database, parse it, and write
//! it to CockroachDB when we start it up.

use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use std::sync::Arc;

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

/// The database used store blobs.
///
/// We separate them because they are encrypted and accessed differently.
pub enum Db {
    /// Used pre-rack unlock: Contains key shares and membership data
    TrustQuorum,

    /// Used post-rack unlock: Contains information necessary for setting
    /// up NTP.
    NetworkConfig,
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
    db: Db,

    // The name of the item in the database
    // The monotonically increasing generation number of the data value
    gen: u64,
}

/// A sprockets server endpoint. We don't know the identity of the endpoint
/// until the session is established.
pub struct StorageNode {
    addr: Ipv6Addr,
    session: sprockets_host::Session<TcpStream>,
    id: Identity,
}

/// The coordinator of the 2PC protocol. It establishes connections
/// to the storage nodes and transfers data via 2PC.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
