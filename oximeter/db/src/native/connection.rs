// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! A connection and pool for talking to the ClickHouse server.

use super::packets::server::Packet as ServerPacket;
use super::packets::{
    client::{Packet as ClientPacket, Query, QueryResult},
    server::Progress,
};
use super::{
    io::packet::{client::Encoder, server::Decoder},
    packets::{
        client::{OXIMETER_HELLO, VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH},
        server::{Hello as ServerHello, REVISION},
    },
    Error,
};
use crate::native::probes;
use futures::{SinkExt as _, StreamExt as _};
use std::net::SocketAddr;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
use tokio_util::codec::{FramedRead, FramedWrite};
use uuid::Uuid;

/// A connection to a ClickHouse server.
///
/// This connection object can be used to talk to a ClickHouse server through
/// its native protocol.
#[derive(Debug)]
pub struct Connection {
    /// Our local socket address.
    address: SocketAddr,
    /// The identity of the server we're talking to.
    server_info: ServerHello,
    /// A reader for decoding packets from the server.
    reader: FramedRead<OwnedReadHalf, Decoder>,
    /// A writer for encoding packets to the server.
    writer: FramedWrite<OwnedWriteHalf, Encoder>,
    /// True if we are currently executing a query.
    outstanding_query: bool,
}

#[allow(dead_code)]
impl Connection {
    /// Create a new client connection to a ClickHouse server.
    ///
    /// This will connect to the server and exchange the initial handshake
    /// messages.
    pub async fn new(address: SocketAddr) -> Result<Self, Error> {
        let stream = TcpStream::connect(address).await?;
        let address = stream.local_addr()?;
        let (reader, writer) = stream.into_split();
        let mut reader = FramedRead::new(reader, Decoder);
        let mut writer = FramedWrite::new(writer, Encoder);
        let server_info =
            Self::exchange_hello(&mut reader, &mut writer).await?;
        Ok(Self {
            address,
            server_info,
            reader,
            writer,
            outstanding_query: false,
        })
    }

    /// Return the identity of the connected server.
    pub fn server_info(&self) -> &ServerHello {
        &self.server_info
    }

    /// Exchange hello messages with the server.
    ///
    /// This is run automatically at the time we connnect to it.
    async fn exchange_hello(
        reader: &mut FramedRead<OwnedReadHalf, Decoder>,
        writer: &mut FramedWrite<OwnedWriteHalf, Encoder>,
    ) -> Result<ServerHello, Error> {
        writer.send(ClientPacket::Hello(OXIMETER_HELLO.clone())).await?;
        let hello = match reader.next().await {
            Some(Ok(ServerPacket::Hello(hello))) => hello,
            Some(Ok(packet)) => {
                probes::unexpected__server__packet!(|| packet.kind());
                return Err(Error::UnexpectedPacket(packet.kind()));
            }
            Some(Err(e)) => return Err(e),
            None => return Err(Error::Disconnected),
        };
        if hello.name != "ClickHouse"
            || hello.version_major != VERSION_MAJOR
            || hello.version_minor != VERSION_MINOR
            || hello.revision != REVISION
            || hello.version_patch != VERSION_PATCH
        {
            return Err(Error::UnrecognizedClickHouseServer {
                name: hello.name,
                major_version: hello.version_major,
                minor_version: hello.version_minor,
                patch_version: hello.version_patch,
                revision: hello.revision,
            });
        }
        Ok(hello)
    }

    /// Send a ping message to the server to check the connection's health.
    ///
    /// This is a cheap way to check that the server is alive and the connection
    /// is still valid. It will await the pong response from the server.
    pub async fn ping(&mut self) -> Result<(), Error> {
        self.writer.send(ClientPacket::Ping).await?;
        match self.reader.next().await {
            Some(Ok(ServerPacket::Pong)) => Ok(()),
            Some(Ok(packet)) => {
                probes::unexpected__server__packet!(|| packet.kind());
                Err(Error::UnexpectedPacket(packet.kind()))
            }
            Some(Err(e)) => Err(e),
            None => Err(Error::Disconnected),
        }
    }

    // Cancel a running query, if one exists.
    async fn cancel(&mut self) -> Result<(), Error> {
        if self.outstanding_query {
            self.writer.send(ClientPacket::Cancel).await?;
            // Await EOS, throwing everything else away except errors.
            let res = loop {
                match self.reader.next().await {
                    Some(Ok(ServerPacket::EndOfStream)) => break Ok(()),
                    Some(Ok(other_packet)) => {
                        probes::unexpected__server__packet!(
                            || other_packet.kind()
                        );
                    }
                    Some(Err(e)) => break Err(e),
                    None => break Err(Error::Disconnected),
                };
            };
            self.outstanding_query = false;
            return res;
        }
        Ok(())
    }

    /// Send a SQL query, possibly with data.
    pub async fn query(&mut self, query: &str) -> Result<QueryResult, Error> {
        let mut query_result = QueryResult {
            id: Uuid::new_v4(),
            progress: Progress::default(),
            data: None,
            profile_info: None,
            profile_events: None,
        };
        let query = Query::new(query_result.id, self.address, query);
        self.writer.send(ClientPacket::Query(query)).await?;
        probes::packet__sent!(|| "Query");
        self.outstanding_query = true;
        let res = loop {
            match self.reader.next().await {
                Some(Ok(packet)) => match packet {
                    ServerPacket::Hello(_) => {
                        probes::unexpected__server__packet!(|| "Hello");
                        break Err(Error::UnexpectedPacket("Hello"));
                    }
                    ServerPacket::Data(block) => {
                        // Empty blocks are sent twice: the beginning of the
                        // query so that the client knows the table structure,
                        // and then the end to signal the last data transfer.
                        if !block.is_empty() {
                            match query_result.data.as_mut() {
                                Some(data) => data.concat(block)?,
                                None => {
                                    let _ = query_result.data.insert(block);
                                }
                            }
                        }
                    }
                    ServerPacket::Exception(exceptions) => {
                        break Err(Error::Exception { exceptions })
                    }
                    ServerPacket::Progress(progress) => {
                        query_result.progress += progress
                    }
                    ServerPacket::Pong => {
                        probes::unexpected__server__packet!(|| "Hello");
                        break Err(Error::UnexpectedPacket("Pong"));
                    }
                    ServerPacket::EndOfStream => break Ok(query_result),
                    ServerPacket::ProfileInfo(info) => {
                        let _ = query_result.profile_info.replace(info);
                    }
                    ServerPacket::ProfileEvents(block) => {
                        let _ = query_result.profile_events.replace(block);
                    }
                },
                Some(Err(e)) => break Err(e),
                None => break Err(Error::Disconnected),
            }
        };
        self.outstanding_query = false;
        res
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::native::{
        block::{DataType, ValueArray},
        connection::Connection,
    };
    use omicron_test_utils::dev::{
        clickhouse::ClickHouseDeployment, test_setup_log,
    };
    use tokio::sync::{oneshot, Mutex};

    #[tokio::test]
    async fn test_exchange_hello() {
        let logctx = test_setup_log("test_exchange_hello");
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let _ = Connection::new(db.native_address().into()).await.unwrap();
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_basic_select_query() {
        let logctx = test_setup_log("test_basic_select_query");
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let mut conn =
            Connection::new(db.native_address().into()).await.unwrap();
        let data = conn
            .query("SELECT number FROM system.numbers LIMIT 10;")
            .await
            .expect("Should have run query");
        println!("{data:#?}");
        let block = data.data.as_ref().expect("Should have a data block");
        assert_eq!(block.n_columns, 1);
        assert_eq!(block.n_rows, 10);
        let (name, col) = block.columns.first().unwrap();
        assert_eq!(name, "number");
        let ValueArray::UInt64(values) = &col.values else {
            panic!("Expected UInt64 values from query, found {col:?}");
        };
        assert_eq!(values.len(), 10);
        assert_eq!(values, &(0u64..10).collect::<Vec<_>>());
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_select_nullable_column() {
        let logctx = test_setup_log("test_select_nullable_column");
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let mut conn =
            Connection::new(db.native_address().into()).await.unwrap();
        let data = conn
            .query("SELECT toNullable(number) as number FROM system.numbers LIMIT 10;")
            .await
            .expect("Should have run query");
        println!("{data:#?}");
        let block = data.data.as_ref().expect("Should have a data block");
        assert_eq!(block.n_columns, 1);
        assert_eq!(block.n_rows, 10);
        let (name, col) = block.columns.first().unwrap();
        assert_eq!(name, "number");
        assert_eq!(
            col.data_type,
            DataType::Nullable(Box::new(DataType::UInt64))
        );
        let ValueArray::Nullable { is_null, values } = &col.values else {
            panic!("Expected a Nullable columne, found: {col:?}");
        };
        let ValueArray::UInt64(values) = &**values else {
            panic!("Expected UInt64 values from query, found {col:?}");
        };
        assert_eq!(is_null.len(), values.len());
        assert!(!is_null.iter().any(|x| *x));
        assert_eq!(values.len(), 10);
        assert_eq!(values, &(0u64..10).collect::<Vec<_>>());
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_select_array_column() {
        let logctx = test_setup_log("test_select_array_column");
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let mut conn =
            Connection::new(db.native_address().into()).await.unwrap();
        let data = conn
            .query("SELECT arrayJoin([[4, 5, 6], [7, 8]]) AS arr;")
            .await
            .expect("Should have run query");
        println!("{data:#?}");
        let block = data.data.as_ref().expect("Should have a data block");
        assert_eq!(block.n_columns, 1);
        assert_eq!(block.n_rows, 2);
        let (name, col) = block.columns.first().unwrap();
        assert_eq!(name, "arr");
        assert_eq!(col.data_type, DataType::Array(Box::new(DataType::UInt8)));
        let ValueArray::Array { values, inner_type: DataType::UInt8 } =
            &col.values
        else {
            panic!("Expected arrays of UInt8 values from query, found {col:?}");
        };
        assert_eq!(values.len(), 2);
        let ValueArray::UInt8(arr) = &values[0] else {
            panic!("Expected each array to have UInt8 type");
        };
        assert_eq!(arr, &[4, 5, 6]);
        let ValueArray::UInt8(arr) = &values[1] else {
            panic!("Expected each array to have UInt8 type");
        };
        assert_eq!(arr, &[7, 8]);
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_select_array_of_nullable_column() {
        let logctx = test_setup_log("test_select_array_of_nullable_column");
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let mut conn =
            Connection::new(db.native_address().into()).await.unwrap();
        let data = conn
            .query("SELECT [1, NULL] AS arr;")
            .await
            .expect("Should have run query");
        println!("{data:#?}");
        let block = data.data.as_ref().expect("Should have a data block");
        assert_eq!(block.n_columns, 1);
        assert_eq!(block.n_rows, 1);
        let (name, col) = block.columns.first().unwrap();
        assert_eq!(name, "arr");
        assert_eq!(
            col.data_type,
            DataType::Array(Box::new(DataType::Nullable(Box::new(
                DataType::UInt8
            ))))
        );
        let ValueArray::Array { values, inner_type } = &col.values else {
            panic!("Expected arrays of UInt8 values from query, found {col:?}");
        };
        assert_eq!(inner_type, &DataType::Nullable(Box::new(DataType::UInt8)));
        assert_eq!(values.len(), 1);
        let ValueArray::Nullable { is_null, values } = &values[0] else {
            panic!();
        };
        assert_eq!(is_null, &[false, true]);
        let ValueArray::UInt8(arr) = &**values else {
            panic!("Expected each array to have UInt8 type");
        };
        assert_eq!(arr, &[1, 0]);
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_send_cancel_with_no_query() {
        let logctx = test_setup_log("test_send_cancel");
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let mut conn =
            Connection::new(db.native_address().into()).await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            conn.cancel(),
        )
        .await
        .expect(
            "Should not timeout when sending cancel with no outstanding query",
        )
        .expect("Should succeed when sending cancel with no outstanding query");
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Smoke test that we _can_ cancel a running query.
    //
    // The existing `Connection::cancel()` method takes `&mut self`, which means
    // you can't easily use it to cancel a query you're running, since
    // `Connection::query()` also takes `&mut self`. This test is intended to
    // prove that the cancellation method itself works, and to serve as a
    // strawman for how one _could_ cancel a running query. We'll wait until we
    // have more experience to see if that's an important feature to support,
    // and how best to do that.
    #[tokio::test]
    async fn test_can_cancel_query() {
        let logctx = test_setup_log("test_send_cancel");
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let conn = Connection::new(db.native_address().into()).await.unwrap();

        // All methods on `Connection` take an exclusive reference to self,
        // which means you can't really cancel a query without a bit of
        // ceremony. That's shown here.
        //
        // We basically put a mutex around the connection, and create a
        // cancellation channel. Then both the connection and the receive-side
        // go into a task, which `tokio::select!`s between the query completion
        // and the cancellation token. It returns a result if that completed
        // first, or None if it was cancelled.
        let conn = Arc::new(Mutex::new(conn));
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let conn_ = conn.clone();
        println!("Spawning task to run a cancellable query");
        let task = tokio::spawn(async move {
            let mut conn = conn_.lock().await;
            const QUERY: &str = "select count(*) from system.numbers";
            println!("query task: unning query: '{QUERY}'");
            let res = conn.query(QUERY);
            tokio::select! {
                query_result = res => {
                    println!("query task: uery future awaited");
                    Some(query_result)
                }
                _ = cancel_rx => {
                    println!("query task: ancel rx awaited, cancelling the query");
                    conn.cancel().await.unwrap();
                    println!("query task: uery cancelled");
                    None
                }
            }
        });

        println!("test task: sleeping");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        println!("test task: awoken, sending cancel tx");
        cancel_tx.send(()).unwrap();
        println!("test task: cancel tx sent");
        let mut c = conn.lock().await;
        println!("test task: acquired conn lock again");
        let result = task.await;
        println!("test task: query task awaited: {result:?}");
        const QUERY: &str = "select now() as timestamp";
        println!("test task: running '{QUERY}'");
        let result = c
            .query(QUERY)
            .await
            .expect("New query after cancel should have worked");
        let Some(block) = &result.data else {
            panic!("Should have received data, but found None");
        };
        assert_eq!(block.n_columns, 1);
        assert_eq!(block.n_rows, 1);
        let (name, col) = block.columns.first().unwrap();
        assert_eq!(name, "timestamp");
        assert_eq!(col.data_type, DataType::DateTime);
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
