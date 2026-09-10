// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//

//! A connection and pool for talking to the ClickHouse server.

use super::Error;
use super::block::Block;
use super::io::packet::client::Encoder;
use super::io::packet::server::Decoder;
use super::packets::client::OXIMETER_HELLO;
use super::packets::client::Packet as ClientPacket;
use super::packets::client::Query;
use super::packets::client::QueryResult;
use super::packets::client::VERSION_MAJOR;
use super::packets::client::VERSION_MINOR;
use super::packets::client::VERSION_PATCH;
use super::packets::server::Hello as ServerHello;
use super::packets::server::Packet as ServerPacket;
use super::packets::server::Progress;
use super::packets::server::REVISION;
use crate::native::packets::client::Settings;
use crate::native::probes;
use futures::SinkExt as _;
use futures::StreamExt as _;
use qorb::backend;
use qorb::backend::Error as QorbError;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use uuid::Uuid;

/// A pool of connections to a ClickHouse server over the native protocol.
pub type Pool = qorb::pool::Pool<Connection>;

/// A type for making connections to a ClickHouse server.
#[derive(Clone, Copy, Debug)]
pub struct Connector;

impl From<Error> for QorbError {
    fn from(e: Error) -> Self {
        QorbError::Other(anyhow::anyhow!(e))
    }
}

#[async_trait::async_trait]
impl backend::Connector for Connector {
    type Connection = Connection;

    async fn connect(
        &self,
        backend: &backend::Backend,
    ) -> Result<Self::Connection, QorbError> {
        Connection::new(backend.address).await.map_err(QorbError::from)
    }

    async fn is_valid(
        &self,
        conn: &mut Self::Connection,
    ) -> Result<(), QorbError> {
        if conn.is_poisoned {
            return Err(QorbError::from(Error::Poisoned));
        }
        conn.ping().await.map_err(QorbError::from)
    }

    async fn on_recycle(
        &self,
        conn: &mut Self::Connection,
    ) -> Result<(), QorbError> {
        if conn.is_poisoned {
            return Err(QorbError::from(Error::Poisoned));
        }
        // We try to cancel an outstanding query. But if there is _no_
        // outstanding query, we sill want to run the validation check of
        // pinging the server. That notifies `qorb` if the server is alive in
        // the case that there was no query to cancel
        match conn.cancel().await.map_err(QorbError::from)? {
            CancelResult::Cancelled => Ok(()),
            CancelResult::NoOutstandingQuery => {
                // No query, so let's run the validation check.
                self.is_valid(conn).await
            }
        }
    }
}

/// The result of attempting to cancel a query.
#[derive(Clone, Copy, Debug)]
pub enum CancelResult {
    /// The query was cancelled successfully.
    Cancelled,
    /// There was no outstanding query to cancel.
    NoOutstandingQuery,
}

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
    has_outstanding_query: bool,
    /// True if the connection should be torn down when returned to the pool.
    ///
    /// The ClickHouse native protocol doesn't include things like message
    /// lengths in its headers. That means we can only know how much data to
    /// read if we can fully, successfully decode the packet. If that fails, or
    /// we have an unknown packet, we don't know how much to read to get to the
    /// start of the next message frame.
    ///
    /// This being true means we hit such an error, and we will signal to the
    /// connection pool that the connection should be reset and replaced
    /// entirely, rather than lent back out to another claimant.
    ///
    /// This is pretty conservative today, and is set any time we hit an
    /// unexpected error. That includes things like unexpected packets, short
    /// reads, or unsupported protocol features -- basically any error except an
    /// explicit `Server::Exception` packet, which we do know how to decode.
    is_poisoned: bool,
}

#[allow(dead_code)]
impl Connection {
    /// Create a new client connection to a ClickHouse server.
    ///
    /// This will connect to the server and exchange the initial handshake
    /// messages.
    pub async fn new(address: SocketAddr) -> Result<Self, Error> {
        let addr = address.ip();
        let stream = TcpStream::connect(address).await?;
        let address = stream.local_addr()?;
        let (reader, writer) = stream.into_split();
        let mut reader = FramedRead::new(reader, Decoder { addr });
        let mut writer = FramedWrite::new(writer, Encoder { addr });
        let server_info =
            Self::exchange_hello(&mut reader, &mut writer).await?;
        Ok(Self {
            address,
            server_info,
            reader,
            writer,
            has_outstanding_query: false,
            is_poisoned: false,
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
        writer
            .send(ClientPacket::Hello(Box::new(OXIMETER_HELLO.clone())))
            .await?;
        let hello = match reader.next().await {
            Some(Ok(ServerPacket::Hello(hello))) => hello,
            Some(Ok(packet)) => {
                probes::unexpected__server__packet!(|| (
                    writer.encoder().addr.to_string(),
                    packet.kind()
                ));
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

    /// Poison the connection, run an exchange with the server, and then clear
    /// the poison flag if the exchange completes. Note that "completes" does
    /// not mean there is no error, only that we can positively say that we've
    /// read to the end of a framed exchange.
    ///
    /// See `Self.is_poisoned` for details.
    async fn poison_and_clear_on_completion<T, F>(
        &mut self,
        exchange: F,
    ) -> Result<T, Error>
    where
        F: AsyncFnOnce(&mut Self) -> Result<T, Error>,
    {
        self.is_poisoned = true;
        let result = exchange(self).await;
        if matches!(&result, Ok(_) | Err(Error::Exception { .. })) {
            self.is_poisoned = false;
        }
        result
    }

    /// Handle an unexpected packet from the server.
    fn on_unexpected_packet(&self, kind: &'static str) -> Error {
        probes::unexpected__server__packet!(|| (
            self.address.ip().to_string(),
            kind,
        ));
        Error::UnexpectedPacket(kind)
    }

    /// Handle a generic error decoding a packet from the server.
    fn on_decode_error(&self, err: Error) -> Error {
        probes::decode__failed!(|| {
            (self.address.ip().to_string(), err.to_string())
        });
        err
    }

    /// Handle being disconnected from the server.
    fn on_disconnected(&self) -> Error {
        probes::disconnected!(|| self.address.ip().to_string());
        Error::Disconnected
    }

    /// Handle a protocol error.
    fn on_protocol_error(&self, err: Error) -> Error {
        probes::protocol__error!(|| {
            (self.address.ip().to_string(), err.to_string())
        });
        err
    }

    /// Send a ping message to the server to check the connection's health.
    ///
    /// This is a cheap way to check that the server is alive and the connection
    /// is still valid. It will await the pong response from the server.
    pub async fn ping(&mut self) -> Result<(), Error> {
        if self.is_poisoned {
            return Err(Error::Poisoned);
        }
        self.poison_and_clear_on_completion(Self::ping_impl).await
    }

    async fn ping_impl(&mut self) -> Result<(), Error> {
        self.writer.send(ClientPacket::Ping).await?;
        match self.reader.next().await {
            Some(Ok(ServerPacket::Pong)) => Ok(()),
            Some(Ok(packet)) => Err(self.on_unexpected_packet(packet.kind())),
            Some(Err(e)) => Err(self.on_decode_error(e)),
            None => Err(self.on_disconnected()),
        }
    }

    // Cancel a running query, if one exists.
    //
    // This returns an error if there is a query and we could not cancel it for
    // some reason.
    async fn cancel(&mut self) -> Result<CancelResult, Error> {
        if !self.has_outstanding_query {
            return Ok(CancelResult::NoOutstandingQuery);
        }
        self.poison_and_clear_on_completion(Self::cancel_impl).await
    }

    async fn cancel_impl(&mut self) -> Result<CancelResult, Error> {
        self.writer.send(ClientPacket::Cancel).await?;
        // Await EOS, throwing everything else away except errors.
        let res = loop {
            match self.reader.next().await {
                Some(Ok(ServerPacket::EndOfStream)) => {
                    break Ok(CancelResult::Cancelled);
                }
                Some(Ok(other_packet)) => {
                    // Do _not_ break here, so we can drain the entire stream if
                    // possible.
                    probes::unexpected__server__packet!(|| (
                        self.address.ip().to_string(),
                        other_packet.kind(),
                    ));
                }
                Some(Err(e)) => break Err(self.on_decode_error(e)),
                None => break Err(self.on_disconnected()),
            };
        };
        self.has_outstanding_query = false;
        res
    }

    /// Send a SQL query that inserts data.
    pub async fn insert(
        &mut self,
        query_id: Uuid,
        query: &str,
        block: Block,
    ) -> Result<QueryResult, Error> {
        self.query_inner(query_id, query, Some(block), Settings::new()).await
    }

    /// Send a SQL query, without any data.
    pub async fn query(
        &mut self,
        query_id: Uuid,
        query: &str,
    ) -> Result<QueryResult, Error> {
        self.query_inner(query_id, query, None, Settings::new()).await
    }

    /// Check that the block to insert matches the expected structure, then
    /// insert it.
    ///
    /// When inserting data, we first send the query itself. The server then
    /// responds with an empty data block that describes the columns of the
    /// table we're inserting into. We're checking that the block we're trying
    /// to insert actually matches, and then inserting it.
    async fn check_block_structure_and_insert(
        &mut self,
        block_to_insert: Block,
        received_block: Block,
    ) -> Result<(), Error> {
        // Server's data block isn't actually empty.
        if received_block.n_rows() != 0 {
            return Err(self.on_protocol_error(Error::ExpectedEmptyDataBlock));
        }

        // Don't concatenate the block, but check that its
        // structure matches what we're about to insert.
        if !block_to_insert.matches_structure(&received_block) {
            return Err(self.on_protocol_error(Error::MismatchedBlockStructure));
        }

        // Finally, send the actual data block and an empty
        // block to tell the server we're finished.
        self.writer.send(ClientPacket::Data(block_to_insert)).await?;
        self.writer.send(ClientPacket::Data(Block::empty())).await
    }

    #[cfg(test)]
    async fn query_with_settings(
        &mut self,
        query_id: Uuid,
        query: &str,
        settings: Settings,
    ) -> Result<QueryResult, Error> {
        self.query_inner(query_id, query, None, settings).await
    }

    // Send a SQL query, possibly with data.
    //
    // If data is present, it is sent after the SQL query itself. An error is
    // returned if the server indicates that the block structure required by the
    // insert query doesn't match that of the provided block.
    //
    // IMPORTANT: We do not currently validate that data is provided iff the
    // query is an INSERT statement! Callers are required to ensure that they
    // provide data if and only if the query requires it.
    async fn query_inner(
        &mut self,
        query_id: Uuid,
        query: &str,
        maybe_data: Option<Block>,
        settings: Settings,
    ) -> Result<QueryResult, Error> {
        if self.is_poisoned {
            return Err(Error::Poisoned);
        }
        self.poison_and_clear_on_completion(async |conn| {
            conn.query_inner_impl(query_id, query, maybe_data, settings).await
        })
        .await
    }

    async fn query_inner_impl(
        &mut self,
        query_id: Uuid,
        query: &str,
        maybe_data: Option<Block>,
        settings: Settings,
    ) -> Result<QueryResult, Error> {
        let mut query_result = QueryResult {
            id: query_id,
            query: String::from(query),
            progress: Progress::default(),
            data: None,
            profile_info: None,
            profile_events: None,
        };
        let query = Query::new_with_settings(
            query_result.id,
            self.address,
            query,
            settings,
        );
        self.writer.send(ClientPacket::Query(Box::new(query))).await?;
        self.has_outstanding_query = true;

        // If we have data to send, wait for the server to send an empty block
        // that describes its structure.
        if let Some(block_to_insert) = maybe_data {
            let res: Result<(), Error> = loop {
                match self.reader.next().await {
                    Some(Ok(packet)) => match packet {
                        ServerPacket::Hello(_)
                            | ServerPacket::Pong
                            // The server should only send this after we've
                            // inserted our data.
                            | ServerPacket::EndOfStream =>
                        {
                            break Err(self.on_unexpected_packet(packet.kind()));
                        }
                        ServerPacket::Data(block) => {
                            break self.check_block_structure_and_insert(block_to_insert, block).await;
                        }
                        ServerPacket::Exception(exceptions) => {
                            break Err(Error::Exception { exceptions })
                        }
                        ServerPacket::Progress(progress) => {
                            query_result.progress += progress
                        }
                        ServerPacket::ProfileInfo(info) => {
                            let _ = query_result.profile_info.replace(info);
                        }
                        ServerPacket::TableColumns(columns) => {
                            if !block_to_insert
                                .insertable_into(&columns)
                            {
                                break Err(self.on_protocol_error(Error::MismatchedBlockStructure));
                            }
                        }
                        ServerPacket::ProfileEvents(block) => {
                            let _ = query_result.profile_events.replace(block);
                        }
                    },
                    Some(Err(e)) => break Err(self.on_decode_error(e)),
                    None => break Err(self.on_disconnected()),
                }
            };
            if let Err(e) = res {
                self.has_outstanding_query = false;
                return Err(e);
            }
        }

        // Now wait for the remainder of the query to execute.
        let res = loop {
            match self.reader.next().await {
                Some(Ok(packet)) => match packet {
                    ServerPacket::Hello(_)
                    | ServerPacket::Pong
                    | ServerPacket::TableColumns(_) => {
                        break Err(self.on_unexpected_packet(packet.kind()));
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
                        break Err(Error::Exception { exceptions });
                    }
                    ServerPacket::Progress(progress) => {
                        query_result.progress += progress
                    }
                    ServerPacket::EndOfStream => break Ok(query_result),
                    ServerPacket::ProfileInfo(info) => {
                        let _ = query_result.profile_info.replace(info);
                    }
                    ServerPacket::ProfileEvents(block) => {
                        let _ = query_result.profile_events.replace(block);
                    }
                },
                Some(Err(e)) => break Err(self.on_decode_error(e)),
                None => break Err(self.on_disconnected()),
            }
        };
        self.has_outstanding_query = false;
        res
    }
}

#[cfg(test)]
mod tests {
    use super::Connector;
    use crate::native::block::Block;
    use crate::native::block::Column;
    use crate::native::block::DataType;
    use crate::native::block::ValueArray;
    use crate::native::connection::Connection;
    use crate::native::packets::client::Setting;
    use crate::native::packets::client::Settings;
    use indexmap::IndexMap;
    use omicron_test_utils::dev::clickhouse::ClickHouseDeployment;
    use omicron_test_utils::dev::test_setup_log;
    use qorb::backend::Connector as _;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::sync::oneshot;
    use uuid::Uuid;

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
            .query(
                Uuid::new_v4(),
                "SELECT number FROM system.numbers LIMIT 10;",
            )
            .await
            .expect("Should have run query");
        println!("{data:#?}");
        let block = data.data.as_ref().expect("Should have a data block");
        assert_eq!(block.n_columns(), 1);
        assert_eq!(block.n_rows(), 10);
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
            .query(Uuid::new_v4(), "SELECT toNullable(number) as number FROM system.numbers LIMIT 10;")
            .await
            .expect("Should have run query");
        println!("{data:#?}");
        let block = data.data.as_ref().expect("Should have a data block");
        assert_eq!(block.n_columns(), 1);
        assert_eq!(block.n_rows(), 10);
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
            .query(
                Uuid::new_v4(),
                "SELECT arrayJoin([[4, 5, 6], [7, 8]]) AS arr;",
            )
            .await
            .expect("Should have run query");
        println!("{data:#?}");
        let block = data.data.as_ref().expect("Should have a data block");
        assert_eq!(block.n_columns(), 1);
        assert_eq!(block.n_rows(), 2);
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
            .query(Uuid::new_v4(), "SELECT [1, NULL] AS arr;")
            .await
            .expect("Should have run query");
        println!("{data:#?}");
        let block = data.data.as_ref().expect("Should have a data block");
        assert_eq!(block.n_columns(), 1);
        assert_eq!(block.n_rows(), 1);
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
            println!("query task: running query: '{QUERY}'");
            let res = conn.query(Uuid::new_v4(), QUERY);
            tokio::select! {
                query_result = res => {
                    println!("query task: query future awaited");
                    Some(query_result)
                }
                _ = cancel_rx => {
                    println!("query task: cancel rx awaited, cancelling the query");
                    conn.cancel().await.unwrap();
                    println!("query task: query cancelled");
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
            .query(Uuid::new_v4(), QUERY)
            .await
            .expect("New query after cancel should have worked");
        let Some(block) = &result.data else {
            panic!("Should have received data, but found None");
        };
        assert_eq!(block.n_columns(), 1);
        assert_eq!(block.n_rows(), 1);
        let (name, col) = block.columns.first().unwrap();
        assert_eq!(name, "timestamp");
        assert!(matches!(col.data_type, DataType::DateTime(_)));
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_insert_and_select_data() {
        let logctx = test_setup_log("test_insert_and_select_data");
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let mut conn =
            Connection::new(db.native_address().into()).await.unwrap();
        conn.query(
            Uuid::new_v4(),
            "CREATE TABLE tmp (x UInt8, name String) ENGINE = Memory",
        )
        .await
        .expect("Failed to create test table");

        let block = Block {
            name: String::new(),
            info: Default::default(),
            columns: IndexMap::from([
                (
                    String::from("x"),
                    Column::from(ValueArray::from(vec![0u8, 1, 2, 3])),
                ),
                (
                    String::from("name"),
                    Column::from(ValueArray::from(vec![
                        String::from("a"),
                        String::from("b"),
                        String::from("c"),
                        String::from("d"),
                    ])),
                ),
            ]),
        };
        let _ = conn
            .insert(
                Uuid::new_v4(),
                "INSERT INTO tmp FORMAT Native",
                block.clone(),
            )
            .await
            .expect("Should have inserted data");

        let result = conn
            .query(Uuid::new_v4(), "SELECT * FROM tmp")
            .await
            .expect("Failed to select data");
        let actual_block =
            result.data.as_ref().expect("Failed to select block");
        assert_eq!(
            &block, actual_block,
            "Inserted and selected data do not match"
        );
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_insert_and_select_uuid() {
        let logctx = test_setup_log("test_insert_and_select_uuid");
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let mut conn =
            Connection::new(db.native_address().into()).await.unwrap();
        conn.query(Uuid::new_v4(), "CREATE TABLE tmp (x UUID) ENGINE = Memory")
            .await
            .expect("Failed to create test table");

        const ID: uuid::Uuid =
            uuid::uuid!("00112233-4455-6677-8899-aabbccddeeff");
        let block = Block {
            name: String::new(),
            info: Default::default(),
            columns: IndexMap::from([(
                String::from("x"),
                Column::from(ValueArray::from(vec![ID])),
            )]),
        };
        let _ = conn
            .insert(
                Uuid::new_v4(),
                "INSERT INTO tmp FORMAT Native",
                block.clone(),
            )
            .await
            .expect("Should have inserted data");
        let result = conn
            .query(Uuid::new_v4(), "SELECT toString(x) AS x FROM tmp")
            .await
            .expect("Failed to select data");
        let actual_block =
            result.data.as_ref().expect("Failed to select block");
        let ValueArray::String(ids) = actual_block.column_values("x").unwrap()
        else {
            panic!();
        };
        let id: uuid::Uuid = ids[0].parse().unwrap();
        assert_eq!(id, ID, "UUID stored incorrectly in ClickHouse");
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_desynced_connections_are_poisoned() {
        let logctx = test_setup_log("ensure_desynced_connections_are_poisoned");
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let mut conn =
            Connection::new(db.native_address().into()).await.unwrap();
        assert!(!conn.is_poisoned, "new connection should not be poisoned");

        // Run a query that gives us an unsupported data type. We only learn
        // this partway through the query, at which point we abort processing
        // the rest of the stream. That should leave some junk in the buffers,
        // and we definitely want to mark the connection as poisoned.
        let res = conn.query(Uuid::new_v4(), "SELECT map('a', 1) AS m").await;
        assert!(
            res.is_err(),
            "expected decoding error with unsupported map data type"
        );
        assert!(
            conn.is_poisoned,
            "connection should be poisoned after a failure mid-stream"
        );

        // Sanity check, but make sure we report to qorb that the connections
        // are unhealthy.
        let connector = Connector;
        assert!(connector.is_valid(&mut conn).await.is_err());
        assert!(connector.on_recycle(&mut conn).await.is_err());

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_dropped_query_future_poisons_connection() {
        let logctx =
            test_setup_log("ensure_dropped_query_future_poisons_connection");
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let mut conn =
            Connection::new(db.native_address().into()).await.unwrap();
        assert!(!conn.is_poisoned, "new connection should not be poisoned");

        // Run an infinite query and drop it right away. We should poison the
        // connection because it's not been fully completed.
        let res = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            conn.query(Uuid::new_v4(), "SELECT COUNT(*) FROM system.numbers"),
        )
        .await;
        assert!(res.is_err(), "should have timed out");
        assert!(
            conn.is_poisoned,
            "dropping a query future should leave a connection poisoned"
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn ensure_settings_correctly_encoded() {
        let logctx = test_setup_log("ensure_settings_correctly_encoded");
        let mut db = ClickHouseDeployment::new_single_node(&logctx)
            .await
            .expect("Failed to start ClickHouse");
        let mut conn =
            Connection::new(db.native_address().into()).await.unwrap();

        // Simple query with some non-empty settings.
        let mut settings = Settings::new();
        settings.insert(
            "max_threads".into(),
            Setting { value: "1".into(), important: false },
        );
        let query_result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            conn.query_with_settings(
                Uuid::new_v4(),
                "SELECT toString(getSetting('max_threads')) AS x",
                settings,
            ),
        )
        .await
        .expect("query with custom settings should not have timed out")
        .expect("query with custom settings should have succeeded");
        let block = query_result.data.expect("query should have data");
        let ValueArray::String(values) = block.column_values("x").unwrap()
        else {
            panic!("expected a String result");
        };
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], "1", "server failed to apply settings");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
