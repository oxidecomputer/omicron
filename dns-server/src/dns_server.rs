// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Guts of the DNS (protocol) server within our DNS server program
//!
//! The facilities here handle binding a UDP socket, receiving DNS messages on
//! that socket, and replying to them.

use crate::dns_types::DnsRecord;
use crate::storage;
use crate::storage::QueryError;
use crate::storage::Store;
use anyhow::anyhow;
use anyhow::Context;
use pretty_hex::*;
use serde::Deserialize;
use slog::{debug, error, info, o, trace, Logger};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tokio::net::UdpSocket;
use trust_dns_proto::op::header::Header;
use trust_dns_proto::op::response_code::ResponseCode;
use trust_dns_proto::rr::rdata::SRV;
use trust_dns_proto::rr::record_data::RData;
use trust_dns_proto::rr::record_type::RecordType;
use trust_dns_proto::rr::{Name, Record};
use trust_dns_proto::serialize::binary::{
    BinDecodable, BinDecoder, BinEncoder,
};
use trust_dns_server::authority::MessageResponse;
use trust_dns_server::authority::{MessageRequest, MessageResponseBuilder};
use uuid::Uuid;

/// Configuration related to the DNS server
#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The address to listen for DNS requests on
    pub bind_address: SocketAddr,
}

/// Handle to the DNS server
///
/// Dropping this handle shuts down the DNS server.
pub struct ServerHandle {
    local_address: SocketAddr,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.handle.abort()
    }
}

impl ServerHandle {
    pub fn local_address(&self) -> &SocketAddr {
        &self.local_address
    }
}

/// DNS (protocol) server
///
/// You construct one of these with [`Server::start()`].  But what you get back
/// is a [`ServerHandle`].  You don't deal with the Server directly.
pub struct Server {
    log: Logger,
    store: storage::Store,
    server_socket: Arc<UdpSocket>,
}

impl Server {
    /// Starts a DNS server whose DNS data comes from the given `store`
    pub async fn start(
        log: Logger,
        store: storage::Store,
        config: &Config,
    ) -> anyhow::Result<ServerHandle> {
        let server_socket = Arc::new(
            UdpSocket::bind(config.bind_address).await.with_context(|| {
                format!(
                    "DNS server start: UDP bind to {:?}",
                    config.bind_address
                )
            })?,
        );

        let local_address = server_socket.local_addr().context(
            "DNS server start: failed to get local address of bound socket",
        )?;

        info!(&log, "DNS server bound to address";
            "local_address" => ?local_address
        );

        let server = Server { log, store, server_socket };
        let handle = tokio::task::spawn(server.run());
        Ok(ServerHandle { local_address, handle })
    }

    async fn run(self) -> anyhow::Result<()> {
        // The guts of the DNS server: read packets from the bound socket and
        // handle them.
        loop {
            let mut buf = vec![0u8; 16384];
            let (n, client_addr) = self
                .server_socket
                .recv_from(&mut buf)
                .await
                .context("receiving packet from UDP listen socket")?;
            buf.resize(n, 0);

            let req_id = Uuid::new_v4();
            let log = self.log.new(o!(
                "req_id" => req_id.to_string(),
                "peer_addr" => client_addr.to_string(),
            ));

            let request = Request {
                log,
                store: self.store.clone(),
                socket: self.server_socket.clone(),
                client_addr,
                packet: buf,
                req_id,
            };

            // TODO-robustness We should cap the number of tokio tasks that
            // we're willing to spawn if we receive a flood of requests.
            tokio::spawn(handle_dns_packet(request));
        }
    }
}

/// Describes an incoming DNS request
struct Request {
    log: Logger,
    store: Store,
    socket: Arc<UdpSocket>,
    client_addr: SocketAddr,
    packet: Vec<u8>,
    #[allow(dead_code)]
    req_id: Uuid,
}

async fn handle_dns_packet(request: Request) {
    let log = &request.log;
    let buf = &request.packet;

    trace!(&log, "buffer"; "buffer" => ?buf.hex_dump());

    // Decode the message.
    let mut dec = BinDecoder::new(&buf);
    let mr = match MessageRequest::read(&mut dec) {
        Ok(mr) => mr,
        Err(error) => {
            error!(log, "failed to parse incoming DNS message: {:#}", error);
            return;
        }
    };

    // Handle the message.
    match handle_dns_message(&request, &mr).await {
        Ok(_) => (),
        Err(error) => {
            let header = Header::response_from_request(mr.header());
            let rb_servfail = MessageResponseBuilder::from_message_request(&mr);
            error!(log, "failed to handle incoming DNS message: {:#}", error);
            match error {
                RequestError::NxDomain(_) => {
                    let rb_nxdomain =
                        MessageResponseBuilder::from_message_request(&mr);
                    respond_nxdomain(
                        &request,
                        rb_nxdomain,
                        rb_servfail,
                        &header,
                    )
                    .await
                }
                RequestError::ServFail(_) => {
                    let rb_servfail =
                        MessageResponseBuilder::from_message_request(&mr);
                    respond_servfail(&request, rb_servfail, &header).await
                }
            };
        }
    }
}

/// Describes how to respond to a particular request failure
#[derive(Debug, Error)]
enum RequestError {
    #[error("NXDOMAIN: {0:#}")]
    NxDomain(#[source] QueryError),
    #[error("SERVFAIL: {0:#}")]
    ServFail(#[source] anyhow::Error),
}

impl From<QueryError> for RequestError {
    fn from(source: QueryError) -> Self {
        match &source {
            QueryError::NoName(_) => RequestError::NxDomain(source),
            // Bail with servfail when this query is for a zone that we don't
            // own (and other server-side failures) so that resolvers will look
            // to other DNS servers for this query.
            QueryError::NoZone(_)
            | QueryError::QueryFail(_)
            | QueryError::ParseFail(_) => RequestError::ServFail(source.into()),
        }
    }
}

fn dns_record_to_record(
    name: &Name,
    record: DnsRecord,
) -> Result<Record, RequestError> {
    match record {
        DnsRecord::A(addr) => {
            let mut a = Record::new();
            a.set_name(name.clone())
                .set_rr_type(RecordType::A)
                .set_data(Some(RData::A(addr)));
            Ok(a)
        }

        DnsRecord::AAAA(addr) => {
            let mut aaaa = Record::new();
            aaaa.set_name(name.clone())
                .set_rr_type(RecordType::AAAA)
                .set_data(Some(RData::AAAA(addr)));
            Ok(aaaa)
        }

        DnsRecord::SRV(crate::dns_types::SRV {
            prio,
            weight,
            port,
            target,
        }) => {
            let tgt = Name::from_str(&target).map_err(|error| {
                RequestError::ServFail(anyhow!(
                    "serialization failed due to bad SRV target {:?}: {:#}",
                    &target,
                    error
                ))
            })?;
            let mut srv = Record::new();
            srv.set_name(name.clone())
                .set_rr_type(RecordType::SRV)
                .set_data(Some(RData::SRV(SRV::new(prio, weight, port, tgt))));
            Ok(srv)
        }
    }
}

/// Handle a well-formed, decoded DNS query
async fn handle_dns_message(
    request: &Request,
    mr: &MessageRequest,
) -> Result<(), RequestError> {
    let log = &request.log;
    let store = &request.store;
    debug!(&log, "message_request"; "mr" => #?mr);

    let header = Header::response_from_request(mr.header());
    let query = mr.query();
    let name = query.original().name().clone();
    let records = store.query(mr)?;
    let rb = MessageResponseBuilder::from_message_request(mr);
    let mut additional_records = vec![];
    let response_records = records
        .into_iter()
        .map(|record| {
            let record = dns_record_to_record(&name, record)?;

            // DNS allows for the server to return additional records
            // that weren't explicitly asked for by the client but that
            // the server expects the client will want. The records
            // corresponding to a lookup on a SRV target is one such case.
            // We opportunistically attempt to resolve the target here
            // and if successful return those additional records in the
            // response.
            // NOTE: we only do this one-layer deep.
            if let Some(RData::SRV(srv)) = record.data() {
                let target_records =
                    store.query_name(srv.target()).map(|records| {
                        records
                            .into_iter()
                            .map(|record| {
                                dns_record_to_record(srv.target(), record)
                            })
                            .collect::<Result<Vec<_>, _>>()
                    });
                match target_records {
                    Ok(Ok(target_records)) => {
                        additional_records.extend(target_records);
                    }
                    // Don't bail out if we failed to lookup or
                    // handle the response as the original request
                    // did succeed and we only care to do this on
                    // a best-effort basis.
                    Err(error) => {
                        slog::warn!(
                            &log,
                            "SRV target lookup failed";
                            "original_mr" => #?mr,
                            "target" => ?srv.target(),
                            "error" => ?error,
                        );
                    }
                    Ok(Err(error)) => {
                        slog::warn!(
                            &log,
                            "SRV target unexpected response";
                            "original_mr" => #?mr,
                            "target" => ?srv.target(),
                            "error" => ?error,
                        );
                    }
                }
            }
            Ok(record)
        })
        .collect::<Result<Vec<_>, RequestError>>()?;
    debug!(
        &log,
        "dns response";
        "query" => ?query,
        "records" => ?&response_records,
        "additional_records" => ?&additional_records,
    );
    respond_records(request, rb, header, &response_records, &additional_records)
        .await
}

/// Respond to a DNS query with the given set of DNS records
async fn respond_records(
    request: &Request,
    rb: MessageResponseBuilder<'_>,
    header: Header,
    response_records: &[Record],
    additional_records: &[Record],
) -> Result<(), RequestError> {
    let mresp = rb.build(
        header,
        response_records.iter().collect::<Vec<&Record>>(),
        vec![],
        vec![],
        additional_records,
    );

    encode_and_send(&request, mresp, "records").await.map_err(|error| {
        RequestError::ServFail(anyhow!("failed to emit response: {:#}", error))
    })
}

/// Respond to a DNS query with an NXDOMAIN error
///
/// This means that we are authoritative for the parent domain and the requested
/// name definitely does not exist.
async fn respond_nxdomain(
    request: &Request,
    rb_nxdomain: MessageResponseBuilder<'_>,
    rb_servfail: MessageResponseBuilder<'_>,
    header: &Header,
) {
    let log = &request.log;
    let mresp = rb_nxdomain.error_msg(&header, ResponseCode::NXDomain);
    if let Err(error) = encode_and_send(request, mresp, "NXDOMAIN").await {
        error!(
            log,
            "switching to SERVFAIL after failure to encode NXDOMAIN ({:#})",
            error
        );
        respond_servfail(request, rb_servfail, header).await;
    }
}

/// Respond to a DNS query with a SERVFAIL error
///
/// This can be a catch-all for any kind of server-side failure.  We also use it
/// when we're not authoritative for a domain because this generally causes
/// clients to try another nameserver (which is usually what's wanted).
async fn respond_servfail(
    request: &Request,
    rb: MessageResponseBuilder<'_>,
    header: &Header,
) {
    let mresp = rb.error_msg(header, ResponseCode::ServFail);
    if let Err(error) = encode_and_send(request, mresp, "SERVFAIL").await {
        error!(&request.log, "failed to send SERVFAIL: {:#}", error);
    }
}

/// Encode the given message (which might describe an error or a collection of
/// records) as a reply to a request
fn encode_and_send<'a, Answers, NameServers, Soa, Additionals>(
    request: &'a Request,
    mresp: MessageResponse<'a, 'a, Answers, NameServers, Soa, Additionals>,
    label: &'static str,
) -> impl std::future::Future<Output = anyhow::Result<()>> + 'a
where
    Answers: Iterator<Item = &'a Record> + Send + 'a,
    NameServers: Iterator<Item = &'a Record> + Send + 'a,
    Soa: Iterator<Item = &'a Record> + Send + 'a,
    Additionals: Iterator<Item = &'a Record> + Send + 'a,
{
    async move {
        let mut resp_data = Vec::new();
        let mut enc = BinEncoder::new(&mut resp_data);
        let _ = mresp
            .destructive_emit(&mut enc)
            .with_context(|| format!("encoding {}", label))?;

        // If we get this far and fail to send the data, there's nothing else to
        // do.  Log the problem and treat this as a success as far as the caller
        // is concerned.
        if let Err(error) =
            request.socket.send_to(&resp_data, &request.client_addr).await
        {
            error!(
                &request.log,
                "failed to send {} to {:?}: {:#}",
                label,
                request.client_addr.to_string(),
                error
            );
        }

        Ok(())
    }
}
