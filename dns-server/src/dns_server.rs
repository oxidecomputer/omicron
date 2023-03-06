// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::io::Result;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use crate::dns_types::DnsRecord;
use pretty_hex::*;
use serde::Deserialize;
use slog::{debug, error, Logger};
use tokio::net::UdpSocket;
use trust_dns_client::rr::LowerName;
use trust_dns_proto::op::header::Header;
use trust_dns_proto::op::response_code::ResponseCode;
use trust_dns_proto::rr::rdata::SRV;
use trust_dns_proto::rr::record_data::RData;
use trust_dns_proto::rr::record_type::RecordType;
use trust_dns_proto::rr::{Name, Record};
use trust_dns_proto::serialize::binary::{
    BinDecodable, BinDecoder, BinEncoder,
};
use trust_dns_server::authority::{MessageRequest, MessageResponseBuilder};

/// Configuration related to the DNS server
#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// The address to listen for DNS requests on
    pub bind_address: String,
}

pub struct Server {
    pub address: SocketAddr,
    pub handle: tokio::task::JoinHandle<Result<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.handle.abort()
    }
}

pub async fn run(
    log: Logger,
    db: Arc<sled::Db>,
    config: Config,
) -> Result<Server> {
    let socket = Arc::new(UdpSocket::bind(config.bind_address).await?);
    let address = socket.local_addr()?;

    let handle = tokio::task::spawn(async move {
        loop {
            let mut buf = vec![0u8; 16384];
            let (n, src) = socket.recv_from(&mut buf).await?;
            buf.resize(n, 0);

            let socket = socket.clone();
            let log = log.clone();
            let db = db.clone();

            tokio::spawn(
                async move { handle_req(log, db, socket, src, buf).await },
            );
        }
    });

    Ok(Server { address, handle })
}

async fn respond_nxdomain(
    log: &Logger,
    socket: Arc<UdpSocket>,
    src: SocketAddr,
    rb: MessageResponseBuilder<'_>,
    header: Header,
    mr: &MessageRequest,
) {
    let mresp = rb.error_msg(&header, ResponseCode::NXDomain);
    let mut resp_data = Vec::new();
    let mut enc = BinEncoder::new(&mut resp_data);
    match mresp.destructive_emit(&mut enc) {
        Ok(_) => {}
        Err(e) => {
            error!(log, "NXDOMAIN destructive emit: {}", e);
            nack(&log, &mr, &socket, &header, &src).await;
            return;
        }
    }
    match socket.send_to(&resp_data, &src).await {
        Ok(_) => {}
        Err(e) => {
            error!(log, "NXDOMAIN send: {}", e);
        }
    }
}

async fn handle_req(
    log: Logger,
    db: Arc<sled::Db>,
    socket: Arc<UdpSocket>,
    src: SocketAddr,
    buf: Vec<u8>,
) {
    debug!(&log, "handle_req: buffer"; "buffer" => ?buf.hex_dump());

    let mut dec = BinDecoder::new(&buf);
    let mr = match MessageRequest::read(&mut dec) {
        Ok(mr) => mr,
        Err(e) => {
            error!(log, "read message: {}", e);
            return;
        }
    };

    debug!(&log, "handle_req: message_request"; "mr" => #?mr);

    let header = Header::response_from_request(mr.header());

    // Ensure the query is for this zone, otherwise bail with servfail. This
    // will cause resolvers to look to other DNS servers for this query.
    // XXX-dap
//    let name = mr.query().name();
//    if !zone.zone_of(name) {
//        nack(&log, &mr, &socket, &header, &src).await;
//        return;
//    }

    let name = mr.query().original().name().clone();
    let key = name.to_string();
    let key = key.trim_end_matches('.');

    let rb = MessageResponseBuilder::from_message_request(&mr);

    let bits = match db.get(key.as_bytes()) {
        Ok(Some(bits)) => bits,

        // If no record is found bail with NXDOMAIN.
        Ok(None) => {
            respond_nxdomain(&log, socket, src, rb, header, &mr).await;
            return;
        }

        // If we encountered an error bail with SERVFAIL.
        Err(e) => {
            error!(log, "db get: {}", e);
            nack(&log, &mr, &socket, &header, &src).await;
            return;
        }
    };

    let records: Vec<crate::dns_types::DnsRecord> =
        match serde_json::from_slice(bits.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                error!(log, "deserialize record: {}", e);
                nack(&log, &mr, &socket, &header, &src).await;
                return;
            }
        };

    if records.is_empty() {
        error!(log, "No records found for {}", key);
        respond_nxdomain(&log, socket, src, rb, header, &mr).await;
        return;
    }

    let mut response_records: Vec<Record> = vec![];
    for record in &records {
        let resp = match record {
            DnsRecord::AAAA(addr) => {
                let mut aaaa = Record::new();
                aaaa.set_name(name.clone())
                    .set_rr_type(RecordType::AAAA)
                    .set_data(Some(RData::AAAA(*addr)));
                aaaa
            }
            DnsRecord::SRV(crate::dns_types::SRV {
                prio,
                weight,
                port,
                target,
            }) => {
                let mut srv = Record::new();
                let tgt = match Name::from_str(&target) {
                    Ok(tgt) => tgt,
                    Err(e) => {
                        error!(log, "srv target: '{}' {}", target, e);
                        nack(&log, &mr, &socket, &header, &src).await;
                        return;
                    }
                };
                srv.set_name(name.clone())
                    .set_rr_type(RecordType::SRV)
                    .set_data(Some(RData::SRV(SRV::new(
                        *prio, *weight, *port, tgt,
                    ))));
                srv
            }
        };
        response_records.push(resp);
    }

    let mresp = rb.build(
        header,
        response_records.iter().collect::<Vec<&Record>>(),
        vec![],
        vec![],
        vec![],
    );

    let mut resp_data = Vec::new();
    let mut enc = BinEncoder::new(&mut resp_data);
    match mresp.destructive_emit(&mut enc) {
        Ok(_) => {}
        Err(e) => {
            error!(log, "destructive emit: {}", e);
            nack(&log, &mr, &socket, &header, &src).await;
            return;
        }
    }
    match socket.send_to(&resp_data, &src).await {
        Ok(_) => {}
        Err(e) => {
            error!(log, "send: {}", e);
        }
    }
}

async fn nack(
    log: &Logger,
    mr: &MessageRequest,
    socket: &UdpSocket,
    header: &Header,
    src: &SocketAddr,
) {
    let rb = MessageResponseBuilder::from_message_request(mr);
    let mresp = rb.error_msg(header, ResponseCode::ServFail);
    let mut resp_data = Vec::new();
    let mut enc = BinEncoder::new(&mut resp_data);
    match mresp.destructive_emit(&mut enc) {
        Ok(_) => {}
        Err(e) => {
            error!(log, "destructive emit: {}", e);
            return;
        }
    }
    match socket.send_to(&resp_data, &src).await {
        Ok(_) => {}
        Err(e) => {
            error!(log, "destructive emit: {}", e);
        }
    }
}
