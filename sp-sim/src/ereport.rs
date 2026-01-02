// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//

use crate::config::Ereport;
use crate::config::EreportConfig;
use crate::config::EreportRestart;
use crate::server::UdpServer;
use gateway_ereport_messages::Ena;
use gateway_ereport_messages::Request;
use gateway_ereport_messages::ResponseHeader;
use gateway_ereport_messages::ResponseHeaderV0;
use gateway_ereport_messages::RestartId;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::io::Cursor;
use std::net::SocketAddrV6;
use tokio::sync::oneshot;
use zerocopy::IntoBytes;

pub(crate) struct EreportState {
    ereports: VecDeque<EreportListEntry>,
    meta: toml::map::Map<String, toml::Value>,
    /// Next ENA, used for appending new ENAs at runtime.
    next_ena: Ena,
    restart_id: RestartId,
    log: slog::Logger,
}

#[derive(Debug)]
pub(crate) enum Command {
    Restart(EreportRestart, oneshot::Sender<()>),
    Append(Ereport, oneshot::Sender<Ena>),
}

struct EreportListEntry {
    ena: Ena,
    ereport: Ereport,
    bytes: Vec<u8>,
}

pub(crate) async fn recv_request(
    sock: Option<&mut UdpServer>,
) -> anyhow::Result<(Request, SocketAddrV6, &mut UdpServer)> {
    use zerocopy::TryFromBytes;
    match sock {
        Some(sock) => {
            let (packet, addr) = sock.recv_from().await?;
            let req = Request::try_read_from_bytes(packet).map_err(|e| {
                anyhow::anyhow!(
                    "couldn't understand ereport header from {addr}: {e:?}"
                )
            })?;
            Ok((req, addr, sock))
        }
        None => futures::future::pending().await,
    }
}

impl EreportState {
    pub(crate) fn new(
        EreportConfig { restart, ereports }: EreportConfig,
        log: slog::Logger,
    ) -> Self {
        let EreportRestart { metadata, restart_id } = restart;
        slog::info!(
            log,
            "configuring sim ereports";
            "restart_id" => ?restart_id,
            "n_ereports" => ereports.len(),
            "metadata" => ?metadata,
        );

        let ereports: VecDeque<_> =
            // The ereport queue always begins with an initial loss record. This
            // is used to indicate that the SP has restarted, and any ereports
            // that were previously buffered but not ingested *may* have been
            // lost.
            std::iter::once(Ereport::loss_internal(None))
                .chain(ereports)
                .enumerate()
                .map(|(i, ereport)| {
                    // DON'T EMIT ENA 0
                    let ena = Ena::new(i as u64 + 1);
                    ereport.to_entry(ena)
                })
                .collect();
        let restart_id = RestartId::new(restart_id.as_u128());
        let next_ena = Ena::new(ereports.len() as u64);
        Self { ereports, next_ena, restart_id, meta: metadata, log }
    }

    pub(crate) fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::Append(ereport, tx) => {
                let ena = self.append_ereport(ereport);
                tx.send(ena).map_err(|_| "receiving half died").unwrap();
            }
            Command::Restart(restart, tx) => {
                self.pretend_to_restart(restart);
                tx.send(()).map_err(|_| "receiving half died").unwrap();
            }
        }
    }

    pub(crate) fn pretend_to_restart(
        &mut self,
        EreportRestart { metadata, restart_id }: EreportRestart,
    ) {
        slog::info!(
            self.log,
            "simulating restart";
            "curr_restart_id" => ?self.restart_id,
            "next_restart_id" => ?restart_id,
            "metadata" => ?metadata,
        );
        self.restart_id = RestartId::new(restart_id.as_u128());
        self.meta = metadata;
        self.ereports.clear();
        // Initial loss record. This is used to indicate that the SP has
        // restarted, and any ereports that were previously buffered but not
        // ingested *may* have been lost.
        self.ereports
            .push_back(Ereport::loss_internal(None).to_entry(Ena::new(1)));
        self.next_ena = Ena::new(2);
    }

    pub(crate) fn append_ereport(&mut self, ereport: Ereport) -> Ena {
        let ena = self.next_ena;
        slog::info!(
            self.log,
            "appending new ereport";
            "ena" => ?ena,
            "ereport" => ?ereport,
        );
        self.ereports.push_back(ereport.to_entry(ena));
        self.next_ena.0 += 1;
        ena
    }

    pub(crate) fn handle_request<'buf>(
        &mut self,
        request: &Request,
        addr: SocketAddrV6,
        buf: &'buf mut [u8],
    ) -> &'buf [u8] {
        use serde::ser::Serializer;

        let Request::V0(req) = request;
        slog::info!(
            self.log,
            "received ereport request";
            "src_addr" => %addr,
            "req_id" => ?req.request_id,
            "req" => ?req,
        );

        // If we "restarted", encode the current metadata map, and start at ENA
        // 1.
        let (meta_map, start_ena) = if req.restart_id != self.restart_id {
            slog::info!(
                self.log,
                "requested restart ID is not current, pretending to have \
                 restarted...";
                "src_addr" => %addr,
                "req_id" => ?req.request_id,
                "req_restart_id" => ?req.restart_id,
                "current_restart_id" => ?self.restart_id,
            );
            (Some(&self.meta), Ena::NONE)
        } else {
            // If we didn't "restart", we should honor the committed ENA (if the
            // request includes one), and we should start at the requested ENA.
            if let Some(committed_ena) = req.committed_ena() {
                slog::debug!(
                    self.log,
                    "MGS committed ereports up to {committed_ena:?}";
                    "src_addr" => %addr,
                    "req_id" => ?req.request_id,
                    "committed_ena" => ?committed_ena,
                );
                let mut discarded = 0;
                while self
                    .ereports
                    .front()
                    .map(|ereport| &ereport.ena <= committed_ena)
                    .unwrap_or(false)
                {
                    self.ereports.pop_front();
                    discarded += 1;
                }

                slog::info!(
                    self.log,
                    "discarded {discarded} ereports up to {committed_ena:?}";
                    "src_addr" => %addr,
                    "req_id" => ?req.request_id,
                    "committed_ena" => ?committed_ena,
                );
            }

            (None, req.start_ena)
        };

        let mut respondant_ereports = self
            .ereports
            .iter()
            .filter(|ereport| ereport.ena >= start_ena)
            .take(req.limit as usize)
            .peekable();
        let start_ena = respondant_ereports
            .peek()
            .map(|ereport| ereport.ena)
            // If there are no ereports, send ENA zero (which means "no ereports").
            .unwrap_or(Ena::NONE);

        // Serialize the header.
        ResponseHeader::V0(ResponseHeaderV0 {
            request_id: req.request_id,
            restart_id: self.restart_id,
            start_ena,
        })
        .write_to_prefix(buf)
        .expect("should successfully write header");
        let mut pos = std::mem::size_of::<ResponseHeader>();

        // Serialize the metadata map.
        use serde::ser::SerializeMap;

        let mut cursor = Cursor::new(&mut buf[pos..]);
        // Rather than just using `serde_cbor::to_writer`, we'll manually
        // construct a `Serializer`, so that we can call the `serialize_map`
        // method *without* a length to force it to use the "indefinite-length"
        // encoding.
        let mut serializer = serde_cbor::Serializer::new(
            serde_cbor::ser::IoWrite::new(&mut cursor),
        );
        let mut map = serializer.serialize_map(None).expect("map should start");

        for (key, value) in
            meta_map.into_iter().flat_map(IntoIterator::into_iter)
        {
            map.serialize_entry(key, value).expect("element should serialize");
        }

        map.end().expect("map should end");

        let meta_bytes = cursor.position() as usize;
        slog::debug!(
            self.log,
            "wrote metadata map";
            "src_addr" => %addr,
            "req_id" => ?req.request_id,
            "entries" => meta_map.map(|map| map.len()).unwrap_or(0),
            "bytes" => meta_bytes,
        );
        pos += meta_bytes;

        // Is there enough remaining space for ereports? We need at least two
        // bytes to encode an empty CBOR list
        if buf[pos..].len() <= 2 {
            return &buf[..pos];
        }

        let ereport_start_pos = pos;
        buf[pos] = 0x9f; // start list
        pos += 1;

        // try to fill the rest of the packet with ereports
        let mut encoded_ereports = 0;
        for EreportListEntry { ena, ereport, bytes } in respondant_ereports {
            // packet full!
            if buf[pos..].len() < (bytes.len() + 1) {
                break;
            }

            buf[pos..pos + bytes.len()].copy_from_slice(&bytes[..]);
            pos += bytes.len();
            slog::debug!(
                self.log,
                "wrote ereport: {ereport:#?}";
                "src_addr" => %addr,
                "req_id" => ?req.request_id,
                "ena" => ?ena,
                "packet_bytes" => pos,
                "ereport_bytes" => bytes.len(),
            );
            encoded_ereports += 1;
        }

        buf[pos] = 0xff; // break list;
        pos += 1;

        slog::info!(
            self.log,
            "encoded ereport packet";
            "src_addr" => %addr,
            "req_id" => ?req.request_id,
            "req_start_ena" => ?req.start_ena,
            "start_ena" => ?start_ena,
            "meta_entries" => meta_map.map(|m| m.len()).unwrap_or(0),
            "ereports" => ?encoded_ereports,
            "packet_bytes" => pos,
            "meta_bytes" => meta_bytes,
            "ereport_bytes" => pos - ereport_start_pos,
        );

        &buf[..pos]
    }
}

impl Ereport {
    fn to_entry(self, ena: Ena) -> EreportListEntry {
        let &Ereport { uptime, task_gen, ref task_name, ref data } = &self;
        let body_bytes = match serde_cbor::to_vec(data) {
            Ok(bytes) => bytes,
            Err(e) => {
                panic!("Failed to serialize ereport body: {e}\ndata: {data:#?}",)
            }
        };
        let bytes = match serde_cbor::to_vec(&(
            task_name,
            task_gen,
            uptime,
            // force byte array serialization --- serde will by default turn a
            // `Vec<u8>` into a sequence of integers, which is ghastly (and not
            // what MGS expects...)
            serde_cbor::Value::Bytes(body_bytes),
        )) {
            Ok(bytes) => bytes,
            Err(e) => panic!(
                "Failed to serialize ereport tuple: {e}\nereport: {self:#?}",
            ),
        };
        EreportListEntry { ena, ereport: self, bytes }
    }

    /// Returns a new loss report representing a (simulated) loss of `lost`
    /// ereports.
    pub fn new_loss(lost: u32) -> Self {
        Self::loss_internal(Some(lost))
    }

    fn loss_internal(amount: Option<u32>) -> Self {
        let mut data = BTreeMap::new();
        match amount {
            Some(amount) => data.insert(
                "lost".to_string(),
                serde_cbor::Value::Integer(i128::from(amount)),
            ),
            None => data.insert("lost".to_string(), serde_cbor::Value::Null),
        };
        Self {
            task_name: "packrat".to_string(),
            // Packrat shouldn't restart --- if we were asked to simulate a
            // restart, it's the *whole SP* that has restarted.
            task_gen: 0,
            // TODO(eliza): it would be nice to have a simulated Hubris uptime
            // that's always advanced as new ereports are inserted, but
            // currently, the tests don't care about this, so whatever.
            uptime: 666,
            data,
        }
    }
}
