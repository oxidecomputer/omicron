// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::Ereport;
use crate::config::EreportConfig;
use crate::config::EreportRestart;
use gateway_messages::ereport::Ena;
use gateway_messages::ereport::EreportRequest;
use gateway_messages::ereport::EreportResponseHeader;
use gateway_messages::ereport::ResponseHeaderV0;
use gateway_messages::ereport::RestartId;
use std::collections::VecDeque;
use std::io::Cursor;
use tokio::sync::oneshot;

pub(crate) struct EreportState {
    ereports: VecDeque<(Ena, EreportList)>,
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
        let ereports: VecDeque<(Ena, EreportList)> = ereports
            .into_iter()
            .enumerate()
            .map(|(i, ereport)| (Ena(i as u64), ereport.to_list()))
            .collect();
        let restart_id = RestartId(restart_id.as_u128());
        let next_ena = Ena(ereports.len() as u64);
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
        self.restart_id = RestartId(restart_id.as_u128());
        self.meta = metadata;
        self.ereports.clear();
        self.next_ena = Ena(0);
    }

    pub(crate) fn append_ereport(&mut self, ereport: Ereport) -> Ena {
        let ena = self.next_ena;
        slog::info!(
            self.log,
            "appending new ereport";
            "ena" => ?ena,
            "ereport" => ?ereport,
        );
        self.ereports.push_back((ena, ereport.to_list()));
        self.next_ena.0 += 1;
        ena
    }

    pub(crate) fn handle_request<'buf>(
        &mut self,
        request: EreportRequest,
        buf: &'buf mut [u8],
    ) -> &'buf [u8] {
        use serde::ser::Serializer;

        let EreportRequest::V0(req) = request;
        slog::info!(self.log, "ereport request: {req:?}");

        let mut pos = gateway_messages::serialize(
            buf,
            &EreportResponseHeader::V0(ResponseHeaderV0 {
                request_id: req.request_id,
                restart_id: self.restart_id,
            }),
        )
        .expect("header must serialize");

        // If we "restarted", encode the current metadata map, and start at ENA
        // 0.
        let (meta_map, start_ena) = if req.restart_id != self.restart_id {
            slog::info!(
                self.log,
                "requested restart ID is not current, pretending to have \
                 restarted...";
                "req_restart_id" => ?req.restart_id,
                "current_restart_id" => ?self.restart_id,
            );
            (&self.meta, Ena(0))
        } else {
            // If we didn't "restart", we should honor the committed ENA (if the
            // request includes one), and we should start at the requested ENA.
            if let Some(committed_ena) = req.committed_ena() {
                slog::debug!(
                    self.log,
                    "MGS committed ereports up to {committed_ena:?}"
                );
                let mut discarded = 0;
                while self
                    .ereports
                    .front()
                    .map(|(ena, _)| ena <= &committed_ena)
                    .unwrap_or(false)
                {
                    self.ereports.pop_front();
                    discarded += 1;
                }

                slog::info!(
                    self.log,
                    "discarded {discarded} ereports up to {committed_ena:?}"
                );
            }

            (&Default::default(), req.start_ena)
        };
        pos += {
            use serde::ser::SerializeMap;

            let mut cursor = Cursor::new(&mut buf[pos..]);
            // Rather than just using `serde_cbor::to_writer`, we'll manually
            // construct a `Serializer`, so that we can call the `serialize_map`
            // method *without* a length to force it to use the "indefinite-length"
            // encoding.
            let mut serializer = serde_cbor::Serializer::new(
                serde_cbor::ser::IoWrite::new(&mut cursor),
            );
            let mut map =
                serializer.serialize_map(None).expect("map should start");
            for (key, value) in meta_map {
                map.serialize_entry(key, value)
                    .expect("element should serialize");
            }
            map.end().expect("map should end");
            cursor.position() as usize
        };

        // Is there enough remaining space for ereports? We need at least 10
        // bytes (8 for the ENA, and at least two bytes to encode an empty CBOR
        // list)
        if buf[pos..].len() < 10 {
            return &buf[..pos];
        }

        let mut respondant_ereports = self
            .ereports
            .iter()
            .filter(|(ena, _)| ena.0 >= start_ena.0)
            .take(req.limit as usize);
        if let Some((ena, ereport)) = respondant_ereports.next() {
            pos += gateway_messages::serialize(&mut buf[pos..], ena)
                .expect("serialing ena shouldn't fail");
            buf[pos] = 0x9f; // start list
            pos += 1;

            // Okay, this is a little bit goofy: we're going to encode each
            // message to a separate Vec before writing it to the packet buffer.
            // This is because we need to know if the *whole* ereport fits in
            // the packet before deciding whether or not to include it.
            //
            // We could certainly be a bit more efficient here by reusing a
            // dedicated encoding buffer for this, but honestly? This is the SP
            // simulator, so I don't care enough.
            let bytes = serde_cbor::to_vec(ereport).unwrap();
            buf[pos..pos + bytes.len()].copy_from_slice(&bytes[..]);
            pos += bytes.len();
            slog::debug!(
                self.log,
                "wrote initial ereport: {ereport:#?}";
                "ena" => ?ena,
                "packet_bytes" => pos,
                "ereport_bytes" => bytes.len(),
            );

            // try to fill the rest of the packet
            for (_, ereport) in respondant_ereports {
                let bytes = serde_cbor::to_vec(ereport).unwrap();
                // packet full!
                if buf[pos..].len() < (bytes.len() + 1) {
                    break;
                }

                buf[pos..pos + bytes.len()].copy_from_slice(&bytes[..]);
                pos += bytes.len();
                slog::debug!(
                    self.log,
                    "wrote subsequent ereport: {ereport:#?}";
                    "packet_bytes" => pos,
                    "ereport_bytes" => bytes.len(),
                );
            }

            buf[pos] = 0xff; // break list;
            pos += 1;
        }

        &buf[..pos]
    }
}

type EreportList = Vec<toml::Value>;

impl Ereport {
    fn to_list(self) -> EreportList {
        let Ereport { task_name, task_gen, uptime, data } = self;
        vec![
            task_name.into(),
            task_gen.into(),
            (uptime as i64).into(),
            data.into(),
        ]
    }
}

// fn toml2cbor(toml: &toml::Value) -> serde_cbor::Value {
//     match *toml {
//         toml::Value::String(ref s) => serde_cbor::Value::Text(s.clone()),
//         toml::Value::Integer(i) => serde_cbor::Value::Integer(i as i128),
//         toml::Value::Float(f) => serde_cbor::Value::Float(f),
//         toml::Value::Boolean(b) => serde_cbor::Value::Bool(b),
//         toml::Value::Datetime(d) => unimplemented!("don't use toml datetimes"),
//         toml::Value::Array(ref a) => {
//             serde_cbor::Value::Array(a.iter().map(toml2cbor).collect())
//         }
//         toml::Value::Table(ref t) => serde_cbor::Value::Map(toml2cbor_table::<serde_cbor::Value>(toml))
//     }
// }

// fn toml2cbor_table<K: From<String>>(toml: &toml::Table) -> BTreeMap<K, serde_cbor::Value> {
//     toml.iter().map(|(k, v) {
//         (k.clone().into(), toml2cbor(v))
//     }).collect()
// }
