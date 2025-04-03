// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::Ereport;
use crate::config::EreportRestart;
use gateway_messages::EreportHeader;
use gateway_messages::EreportHeaderV0;
use gateway_messages::EreportRequest;
use gateway_messages::EreportRequestFlags;
use gateway_messages::ereport::Ena;
use gateway_messages::ereport::RestartId;
use std::collections::VecDeque;
use std::io::Cursor;

pub(crate) struct EreportState {
    restarts: VecDeque<EreportRestart>,
    current_restart: Option<RestartState>,
    log: slog::Logger,
}

impl EreportState {
    pub(crate) fn new(
        mut restarts: VecDeque<EreportRestart>,
        log: slog::Logger,
    ) -> Self {
        let current_restart = restarts.pop_front().map(RestartState::from);
        Self { restarts, current_restart, log }
    }

    pub(crate) fn pretend_to_restart(&mut self) {
        self.current_restart =
            self.restarts.pop_front().map(RestartState::from);
    }

    pub(crate) fn handle_request<'buf>(
        &mut self,
        request: EreportRequest,
        buf: &'buf mut [u8],
    ) -> &'buf [u8] {
        let EreportRequest::V0(req) = request;
        slog::info!(self.log, "ereport request: {req:?}");

        let current_restart = match self.current_restart.as_mut() {
            None => {
                let amt = gateway_messages::serialize(
                    buf,
                    &EreportHeader::V0(EreportHeaderV0::new_empty(
                        req.restart_id,
                    )),
                )
                .expect("serialization shouldn't fail");
                return &buf[..amt];
            }
            Some(c) => c,
        };

        if req.restart_id != current_restart.restart_id {
            slog::info!(
                self.log,
                "requested restart ID is not current, pretending to have \
                 restarted...";
                "req_restart_id" => ?req.restart_id,
                "current_restart_id" => ?current_restart.restart_id,
            );
            let amt = gateway_messages::serialize(
                buf,
                &EreportHeader::V0(EreportHeaderV0::new_restarted(
                    current_restart.restart_id,
                )),
            )
            .expect("serialization shouldn't fail");
            let amt = {
                let mut cursor = Cursor::new(&mut buf[amt..]);
                serde_cbor::to_writer(&mut cursor, &current_restart.meta)
                    .expect("serializing metadata should fit in a packet...");
                amt + cursor.position() as usize
            };
            return &buf[..amt];
        }

        if req.flags.contains(EreportRequestFlags::COMMIT) {
            let committed = req.committed_ena;
            let mut discarded = 0;
            while current_restart
                .ereports
                .front()
                .map(|(ena, _)| ena.0 <= committed.0)
                .unwrap_or(false)
            {
                current_restart.ereports.pop_front();
                discarded += 1;
            }

            slog::info!(
                self.log,
                "discarded {discarded} ereports up to {committed:?}"
            );
        }

        let mut respondant_ereports = current_restart
            .ereports
            .iter()
            .filter(|(ena, _)| ena.0 >= req.start_ena.0);
        let end = if let Some((ena, ereport)) = respondant_ereports.next() {
            let mut pos = gateway_messages::serialize(
                buf,
                &EreportHeader::V0(EreportHeaderV0::new_data(
                    current_restart.restart_id,
                )),
            )
            .expect("serialization shouldn't fail");
            pos += gateway_messages::serialize(&mut buf[pos..], ena)
                .expect("serialing ena shouldn't fail");
            let bytes = serde_cbor::to_vec(ereport).unwrap();
            buf[pos..pos + bytes.len()].copy_from_slice(&bytes[..]);
            pos += bytes.len();
            slog::debug!(
                self.log,
                "wrote initial ereport: {ereport:#?}";
                "ena" => ?ena,
                "pos" => pos,
                "bytes" => bytes.len(),
            );

            // try to fill the rest of the packet
            for (_, ereport) in respondant_ereports {
                let bytes = serde_cbor::to_vec(ereport).unwrap();
                // packet full!
                if buf[pos..].len() < bytes.len() {
                    break;
                }

                buf[pos..pos + bytes.len()].copy_from_slice(&bytes[..]);
                pos += bytes.len();
                slog::debug!(
                    self.log,
                    "wrote subsequent ereport: {ereport:#?}";
                    "pos" => pos,
                    "bytes" => bytes.len(),
                );
            }
            pos
        } else {
            gateway_messages::serialize(
                buf,
                &EreportHeader::V0(EreportHeaderV0::new_empty(req.restart_id)),
            )
            .expect("serialization shouldn't fail")
        };

        &buf[..end]
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

struct RestartState {
    ereports: VecDeque<(Ena, EreportList)>,
    meta: toml::map::Map<String, toml::Value>,
    restart_id: RestartId,
}

impl From<EreportRestart> for RestartState {
    fn from(restart: EreportRestart) -> Self {
        let EreportRestart { id, metadata, ereports } = restart;
        let ereports = ereports
            .into_iter()
            .enumerate()
            .map(|(ena, ereport)| (Ena(ena as u64), ereport.to_list()))
            .collect();
        Self { ereports, meta: metadata, restart_id: RestartId(id.as_u128()) }
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
