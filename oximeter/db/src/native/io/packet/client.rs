// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2025 Oxide Computer Company

//! Encode client packets destined for the server.

use crate::native::Error;
use crate::native::block::Block;
use crate::native::io;
use crate::native::packets::client::ClientInfo;
use crate::native::packets::client::Hello;
use crate::native::packets::client::Packet;
use crate::native::packets::client::Query;
use crate::native::packets::client::QueryKind;
use crate::native::packets::client::Settings;
use crate::native::packets::client::Stage;
use crate::native::probes;
use bytes::BufMut as _;
use bytes::BytesMut;
use std::net::IpAddr;

/// Encoder for client packets.
#[derive(Clone, Copy, Debug)]
pub struct Encoder {
    pub addr: IpAddr,
}

impl Encoder {
    /// Encode a client hello packet.
    fn encode_hello(&self, hello: &Hello, mut dst: &mut BytesMut) {
        dst.put_u8(Packet::HELLO);
        io::string::encode(&hello.client_name, &mut dst);
        io::varuint::encode(hello.version_major, &mut dst);
        io::varuint::encode(hello.version_minor, &mut dst);
        io::varuint::encode(hello.protocol_version, &mut dst);
        io::string::encode(&hello.database, &mut dst);
        io::string::encode(&hello.username, &mut dst);
        io::string::encode(&hello.password, &mut dst);

        // NOTE: It's not described in the documentation for the protocol, but
        // recent client versions are required so send an "addendum" packet
        // here. See
        // https://github.com/ClickHouse/ClickHouse/blob/f69cb73df0763fdc81033834bdcc6920af565792/src/Client/Connection.cpp#L447
        // for the method which does that. For the protocol version we currently
        // support, this packet contains only the quota key, which is empty.
        // There is no response packet required, so we just tack in on here for
        // simplicity.
        io::string::encode("", &mut dst);
    }

    /// Encode the client query packet.
    fn encode_query(&self, query: Box<Query>, mut dst: &mut BytesMut) {
        dst.put_u8(Packet::QUERY);
        io::string::encode(query.id, &mut dst);
        self.encode_client_info(query.client_info, &mut dst);
        self.encode_settings(query.settings, &mut dst);
        io::string::encode(query.secret, &mut dst);
        let stage = match query.stage {
            Stage::FetchColumns => 0,
            Stage::WithMergeableState => 1,
            Stage::Complete => 2,
        };
        io::varuint::encode(stage, &mut dst);
        io::varuint::encode(query.compression, &mut dst);
        io::string::encode(query.body, &mut dst);

        // No parameters
        io::string::encode("", &mut dst);

        // Send an empty block to signal the end of data transfer.
        self.encode_block(Block::empty(), &mut dst).unwrap();
    }

    /// Encode a ClientInfo into the buffer.
    ///
    /// See the source here:
    /// <https://github.com/ClickHouse/ClickHouse/blob/98a2c1c638c2ff9cea36e68c9ac16b3cf142387b/src/Interpreters/ClientInfo.cpp#L24>
    /// for details on the ordering of these fields.
    fn encode_client_info(&self, info: ClientInfo, mut dst: &mut BytesMut) {
        let kind = match info.query_kind {
            QueryKind::None => 0,
            QueryKind::Initial => 1,
            QueryKind::Secondary => 2,
        };
        dst.put_u8(kind);
        io::string::encode(info.initial_user, &mut dst);
        io::string::encode(info.initial_query_id, &mut dst);
        io::string::encode(info.initial_address, &mut dst);
        dst.put_i64_le(info.initial_time);
        dst.put_u8(info.interface);
        io::string::encode(info.os_user, &mut dst);
        io::string::encode(info.client_hostname, &mut dst);
        io::string::encode(info.client_name, &mut dst);
        io::varuint::encode(info.version_major, &mut dst);
        io::varuint::encode(info.version_minor, &mut dst);
        io::varuint::encode(info.protocol_version, &mut dst);
        io::string::encode(info.quota_key, &mut dst);
        io::varuint::encode(info.distributed_depth, &mut dst);
        io::varuint::encode(info.version_patch, &mut dst);
        if let Some(state) = info.otel_state.as_ref() {
            dst.put_u8(1);
            dst.put(state.trace_id.as_slice());
            dst.put(state.span_id.as_slice());
            io::string::encode(&state.trace_state, &mut dst);
            dst.put_u8(state.trace_flags);
        } else {
            dst.put_u8(0);
        }

        // We expected to be connected to a server at PROTOCOL_VERSION, which
        // is after the revision that added these fields, so we need to include
        // them.
        //
        // "Collaborate with initiator"
        io::varuint::encode(0, &mut dst);
        // Obsolete count participating replicas
        io::varuint::encode(0, &mut dst);
        // Number of current replica
        io::varuint::encode(0, &mut dst);
    }

    fn encode_settings(&self, settings: Settings, mut dst: &mut BytesMut) {
        for (name, setting) in settings.iter() {
            io::string::encode(name, &mut dst);
            io::string::encode(&setting.value, &mut dst);
            dst.put_u8(u8::from(setting.important));
        }

        // There is no prefix for the length of the settings map. It's NULL
        // terminated, like the C of old! Note that this is _exactly one_ empty
        // string, not a key-value pair with both empty.
        io::string::encode("", &mut dst);
    }

    fn encode_block(
        &self,
        block: Block,
        dst: &mut BytesMut,
    ) -> Result<(), Error> {
        dst.put_u8(Packet::DATA);
        io::block::encode(block, dst)
    }
}

impl tokio_util::codec::Encoder<Packet> for Encoder {
    type Error = Error;

    fn encode(
        &mut self,
        item: Packet,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let kind = item.kind();
        probes::packet__send__start!(|| (self.addr.to_string(), kind, &item));
        match item {
            Packet::Hello(hello) => self.encode_hello(&hello, dst),
            Packet::Query(query) => self.encode_query(query, dst),
            Packet::Data(block) => self.encode_block(block, dst)?,
            Packet::Cancel => dst.put_u8(Packet::CANCEL),
            Packet::Ping => dst.put_u8(Packet::PING),
        };
        probes::packet__send__done!(|| self.addr.to_string());
        Ok(())
    }
}
