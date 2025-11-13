// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Omicron-specific implementation of hickory-server's Authority trait
//!
//! This module implements the `Authority` trait to integrate our existing
//! Store-based DNS data with hickory-server's high-level server infrastructure.

use crate::storage::{QueryError, Store};
use anyhow::anyhow;
use async_trait::async_trait;
use hickory_proto::op::ResponseCode;
use hickory_proto::rr::rdata::{NS, SRV};
use hickory_proto::rr::{LowerName, Name, RData, Record, RecordType};
use hickory_server::authority::{
    Authority, LookupControlFlow, LookupError, LookupObject, LookupOptions,
    MessageRequest, ZoneType,
};
use hickory_server::server::RequestInfo;
use internal_dns_types::config::{DnsRecord, Srv};
use slog::{Logger, debug, error, warn};
use slog_error_chain::InlineErrorChain;
use std::io;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
enum AuthorityLookupError {
    #[error("error querying store")]
    QueryError(#[from] QueryError),
    #[error("error converting record")]
    ConversionError(#[from] anyhow::Error),
    #[error("NXDOMAIN")]
    Nxdomain,
}

/// Omicron's Authority implementation that wraps our Store
///
/// This authority delegates all DNS queries to the Store, which already
/// handles zone routing and record lookups. The Authority trait integration
/// allows us to use hickory-server's ServerFuture with its automatic
/// eDNS support, TCP handling, and truncation.
pub struct OmicronAuthority {
    store: Store,
    origin: LowerName,
    log: Logger,
}

impl OmicronAuthority {
    /// Create a new authority for the given zone
    pub fn new(store: Store, origin: Name, log: Logger) -> Self {
        Self { store, origin: origin.into(), log }
    }

    fn lookup_impl(
        &self,
        name: &LowerName,
        rtype: RecordType,
    ) -> Result<LookupControlFlow<OmicronLookup>, AuthorityLookupError> {
        let log = &self.log;
        let store = &self.store;

        // Query the underlying store.
        let answer = store.query_name(&name)?;

        // Convert our internal representation to the one we'll return back to
        // hickory-dns.
        let mut name_records = answer
            .records
            .as_ref()
            .unwrap_or(&Vec::new())
            .iter()
            .map(|record| dns_record_to_record(&name, record))
            .collect::<Result<Vec<_>, _>>()?;
        let mut additional_records = vec![];

        if answer.name.is_none() && rtype == RecordType::SOA {
            // The query was for an SOA record at the apex. There isn't an SOA
            // record in the database, but we can build one from the answer.
            name_records.push(store.soa_for(&answer)?);
        }

        // If we got no records at this point, then the name does not exist.
        // Return NXDOMAIN for this case rather than no records at all.
        if name_records.is_empty() {
            debug!(
                &log,
                "authority lookup result: nxdomain";
                "name" => ?name,
                "rtype" => ?rtype,
                "origin" => ?self.origin,
            );
            return Err(AuthorityLookupError::Nxdomain);
        }

        // Filter for just the record types that were requested.
        let response_records = name_records
            .into_iter()
            .filter(|record| match (rtype, record.data()) {
                (RecordType::ANY, _) => true,
                (RecordType::A, RData::A(_)) => true,
                (RecordType::AAAA, RData::AAAA(_)) => true,
                (RecordType::SRV, RData::SRV(_)) => true,
                (RecordType::NS, RData::NS(_)) => true,
                (RecordType::SOA, RData::SOA(_)) => true,
                _ => false,
            })
            .collect::<Vec<_>>();

        // DNS allows for the server to return additional records that weren't
        // explicitly asked for by the client but that the server expects the
        // client will want. SRV and NS records both use names for their
        // referents (rather than IP addresses directly). If someone has queried
        // for one of those kinds of records, they'll almost certainly be
        // needing the IP addresses that go with them as well. We
        // opportunistically attempt to resolve the target here and if
        // successful return those additional records in the response.
        //
        // NOTE: we only do this one-layer deep. If the target of a SRV or
        // NS is a CNAME instead of A/AAAA directly, it will be lost here.
        for record in &response_records {
            let target = match record.data() {
                RData::SRV(srv) => srv.target(),
                RData::NS(ns) => &ns.0,
                _ => {
                    continue;
                }
            };

            let target_records = store.query_name(target).map(|answer| {
                answer
                    .records
                    .unwrap_or(Vec::new())
                    .into_iter()
                    .map(|record| dns_record_to_record(target, &record))
                    .collect::<Result<Vec<_>, _>>()
            });

            match target_records {
                Ok(Ok(target_records)) => {
                    additional_records.extend(target_records);
                }
                // Don't bail out if we failed to lookup or handle the
                // response as the original request did succeed and we only
                // care to do this on a best-effort basis.
                Err(error) => {
                    let error = InlineErrorChain::new(&error);
                    warn!(
                        &log,
                        "additional records lookup failed";
                        "target" => ?target,
                        error,
                    );
                }
                Ok(Err(error)) => {
                    let error = InlineErrorChain::new(&*error);
                    warn!(
                        &log,
                        "additional records unexpected response";
                        "target" => ?target,
                        error,
                    );
                }
            }
        }

        debug!(
            &log,
            "authority lookup result";
            "name" => ?name,
            "rtype" => ?rtype,
            "origin" => ?self.origin,
            "records" => ?&response_records,
            "additional_records" => ?&additional_records,
        );

        Ok(LookupControlFlow::Break(Ok(OmicronLookup::with_additionals(
            response_records,
            additional_records,
        ))))
    }
}

#[async_trait]
impl Authority for OmicronAuthority {
    type Lookup = OmicronLookup;

    fn zone_type(&self) -> ZoneType {
        ZoneType::Primary
    }

    fn is_axfr_allowed(&self) -> bool {
        // Zone transfers are not (yet) supported.
        false
    }

    async fn update(
        &self,
        _update: &MessageRequest,
    ) -> Result<bool, ResponseCode> {
        // We do not support updates via DNS (our consumers).
        Err(ResponseCode::NotImp)
    }

    fn origin(&self) -> &LowerName {
        &self.origin
    }

    async fn lookup(
        &self,
        query_name: &LowerName,
        rtype: RecordType,
        _lookup_options: LookupOptions,
    ) -> LookupControlFlow<Self::Lookup> {
        debug!(
            &self.log,
            "authority lookup";
            "name" => ?query_name,
            "rtype" => ?rtype,
            "origin" => ?self.origin,
        );

        match self.lookup_impl(query_name, rtype) {
            Ok(rv) => rv,
            Err(AuthorityLookupError::Nxdomain) => LookupControlFlow::Break(
                Err(LookupError::ResponseCode(ResponseCode::NXDomain)),
            ),
            Err(err) => {
                let error = InlineErrorChain::new(&err);
                error!(&self.log,
                    "authority lookup failed";
                    "name" => ?query_name,
                    "rtype" => ?rtype,
                    "origin" => ?self.origin,
                    &error,
                );

                LookupControlFlow::Break(Err(LookupError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("internal error: {}", error),
                ))))
            }
        }
    }

    async fn search(
        &self,
        request: RequestInfo<'_>,
        lookup_options: LookupOptions,
    ) -> LookupControlFlow<Self::Lookup> {
        // Delegate to lookup.  These functions appear to be roughly equivalent.
        // See https://github.com/hickory-dns/hickory-dns/issues/1309.
        self.lookup(
            request.query.name(),
            request.query.query_type(),
            lookup_options,
        )
        .await
    }

    async fn get_nsec_records(
        &self,
        _name: &LowerName,
        _lookup_options: LookupOptions,
    ) -> LookupControlFlow<Self::Lookup> {
        // DNSSEC not supported.
        LookupControlFlow::Break(Ok(OmicronLookup::empty()))
    }
}

/// The Lookup type for OmicronAuthority
///
/// This wraps our DNS records in a type that implements the necessary
/// traits for hickory-server's Authority trait.
pub struct OmicronLookup {
    records: Vec<Record>,
    additionals: Option<Vec<Record>>,
}

impl OmicronLookup {
    fn new(records: Vec<Record>) -> Self {
        Self { records, additionals: None }
    }

    fn with_additionals(
        records: Vec<Record>,
        additionals: Vec<Record>,
    ) -> Self {
        Self {
            records,
            additionals: if additionals.is_empty() {
                None
            } else {
                Some(additionals)
            },
        }
    }

    fn empty() -> Self {
        Self { records: Vec::new(), additionals: None }
    }
}

// Implement LookupObject so OmicronAuthority can be used as AuthorityObject
impl LookupObject for OmicronLookup {
    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Record> + Send + 'a> {
        Box::new(self.records.iter())
    }

    fn take_additionals(&mut self) -> Option<Box<dyn LookupObject>> {
        // Return additional records if we have any
        self.additionals.take().map(|additionals| {
            Box::new(OmicronLookup::new(additionals)) as Box<dyn LookupObject>
        })
    }
}

/// Convert a DnsRecord to a hickory Record
fn dns_record_to_record(
    name: &Name,
    record: &DnsRecord,
) -> Result<Record, anyhow::Error> {
    match record {
        DnsRecord::A(addr) => {
            Ok(Record::from_rdata(name.clone(), 0, RData::A((*addr).into())))
        }

        DnsRecord::Aaaa(addr) => {
            Ok(Record::from_rdata(name.clone(), 0, RData::AAAA((*addr).into())))
        }

        DnsRecord::Srv(Srv { prio, weight, port, target }) => {
            let tgt = Name::from_str(&target).map_err(|error| {
                anyhow!(error).context(format!("bad SRV target {:?}", target))
            })?;
            Ok(Record::from_rdata(
                name.clone(),
                0,
                RData::SRV(SRV::new(*prio, *weight, *port, tgt)),
            ))
        }

        DnsRecord::Ns(nsdname) => {
            let nsdname = Name::from_str(&nsdname).map_err(|error| {
                anyhow!(error).context(format!("bad NS dname {:?}", nsdname))
            })?;
            Ok(Record::from_rdata(name.clone(), 0, RData::NS(NS(nsdname))))
        }
    }
}
