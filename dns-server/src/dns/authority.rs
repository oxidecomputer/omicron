// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Omicron-specific implementation of hickory-server's Authority trait
//!
//! This module implements the `Authority` trait to integrate our existing
//! Store-based DNS data with hickory-server's high-level server infrastructure.

use crate::storage::{QueryError, Store};
use hickory_proto::rr::rdata::{NS, SRV};
use hickory_proto::rr::{LowerName, Name, RData, Record, RecordType};
use hickory_server::authority::{
    Authority, LookupControlFlow, LookupError, LookupObject, LookupOptions,
    MessageRequest, ZoneType,
};
use hickory_server::server::RequestInfo;
use internal_dns_types::config::{DnsRecord, Srv};
use slog::{Logger, debug, warn};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::str::FromStr;

/// Omicron's Authority implementation that wraps our Store
///
/// This authority delegates all DNS queries to the Store, which already
/// handles zone routing and record lookups. The Authority trait integration
/// allows us to use hickory-server's ServerFuture with its automatic
/// eDNS support, TCP handling, and truncation.
pub struct OmicronAuthority {
    store: Store,
    origin: LowerName,
    zone_type: ZoneType,
    log: Logger,
}

impl OmicronAuthority {
    /// Create a new authority for the given zone
    pub fn new(
        store: Store,
        origin: Name,
        zone_type: ZoneType,
        log: Logger,
    ) -> Self {
        Self { store, origin: origin.into(), zone_type, log }
    }

    /// Convert a DnsRecord to a hickory Record
    ///
    /// This is essentially the same logic as the old dns_record_to_record function.
    fn dns_record_to_record(
        name: &Name,
        record: &DnsRecord,
    ) -> Result<Record, LookupError> {
        match record {
            DnsRecord::A(addr) => Ok(Record::from_rdata(
                name.clone(),
                0,
                RData::A((*addr).into()),
            )),

            DnsRecord::Aaaa(addr) => Ok(Record::from_rdata(
                name.clone(),
                0,
                RData::AAAA((*addr).into()),
            )),

            DnsRecord::Srv(Srv { prio, weight, port, target }) => {
                let tgt = Name::from_str(&target).map_err(|error| {
                    LookupError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("bad SRV target {:?}: {:#}", &target, error),
                    ))
                })?;
                Ok(Record::from_rdata(
                    name.clone(),
                    0,
                    RData::SRV(SRV::new(*prio, *weight, *port, tgt)),
                ))
            }

            DnsRecord::Ns(nsdname) => {
                let nsdname = Name::from_str(&nsdname).map_err(|error| {
                    LookupError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("bad NS dname {:?}: {:#}", &nsdname, error),
                    ))
                })?;
                Ok(Record::from_rdata(name.clone(), 0, RData::NS(NS(nsdname))))
            }
        }
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

// Implement the necessary traits for OmicronLookup
impl Iterator for OmicronLookup {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        if self.records.is_empty() {
            None
        } else {
            Some(self.records.remove(0))
        }
    }
}

impl From<Vec<Record>> for OmicronLookup {
    fn from(records: Vec<Record>) -> Self {
        Self::new(records)
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

impl Authority for OmicronAuthority {
    type Lookup = OmicronLookup;

    fn zone_type(&self) -> ZoneType {
        self.zone_type
    }

    fn is_axfr_allowed(&self) -> bool {
        // Zone transfers not yet supported
        false
    }

    fn update<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _update: &'life1 MessageRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<bool, hickory_proto::op::ResponseCode>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        // Dynamic DNS updates are handled via HTTP API, not DNS protocol
        Box::pin(async { Err(hickory_proto::op::ResponseCode::NotImp) })
    }

    fn origin(&self) -> &LowerName {
        &self.origin
    }

    fn lookup<'life0, 'life1, 'async_trait>(
        &'life0 self,
        name: &'life1 LowerName,
        rtype: RecordType,
        _lookup_options: LookupOptions,
    ) -> Pin<
        Box<
            dyn Future<Output = LookupControlFlow<Self::Lookup>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let store = self.store.clone();
        let name = name.clone();
        let log = self.log.clone();
        let origin = self.origin.clone();

        Box::pin(async move {
            // Convert LowerName to Name
            let query_name: Name = name.into();

            debug!(&log, "authority lookup";
                "name" => ?query_name,
                "rtype" => ?rtype,
                "origin" => ?origin,
            );

            // Query the store using query_name directly
            let answer = match store.query_name(&query_name) {
                Ok(answer) => answer,
                Err(QueryError::NoZone(zone)) => {
                    debug!(&log, "no zone for query";
                        "name" => ?query_name,
                        "zone" => &zone,
                    );
                    // Not our zone - return empty
                    return LookupControlFlow::Break(
                        Ok(OmicronLookup::empty()),
                    );
                }
                Err(e) => {
                    warn!(&log, "query failed"; "error" => ?e);
                    return LookupControlFlow::Break(Err(LookupError::Io(
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("query failed: {:#}", e),
                        ),
                    )));
                }
            };

            // Convert DnsRecords to hickory Records
            let mut records: Vec<Record> = Vec::new();

            // Add regular records
            if let Some(ref dns_records) = answer.records {
                for dns_record in dns_records {
                    match Self::dns_record_to_record(&query_name, dns_record) {
                        Ok(record) => records.push(record),
                        Err(e) => {
                            warn!(&log, "failed to convert record"; "error" => ?e);
                        }
                    }
                }
            }

            // Handle SOA queries at the apex
            if answer.name.is_none() && rtype == RecordType::SOA {
                match store.soa_for(&answer) {
                    Ok(soa_record) => records.push(soa_record),
                    Err(e) => {
                        warn!(&log, "failed to generate SOA"; "error" => ?e);
                    }
                }
            }

            // Filter records by type
            let filtered_records: Vec<Record> = records
                .into_iter()
                .filter(|record| match rtype {
                    RecordType::ANY => true,
                    RecordType::A => matches!(record.data(), RData::A(_)),
                    RecordType::AAAA => matches!(record.data(), RData::AAAA(_)),
                    RecordType::SRV => matches!(record.data(), RData::SRV(_)),
                    RecordType::NS => matches!(record.data(), RData::NS(_)),
                    RecordType::SOA => matches!(record.data(), RData::SOA(_)),
                    _ => false,
                })
                .collect();

            // Handle additional records for SRV and NS
            let mut additional_records: Vec<Record> = Vec::new();
            for record in &filtered_records {
                let target = match record.data() {
                    RData::SRV(srv) => Some(srv.target()),
                    RData::NS(ns) => Some(&ns.0),
                    _ => None,
                };

                if let Some(target) = target {
                    if let Ok(target_answer) = store.query_name(target) {
                        if let Some(target_records) = target_answer.records {
                            for target_record in &target_records {
                                if let Ok(record) = Self::dns_record_to_record(
                                    target,
                                    target_record,
                                ) {
                                    additional_records.push(record);
                                }
                            }
                        }
                    }
                }
            }

            // Return answers and additional records separately
            LookupControlFlow::Break(Ok(OmicronLookup::with_additionals(
                filtered_records,
                additional_records,
            )))
        })
    }

    fn search<'life0, 'life1, 'async_trait>(
        &'life0 self,
        request: RequestInfo<'life1>,
        lookup_options: LookupOptions,
    ) -> Pin<
        Box<
            dyn Future<Output = LookupControlFlow<Self::Lookup>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        // Delegate to lookup
        Box::pin(self.lookup(
            request.query.name(),
            request.query.query_type(),
            lookup_options,
        ))
    }

    fn get_nsec_records<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _name: &'life1 LowerName,
        _lookup_options: LookupOptions,
    ) -> Pin<
        Box<
            dyn Future<Output = LookupControlFlow<Self::Lookup>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        // DNSSEC not supported
        Box::pin(async { LookupControlFlow::Break(Ok(OmicronLookup::empty())) })
    }
}
