// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Flow and root stat tracking for individual OPTE ports.

use super::Handle;
use omicron_common::api::external::{
    self, Flow, FlowMetadata, FlowStat as ExternalFlowStat,
};
use oxide_vpc::api::{
    Direction, FlowPair, FlowStat, FullCounter, InnerFlowId, Protocol,
};
use slog::Logger;
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

// TODO: Controlplane needs to tell us the UUIDs of all routes, firewall rules.

const FLOW_STAT_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const ROOT_STAT_REFRESH_INTERVAL: Duration = Duration::from_secs(9);
const PRUNE_INTERVAL: Duration = Duration::from_secs(5);
const PRUNE_AGE: Duration = Duration::from_secs(10);

type UniqueFlow = (InnerFlowId, InnerFlowId, u64);

pub struct PortStats {
    shared: Arc<PortStatsShared>,
}

impl Drop for PortStats {
    fn drop(&mut self) {
        self.shared.task_quit.store(true, Ordering::Relaxed);
    }
}

impl PortStats {
    pub async fn new(name: impl Into<String>, log: Logger) -> Self {
        let shared = Arc::new(PortStatsShared {
            name: name.into(),
            task_quit: false.into(),
            state: Default::default(),
            log,
        });

        tokio::spawn(run_port_stat(Arc::clone(&shared)));

        Self { shared }
    }

    /// Gather all flow stats when requested by a client.
    pub fn flow_stats(&self) -> Vec<Flow> {
        let mut out = HashMap::new();

        {
            let state = self.shared.state.read().unwrap();
            for (flowid, Timed { body, .. }) in &state.flows {
                let unique = unique_flow(&flowid, &body.last);
                let Some(ufid) = state.flow_instances.get(&unique) else {
                    slog::error!(&self.shared.log, "Hi?");
                    continue;
                };

                let mut forwarded = None;
                let bases = body
                    .last
                    .bases
                    .iter()
                    .filter_map(|v| match state.label_map.get(v) {
                        Some(FlowLabel::Destination(v)) => {
                            forwarded = Some(v.clone());
                            None
                        }
                        Some(FlowLabel::Entity(v)) => Some(v.clone()),
                        _ => None,
                    })
                    .collect();

                match out.entry(unique) {
                    Entry::Vacant(val) => {
                        // Use the stats from the first entry encountered.
                        let (in_key, out_key, ad_in, ad_out) = match body
                            .last
                            .dir
                        {
                            Direction::In => {
                                (*flowid, body.last.partner, Some(bases), None)
                            }
                            Direction::Out => {
                                (body.last.partner, *flowid, None, Some(bases))
                            }
                        };
                        val.insert(Flow {
                            metadata: FlowMetadata {
                                flow_id: ufid.body,
                                // TODO: need to correlate timestamps between
                                //       kmod and here?!
                                created_at: Default::default(),
                                initial_packet: direction(body.last.first_dir),
                                internal_key: flowkey(out_key),
                                external_key: flowkey(in_key),
                                admitted_by_in: ad_in,
                                admitted_by_out: ad_out,
                                forwarded,
                            },
                            in_stat: ExternalFlowStat {
                                packets: body.last.stats.pkts_in,
                                bytes: body.last.stats.bytes_in,
                                packet_rate: body.in_packets_per_sec,
                                byte_rate: body.in_bytes_per_sec,
                            },
                            out_stat: ExternalFlowStat {
                                packets: body.last.stats.pkts_out,
                                bytes: body.last.stats.bytes_out,
                                packet_rate: body.out_packets_per_sec,
                                byte_rate: body.out_bytes_per_sec,
                            },
                        });
                    }
                    Entry::Occupied(mut val) => {
                        // The second half fills in the remaining metadata.
                        let val = val.get_mut();
                        match body.last.dir {
                            Direction::In => {
                                val.metadata.admitted_by_in = Some(bases)
                            }
                            Direction::Out => {
                                val.metadata.admitted_by_out = Some(bases)
                            }
                        }
                    }
                }
            }
        }

        out.into_values().collect()
    }

    // TODO: want `fn root_stats`, need to be able to pull back up into
    //       oximeter in articular.
}

struct PortStatsShared {
    name: String,
    state: RwLock<State>,
    task_quit: AtomicBool,
    log: Logger,
}

impl PortStatsShared {
    fn collect_flows(&self) -> Result<(), anyhow::Error> {
        let new_stats = {
            let hdl = Handle::new()?;
            hdl.dump_flow_stats(&self.name, [])?
        };
        let now = Instant::now();

        let mut state = self.state.write().unwrap();
        for (flowid, stat) in new_stats.flow_stats {
            let unique = unique_flow(&flowid, &stat);
            state
                .flow_instances
                .entry(unique)
                .or_insert_with(|| Timed { hit_at: now, body: Uuid::new_v4() })
                .hit_at = now;

            match state.flows.entry(flowid) {
                Entry::Occupied(mut val) => {
                    let val = val.get_mut();
                    let elapsed = now.duration_since(val.hit_at);
                    val.body.update(stat, &elapsed);
                    val.hit_at = now;
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(Timed {
                        hit_at: now,
                        body: FlowSnapshot::new(stat),
                    });
                }
            }
        }

        Ok(())
    }

    fn collect_roots(&self) -> Result<(), anyhow::Error> {
        let new_stats = {
            let hdl = Handle::new()?;
            hdl.dump_flow_stats(&self.name, [])?
        };
        let now = Instant::now();

        Ok(())
    }

    fn prune(&self) {
        let now = Instant::now();
        let mut state = self.state.write().unwrap();

        state.flows.retain(|_, v| now.duration_since(v.hit_at) <= PRUNE_AGE);
        state.roots.retain(|_, v| now.duration_since(v.hit_at) <= PRUNE_AGE);
        state
            .flow_instances
            .retain(|_, v| now.duration_since(v.hit_at) <= PRUNE_AGE);
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Hash, Eq)]
struct Timed<S> {
    hit_at: Instant,
    body: S,
}

#[derive(Default)]
struct State {
    flows: HashMap<InnerFlowId, Timed<FlowSnapshot>>,
    roots: HashMap<Uuid, Timed<FullCounter>>,
    label_map: HashMap<Uuid, FlowLabel>,
    flow_instances: HashMap<UniqueFlow, Timed<Uuid>>,
}

async fn run_port_stat(state: Arc<PortStatsShared>) {
    let mut flow_collect = tokio::time::interval(FLOW_STAT_REFRESH_INTERVAL);
    let mut root_collect = tokio::time::interval(ROOT_STAT_REFRESH_INTERVAL);
    let mut prune = tokio::time::interval(PRUNE_INTERVAL);
    flow_collect.set_missed_tick_behavior(MissedTickBehavior::Skip);
    root_collect.set_missed_tick_behavior(MissedTickBehavior::Skip);
    prune.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        if state.task_quit.load(Ordering::Relaxed) {
            return;
        }

        tokio::select! {
            _ = flow_collect.tick() => {
                state.collect_flows();
            },
            _ = root_collect.tick() => {
                state.collect_roots();
            },
            _ = prune.tick() => {
                state.prune();
            },
        }
    }
}

pub enum FlowLabel {
    Entity(external::VpcEntity),
    Destination(external::ForwardClass),
    Builtin(VpcBuiltinLabel),
}

pub enum FirewallLabel {
    Rule(Uuid),
    Default,
}

pub enum VpcBuiltinLabel {
    // TODO
}

#[derive(Debug)]
pub struct FlowSnapshot {
    pub last: FlowStat<InnerFlowId>,

    pub in_packets_per_sec: f64,
    pub in_bytes_per_sec: f64,
    pub out_packets_per_sec: f64,
    pub out_bytes_per_sec: f64,
}

impl FlowSnapshot {
    fn new(stat: FlowStat<InnerFlowId>) -> Self {
        Self {
            last: stat,

            in_packets_per_sec: 0.0,
            in_bytes_per_sec: 0.0,
            out_packets_per_sec: 0.0,
            out_bytes_per_sec: 0.0,
        }
    }

    fn update(&mut self, stat: FlowStat<InnerFlowId>, elapsed: &Duration) {
        let elapsed = elapsed.as_secs_f64();
        self.in_packets_per_sec =
            (stat.stats.pkts_in - self.last.stats.pkts_in) as f64 / elapsed;
        self.in_bytes_per_sec =
            (stat.stats.bytes_in - self.last.stats.bytes_in) as f64 / elapsed;
        self.out_packets_per_sec =
            (stat.stats.pkts_out - self.last.stats.pkts_out) as f64 / elapsed;
        self.out_bytes_per_sec =
            (stat.stats.bytes_out - self.last.stats.bytes_out) as f64 / elapsed;
        self.last = stat;
    }
}

fn unique_flow(id: &InnerFlowId, stat: &FlowStat<InnerFlowId>) -> UniqueFlow {
    let (in_fid, out_fid) = match stat.dir {
        Direction::In => (*id, stat.partner),
        Direction::Out => (stat.partner, *id),
    };

    (in_fid, out_fid, stat.stats.created_at)
}

fn direction(dir: Direction) -> external::Direction {
    match dir {
        Direction::In => external::Direction::In,
        Direction::Out => external::Direction::Out,
    }
}

fn flowkey(id: InnerFlowId) -> external::Flowkey {
    let (source_address, destination_address) = match id.addrs {
        oxide_vpc::api::AddrPair::V4 { src, dst } => {
            (src.bytes().into(), dst.bytes().into())
        }
        oxide_vpc::api::AddrPair::V6 { src, dst } => {
            (src.bytes().into(), dst.bytes().into())
        }
    };
    let info = id.l4_info().map(|v| match v {
        oxide_vpc::api::L4Info::Ports(port_info) => {
            external::ProtocolInfo::Ports(external::PortProtocolInfo {
                source_port: port_info.src_port,
                destination_port: port_info.dst_port,
            })
        }
        oxide_vpc::api::L4Info::Icmpv4(icmp_info) => {
            external::ProtocolInfo::Icmp(external::IcmpProtocolInfo {
                r#type: icmp_info.ty,
                code: icmp_info.code,
                id: match icmp_info.ty {
                    0 | 8 => Some(icmp_info.id),
                    _ => None,
                },
            })
        }
        oxide_vpc::api::L4Info::Icmpv6(icmp_info) => {
            external::ProtocolInfo::Icmp(external::IcmpProtocolInfo {
                r#type: icmp_info.ty,
                code: icmp_info.code,
                id: match icmp_info.ty {
                    128 | 129 => Some(icmp_info.id),
                    _ => None,
                },
            })
        }
    });
    external::Flowkey {
        source_address,
        destination_address,
        protocol: id.proto,
        info,
    }
}
