// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Flow and root stat tracking for individual OPTE ports.

use std::{collections::{hash_map::Entry, HashMap}, sync::{atomic::{AtomicBool, Ordering}, Arc, RwLock}, time::{Duration, Instant}};
use oxide_vpc::api::{FlowPair, FlowStat, FullCounter, InnerFlowId};
use slog::Logger;
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use super::Handle;

const FLOW_STAT_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const ROOT_STAT_REFRESH_INTERVAL: Duration = Duration::from_secs(9);
const PRUNE_INTERVAL: Duration = Duration::from_secs(5);
const PRUNE_AGE: Duration = Duration::from_secs(10);

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
            match state.flows.entry(flowid) {
                Entry::Occupied(mut val) => {
                    let val = val.get_mut();
                    let elapsed = now.duration_since(val.hit_at);
                    val.body.update(stat, &elapsed);
                    val.hit_at = now;

                },
                Entry::Vacant(vacant) => {
                    vacant.insert(Timed {
                        hit_at: now,
                        body: FlowSnapshot::new(stat),
                    });
                },
            }
        }

        Ok(())
    }

    fn prune(&self) {
        let now = Instant::now();
        let mut state = self.state.write().unwrap();

        state.flows.retain(|_, v| now.duration_since(v.hit_at) <= PRUNE_AGE);
        state.roots.retain(|_, v| now.duration_since(v.hit_at) <= PRUNE_AGE);
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
            _ = root_collect.tick() => {},
            _ = prune.tick() => {
                state.prune();
            },
        }
    }
}

pub enum FlowLabel {
    FirewallRule(FirewallLabel),
    Route(Uuid),
    Destination(DeliveryClass),
    Builtin(VpcBuiltinLabel)
}

pub enum FirewallLabel {
    Rule(Uuid),
    Default,
}

pub enum DeliveryClass {
    Vpc,
    VpcPeer,
    Internet,
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
        self.in_packets_per_sec = (stat.stats.pkts_in - self.last.stats.pkts_in) as f64 / elapsed;
        self.in_bytes_per_sec = (stat.stats.bytes_in - self.last.stats.bytes_in) as f64 / elapsed;
        self.out_packets_per_sec = (stat.stats.pkts_out - self.last.stats.pkts_out) as f64 / elapsed;
        self.out_bytes_per_sec = (stat.stats.bytes_out - self.last.stats.bytes_out) as f64 / elapsed;
        self.last = stat;
    }
}

