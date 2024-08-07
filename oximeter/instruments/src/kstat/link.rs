// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Report metrics about Ethernet data links on the host system

use crate::kstat::hrtime_to_utc;
use crate::kstat::ConvertNamedData;
use crate::kstat::Error;
use crate::kstat::KstatList;
use crate::kstat::KstatTarget;
use chrono::DateTime;
use chrono::Utc;
use kstat_rs::Data;
use kstat_rs::Kstat;
use kstat_rs::Named;
use oximeter::types::Cumulative;
use oximeter::Sample;

oximeter::use_timeseries!("sled-data-link.toml");

// Helper function to extract the same kstat metrics from all link targets.
fn extract_link_kstats<T>(
    target: &T,
    named_data: &Named,
    creation_time: DateTime<Utc>,
    snapshot_time: DateTime<Utc>,
) -> Option<Result<Sample, Error>>
where
    T: KstatTarget,
{
    let Named { name, value } = named_data;
    if *name == "rbytes64" {
        Some(value.as_u64().and_then(|x| {
            let metric = sled_data_link::BytesReceived {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "obytes64" {
        Some(value.as_u64().and_then(|x| {
            let metric = sled_data_link::BytesSent {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "ipackets64" {
        Some(value.as_u64().and_then(|x| {
            let metric = sled_data_link::PacketsReceived {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "opackets64" {
        Some(value.as_u64().and_then(|x| {
            let metric = sled_data_link::PacketsSent {
                datum: Cumulative::with_start_time(creation_time, x),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "ierrors" {
        Some(value.as_u32().and_then(|x| {
            let metric = sled_data_link::ErrorsReceived {
                datum: Cumulative::with_start_time(creation_time, x.into()),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else if *name == "oerrors" {
        Some(value.as_u32().and_then(|x| {
            let metric = sled_data_link::ErrorsSent {
                datum: Cumulative::with_start_time(creation_time, x.into()),
            };
            Sample::new_with_timestamp(snapshot_time, target, &metric)
                .map_err(Error::Sample)
        }))
    } else {
        None
    }
}

// Helper trait for defining `KstatTarget` for all the link-based stats.
trait LinkKstatTarget: KstatTarget {
    fn link_name(&self) -> &str;
}

impl LinkKstatTarget for sled_data_link::SledDataLink {
    fn link_name(&self) -> &str {
        &self.link_name
    }
}

impl<T> KstatTarget for T
where
    T: LinkKstatTarget,
{
    fn interested(&self, kstat: &Kstat<'_>) -> bool {
        kstat.ks_module == "link"
            && kstat.ks_instance == 0
            && kstat.ks_name == self.link_name()
    }

    fn to_samples(
        &self,
        kstats: KstatList<'_, '_>,
    ) -> Result<Vec<Sample>, Error> {
        let Some((creation_time, kstat, data)) = kstats.first() else {
            return Ok(vec![]);
        };
        let snapshot_time = hrtime_to_utc(kstat.ks_snaptime)?;
        let Data::Named(named) = data else {
            return Err(Error::ExpectedNamedKstat);
        };
        named
            .iter()
            .filter_map(|nd| {
                extract_link_kstats(self, nd, *creation_time, snapshot_time)
            })
            .collect()
    }
}

#[cfg(all(test, target_os = "illumos"))]
mod tests {
    use super::*;
    use crate::kstat::sampler::KstatPath;
    use crate::kstat::sampler::CREATION_TIME_PRUNE_INTERVAL;
    use crate::kstat::CollectionDetails;
    use crate::kstat::KstatSampler;
    use crate::kstat::TargetStatus;
    use kstat_rs::Ctl;
    use oximeter::Producer;
    use rand::distributions::Uniform;
    use rand::Rng;
    use slog::info;
    use slog::Drain;
    use slog::Logger;
    use std::time::Duration;
    use tokio::time::Instant;
    use uuid::uuid;
    use uuid::Uuid;

    fn test_logger() -> Logger {
        let dec =
            slog_term::PlainSyncDecorator::new(slog_term::TestStdoutWriter);
        let drain = slog_term::FullFormat::new(dec).build().fuse();
        let log =
            Logger::root(drain, slog::o!("component" => "fake-cleanup-task"));
        log
    }

    const RACK_ID: Uuid = uuid!("de784702-cafb-41a9-b3e5-93af189def29");
    const SLED_ID: Uuid = uuid!("88240343-5262-45f4-86f1-3c82fe383f2a");
    const SLED_MODEL: &str = "fake-gimlet";
    const SLED_REVISION: u32 = 1;
    const SLED_SERIAL: &str = "fake-serial";
    const KIND: &str = "etherstub";
    const ZONE_NAME: &str = "global";

    // An etherstub we can use for testing.
    //
    // This is not meant to produce real data. It is simply a data link that
    // shows up with the `link:::` kstat scheme, and which doesn't require us to
    // decide which physical link over which to create something like a VNIC.
    #[derive(Debug)]
    struct TestEtherstub {
        name: String,
    }

    impl TestEtherstub {
        const PFEXEC: &'static str = "/usr/bin/pfexec";
        const DLADM: &'static str = "/usr/sbin/dladm";
        fn new() -> Self {
            let name = format!(
                "kstest{}0",
                rand::thread_rng()
                    .sample_iter(Uniform::new('a', 'z'))
                    .take(5)
                    .map(char::from)
                    .collect::<String>(),
            );
            Self::create(&name);
            Self { name }
        }

        fn create(name: &str) {
            let output = std::process::Command::new(Self::PFEXEC)
                .env_clear()
                .arg(Self::DLADM)
                .arg("create-etherstub")
                .arg("-t")
                .arg(name)
                .output()
                .expect("failed to spawn dladm");
            assert!(
                output.status.success(),
                "failed to create test etherstub:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    impl Drop for TestEtherstub {
        fn drop(&mut self) {
            let output = std::process::Command::new(Self::PFEXEC)
                .env_clear()
                .arg(Self::DLADM)
                .arg("delete-etherstub")
                .arg(&self.name)
                .output()
                .expect("failed to spawn dladm");
            if !output.status.success() {
                eprintln!(
                    "Failed to delete etherstub '{}'.\n\
                    Delete manually with `dladm delete-etherstub {}`:\n{}",
                    &self.name,
                    &self.name,
                    String::from_utf8_lossy(&output.stderr),
                );
            }
        }
    }

    #[test]
    fn test_sled_datalink() {
        let link = TestEtherstub::new();
        let dl = sled_data_link::SledDataLink {
            rack_id: RACK_ID,
            sled_id: SLED_ID,
            sled_serial: SLED_SERIAL.into(),
            link_name: link.name.clone().into(),
            kind: KIND.into(),
            sled_model: SLED_MODEL.into(),
            sled_revision: SLED_REVISION,
            zone_name: ZONE_NAME.into(),
        };
        let ctl = Ctl::new().unwrap();
        let ctl = ctl.update().unwrap();
        let mut kstat = ctl
            .filter(Some("link"), Some(0), Some(link.name.as_str()))
            .next()
            .unwrap();
        let creation_time = hrtime_to_utc(kstat.ks_crtime).unwrap();
        let data = ctl.read(&mut kstat).unwrap();
        let samples = dl.to_samples(&[(creation_time, kstat, data)]).unwrap();
        println!("{samples:#?}");
    }

    #[tokio::test]
    async fn test_kstat_sampler() {
        let mut sampler = KstatSampler::new(&test_logger()).unwrap();
        let link = TestEtherstub::new();
        let dl = sled_data_link::SledDataLink {
            rack_id: RACK_ID,
            sled_id: SLED_ID,
            sled_serial: SLED_SERIAL.into(),
            link_name: link.name.clone().into(),
            kind: KIND.into(),
            sled_model: SLED_MODEL.into(),
            sled_revision: SLED_REVISION,
            zone_name: ZONE_NAME.into(),
        };
        let details = CollectionDetails::never(Duration::from_secs(1));
        let id = sampler.add_target(dl, details).await.unwrap();
        let samples: Vec<_> = sampler.produce().unwrap().collect();
        assert!(samples.is_empty());

        // Pause time, and advance until we're notified of new samples.
        tokio::time::pause();
        const MAX_DURATION: Duration = Duration::from_secs(3);
        const STEP_DURATION: Duration = Duration::from_secs(1);
        let now = Instant::now();
        let expected_counts = loop {
            tokio::time::advance(STEP_DURATION).await;
            if now.elapsed() > MAX_DURATION {
                panic!("Waited too long for samples");
            }
            if let Some(counts) = sampler.sample_counts() {
                break counts;
            }
        };
        let samples: Vec<_> = sampler.produce().unwrap().collect();
        println!("{samples:#?}");
        assert_eq!(samples.len(), expected_counts.total);
        assert_eq!(expected_counts.overflow, 0);

        // Test status and remove behavior.
        tokio::time::resume();
        assert!(matches!(
            sampler.target_status(id).await.unwrap(),
            TargetStatus::Ok { .. },
        ));
        sampler.remove_target(id).await.unwrap();
        assert!(sampler.target_status(id).await.is_err());
    }

    #[tokio::test]
    async fn test_kstat_sampler_with_overflow() {
        let limit = 2;
        let mut sampler =
            KstatSampler::with_sample_limit(&test_logger(), limit).unwrap();
        let link = TestEtherstub::new();
        let dl = sled_data_link::SledDataLink {
            rack_id: RACK_ID,
            sled_id: SLED_ID,
            sled_serial: SLED_SERIAL.into(),
            link_name: link.name.clone().into(),
            kind: KIND.into(),
            sled_model: SLED_MODEL.into(),
            sled_revision: SLED_REVISION,
            zone_name: ZONE_NAME.into(),
        };
        let details = CollectionDetails::never(Duration::from_secs(1));
        sampler.add_target(dl, details).await.unwrap();
        let samples: Vec<_> = sampler.produce().unwrap().collect();
        assert!(samples.is_empty());

        // Pause time, and advance until we're notified of new samples.
        tokio::time::pause();
        const MAX_DURATION: Duration = Duration::from_secs(3);
        const STEP_DURATION: Duration = Duration::from_secs(1);
        let now = Instant::now();
        let expected_counts = loop {
            tokio::time::advance(STEP_DURATION).await;
            if now.elapsed() > MAX_DURATION {
                panic!("Waited too long for samples");
            }
            if let Some(counts) = sampler.sample_counts() {
                break counts;
            }
        };

        // We should have produced 2 samples from the actual target, plus one
        // from the counter indicating we've dropped some samples!
        let samples: Vec<_> = sampler.produce().unwrap().collect();
        let (link_samples, dropped_samples): (Vec<_>, Vec<_>) = samples
            .iter()
            .partition(|s| s.timeseries_name.contains("sled_data_link"));
        println!("{link_samples:#?}");
        assert_eq!(link_samples.len(), limit);

        // The total number of samples less overflow should be match the number
        // of samples for the link we've produced.
        assert_eq!(
            link_samples.len(),
            expected_counts.total - expected_counts.overflow
        );

        // The worker must have produced one sample representing the number of
        // overflows.
        println!("{dropped_samples:#?}");
        assert_eq!(dropped_samples.len(), 1);

        // Verify that we actually counted the correct number of dropped
        // samples.
        let oximeter::Datum::CumulativeU64(overflow) =
            dropped_samples[0].measurement.datum()
        else {
            unreachable!();
        };
        assert_eq!(overflow.value(), expected_counts.overflow as u64);
    }

    #[tokio::test]
    async fn test_kstat_with_expiration() {
        // Create a VNIC, which we'll start tracking from, then delete it and
        // make sure we expire after the expected period.
        let log = test_logger();
        let mut sampler = KstatSampler::new(&log).unwrap();
        let link = TestEtherstub::new();
        info!(log, "created test etherstub"; "name" => &link.name);
        let dl = sled_data_link::SledDataLink {
            rack_id: RACK_ID,
            sled_id: SLED_ID,
            sled_serial: SLED_SERIAL.into(),
            link_name: link.name.clone().into(),
            kind: KIND.into(),
            sled_model: SLED_MODEL.into(),
            sled_revision: SLED_REVISION,
            zone_name: ZONE_NAME.into(),
        };
        let collection_interval = Duration::from_secs(1);
        let expiry = Duration::from_secs(1);
        let details = CollectionDetails::duration(collection_interval, expiry);
        let id = sampler.add_target(dl, details).await.unwrap();
        info!(log, "target added"; "id" => ?id);
        assert!(matches!(
            sampler.target_status(id).await.unwrap(),
            TargetStatus::Ok { .. },
        ));

        // Delete the link right away.
        drop(link);
        info!(log, "dropped test etherstub");

        // Pause time, and advance until we should have expired the target.
        tokio::time::pause();
        const MAX_DURATION: Duration = Duration::from_secs(3);
        let now = Instant::now();
        let is_expired = loop {
            tokio::time::advance(expiry).await;
            if now.elapsed() > MAX_DURATION {
                panic!("Waited too long for samples");
            }
            if let TargetStatus::Expired { .. } =
                sampler.target_status(id).await.unwrap()
            {
                break true;
            }
        };
        assert!(is_expired, "Target should have expired by now");

        // We should have some self-stat expiration samples now.
        let samples = sampler.produce().unwrap();
        let expiration_samples: Vec<_> = samples
            .filter(|sample| {
                sample.timeseries_name == "kstat_sampler:expired_targets"
            })
            .collect();
        assert_eq!(expiration_samples.len(), 1);
    }

    // A sanity check that a cumulative start time does not change over time,
    // since we've fixed the time reference at the time it was added.
    #[tokio::test]
    async fn test_kstat_start_time_is_equal() {
        let log = test_logger();
        let mut sampler = KstatSampler::new(&log).unwrap();
        let link = TestEtherstub::new();
        info!(log, "created test etherstub"; "name" => &link.name);
        let dl = sled_data_link::SledDataLink {
            rack_id: RACK_ID,
            sled_id: SLED_ID,
            sled_serial: SLED_SERIAL.into(),
            link_name: link.name.clone().into(),
            kind: KIND.into(),
            sled_model: SLED_MODEL.into(),
            sled_revision: SLED_REVISION,
            zone_name: ZONE_NAME.into(),
        };
        let collection_interval = Duration::from_secs(1);
        let expiry = Duration::from_secs(1);
        let details = CollectionDetails::duration(collection_interval, expiry);
        let id = sampler.add_target(dl, details).await.unwrap();
        info!(log, "target added"; "id" => ?id);
        assert!(matches!(
            sampler.target_status(id).await.unwrap(),
            TargetStatus::Ok { .. },
        ));
        tokio::time::pause();
        let now = Instant::now();
        while now.elapsed() < (expiry * 10) {
            tokio::time::advance(expiry).await;
        }
        let samples = sampler.produce().unwrap();
        let mut start_times = samples
            .filter(|sample| {
                sample.timeseries_name.as_str().starts_with("sled")
            })
            .map(|sample| sample.measurement.start_time().unwrap());
        let first = start_times.next().unwrap();
        println!("{first}");
        assert!(start_times.all(|t| {
            println!("{t}");
            t == first
        }));
    }

    #[tokio::test]
    async fn test_prune_creation_times_when_kstat_is_gone() {
        // Create a VNIC, which we'll start tracking from, then delete it and
        // make sure the creation times are pruned.
        let log = test_logger();
        let sampler = KstatSampler::new(&log).unwrap();
        let link = TestEtherstub::new();
        let path = KstatPath {
            module: "link".to_string(),
            instance: 0,
            name: link.name.clone(),
        };
        info!(log, "created test etherstub"; "name" => &link.name);
        let dl = sled_data_link::SledDataLink {
            rack_id: RACK_ID,
            sled_id: SLED_ID,
            sled_serial: SLED_SERIAL.into(),
            link_name: link.name.clone().into(),
            kind: KIND.into(),
            sled_model: SLED_MODEL.into(),
            sled_revision: SLED_REVISION,
            zone_name: ZONE_NAME.into(),
        };
        let collection_interval = Duration::from_secs(1);
        let expiry = Duration::from_secs(1);
        let details = CollectionDetails::duration(collection_interval, expiry);
        let id = sampler.add_target(dl, details).await.unwrap();
        info!(log, "target added"; "id" => ?id);
        assert!(matches!(
            sampler.target_status(id).await.unwrap(),
            TargetStatus::Ok { .. },
        ));

        // Delete the link right away.
        drop(link);
        info!(log, "dropped test etherstub");

        // Advance time through the prune interval.
        tokio::time::pause();
        let now = Instant::now();
        while now.elapsed() < CREATION_TIME_PRUNE_INTERVAL + expiry {
            tokio::time::advance(expiry).await;
        }

        // Now check that the creation times are pruned.
        let times = sampler.creation_times().await;
        assert!(!times.contains_key(&path));
    }

    #[tokio::test]
    async fn test_prune_creation_times_when_target_is_removed() {
        // Create a VNIC, which we'll start tracking from, then delete it and
        // make sure the creation times are pruned.
        let log = test_logger();
        let sampler = KstatSampler::new(&log).unwrap();
        let link = TestEtherstub::new();
        let path = KstatPath {
            module: "link".to_string(),
            instance: 0,
            name: link.name.clone(),
        };
        info!(log, "created test etherstub"; "name" => &link.name);
        let dl = sled_data_link::SledDataLink {
            rack_id: RACK_ID,
            sled_id: SLED_ID,
            sled_serial: SLED_SERIAL.into(),
            link_name: link.name.clone().into(),
            kind: KIND.into(),
            sled_model: SLED_MODEL.into(),
            sled_revision: SLED_REVISION,
            zone_name: ZONE_NAME.into(),
        };
        let collection_interval = Duration::from_secs(1);
        let expiry = Duration::from_secs(1);
        let details = CollectionDetails::duration(collection_interval, expiry);
        let id = sampler.add_target(dl, details).await.unwrap();
        info!(log, "target added"; "id" => ?id);
        assert!(matches!(
            sampler.target_status(id).await.unwrap(),
            TargetStatus::Ok { .. },
        ));

        // Remove the target, but do not drop the link. This will mean that the
        // underlying kstat is still around, even though there's no target
        // that's interested in it. We should keep it, in this case.
        sampler.remove_target(id).await.unwrap();

        // Advance time through the prune interval.
        tokio::time::pause();
        let now = Instant::now();
        while now.elapsed() < CREATION_TIME_PRUNE_INTERVAL + expiry {
            tokio::time::advance(expiry).await;
        }

        // Now check that the creation time is still around.
        let times = sampler.creation_times().await;
        assert!(times.contains_key(&path));
    }
}
