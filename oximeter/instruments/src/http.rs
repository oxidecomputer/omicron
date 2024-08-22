// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instrumentation tools for HTTP services.

// Copyright 2024 Oxide Computer Company

use dropshot::{HttpError, HttpResponse, RequestContext, ServerContext};
use futures::Future;
use http::StatusCode;
use oximeter::{
    histogram::Histogram, histogram::Record, MetricsError, Producer, Sample,
};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash as _, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

oximeter::use_timeseries!("http-service.toml");
pub use http_service::HttpService;
pub use http_service::RequestLatencyHistogram;

impl RequestLatencyHistogram {
    /// Build a new `RequestLatencyHistogram` with a specified histogram.
    ///
    /// Latencies are expressed in seconds.
    pub fn new(
        operation_id: &str,
        status_code: StatusCode,
        histogram: Histogram<f64>,
    ) -> Self {
        Self {
            operation_id: operation_id.to_string().into(),
            status_code: status_code.as_u16(),
            datum: histogram,
        }
    }

    /// Build a `RequestLatencyHistogram` with a histogram whose bins span the given decades.
    ///
    /// `start_decade` and `end_decade` specify the lower and upper limits of the histogram's
    /// range, as a power of 10. For example, passing `-3` and `2` results in a histogram with bins
    /// spanning `[10 ** -3, 10 ** 2)`. There are 10 bins in each decade. See the
    /// [`Histogram::span_decades`] method for more details.
    ///
    /// Latencies are expressed as seconds.
    pub fn with_latency_decades(
        operation_id: &str,
        status_code: StatusCode,
        start_decade: i16,
        end_decade: i16,
    ) -> Result<Self, MetricsError> {
        Ok(Self::new(
            operation_id,
            status_code,
            Histogram::span_decades(start_decade, end_decade)?,
        ))
    }

    /// Return a key used to ID this histogram.
    ///
    /// This is a quick way to look up the histogram tracking any particular
    /// request and response.
    fn key_for(operation_id: &str, status_code: StatusCode) -> u64 {
        let mut hasher = DefaultHasher::new();
        operation_id.hash(&mut hasher);
        status_code.hash(&mut hasher);
        hasher.finish()
    }
}

/// The `LatencyTracker` is an [`oximeter::Producer`] that tracks the latencies of requests for an
/// HTTP service, in seconds.
///
/// Consumers should construct one `LatencyTracker` for each HTTP service they wish to instrument.
/// As requests are received, the [`LatencyTracker::update`] method can be called with the
/// request/response and the latency for handling the request, and the tracker will store that in
/// the appropriate histogram.
///
/// The `LatencyTracker` can be used to produce metric data collected by `oximeter`.
#[derive(Debug, Clone)]
pub struct LatencyTracker {
    /// The HTTP service target for which we're tracking request histograms.
    pub service: HttpService,
    /// The latency histogram for each request.
    ///
    /// The map here use a hash of the request fields (operation and status
    /// code) as the key to each histogram. It's a bit redundant to then store
    /// that in a hashmap, but this lets us avoid creating a new
    /// `RequestLatencyHistogram` when handling a request that we already have
    /// one for. Instead, we use this key to get the existing entry.
    latencies: Arc<Mutex<HashMap<u64, RequestLatencyHistogram>>>,
    /// The histogram used to track each request.
    ///
    /// We store it here to clone as we see new requests.
    histogram: Histogram<f64>,
}

impl LatencyTracker {
    /// Build a new tracker for the given `service`, using `histogram` to track latencies.
    ///
    /// Note that the same histogram is used for each tracked timeseries.
    pub fn new(service: HttpService, histogram: Histogram<f64>) -> Self {
        Self {
            service,
            latencies: Arc::new(Mutex::new(HashMap::new())),
            histogram,
        }
    }

    /// Build a new tracker for the given `service`, with a histogram that spans the given decades
    /// (powers of 10). See [`RequestLatencyHistogram::with_latency_decades`] for details on the
    /// arguments.
    pub fn with_latency_decades(
        service: HttpService,
        start_decade: i16,
        end_decade: i16,
    ) -> Result<Self, MetricsError> {
        Ok(Self::new(
            service,
            Histogram::span_decades(start_decade, end_decade)?,
        ))
    }

    /// Update (or create) a timeseries in response to a new request.
    ///
    /// This method adds the given `latency` to the internal histogram for tracking the timeseries
    /// to which the other arguments belong. (One is created if it does not exist.)
    pub fn update(
        &self,
        operation_id: &str,
        status_code: StatusCode,
        latency: Duration,
    ) -> Result<(), MetricsError> {
        let key = RequestLatencyHistogram::key_for(operation_id, status_code);
        let mut latencies = self.latencies.lock().unwrap();
        let entry = latencies.entry(key).or_insert_with(|| {
            RequestLatencyHistogram::new(
                operation_id,
                status_code,
                self.histogram.clone(),
            )
        });
        entry.datum.sample(latency.as_secs_f64()).map_err(MetricsError::from)
    }

    /// Instrument the given Dropshot endpoint handler function.
    ///
    /// This method is intended as a semi-convenient way to instrument the handler for a `dropshot`
    /// endpoint. The `context`, required by the `dropshot::endpoint` macro, provides information
    /// about the `Request` on which the handler operates. The `handler` is any future that
    /// produces an expected `dropshot` response. This method runs and times the handler, records
    /// the latency in the appropriate timeseries, and forwards the result of the handler to the
    /// caller.
    pub async fn instrument_dropshot_handler<T, H, R>(
        &self,
        context: &RequestContext<T>,
        handler: H,
    ) -> Result<R, HttpError>
    where
        R: HttpResponse,
        H: Future<Output = Result<R, HttpError>>,
        T: ServerContext,
    {
        let start = Instant::now();
        let result = handler.await;
        let latency = start.elapsed();
        let status_code = match &result {
            Ok(response) => response.status_code(),
            Err(ref e) => e.status_code,
        };
        if let Err(e) = self.update(&context.operation_id, status_code, latency)
        {
            slog::error!(
                &context.log,
                "error instrumenting dropshot handler";
                "error" => ?e,
                "status_code" => status_code.as_u16(),
                "operation_id" => &context.operation_id,
                "remote_addr" => context.request.remote_addr(),
                "latency" => ?latency,
            );
        }
        result
    }
}

impl Producer for LatencyTracker {
    fn produce(
        &mut self,
    ) -> Result<Box<(dyn Iterator<Item = Sample> + 'static)>, MetricsError>
    {
        // Clippy isn't correct here. It recommends using the iterator
        // over the latencies directly, but there is a lifetime mismatch
        // in that case: '_ would have to be 'static. The point is that
        // we need to return an iterator whose data isn't behind the
        // mutex this types uses internally.
        #[allow(clippy::needless_collect)]
        let latencies: Vec<_> =
            self.latencies.lock().unwrap().values().cloned().collect();
        let service = self.service.clone();
        let samples = latencies
            .into_iter()
            .map(|latency| Sample::new(&service, &latency))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Box::new(samples.into_iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "cc7a22ab-bc69-42c4-a75e-f1d9156eb351";

    #[test]
    fn test_latency_tracker() {
        let service =
            HttpService { name: "my-service".into(), id: ID.parse().unwrap() };
        let hist = Histogram::new(&[0.0, 1.0]).unwrap();
        let tracker = LatencyTracker::new(service, hist);
        let status_code0 = StatusCode::OK;
        let status_code1 = StatusCode::NOT_FOUND;
        let operation_id = "some_operation_id";
        tracker
            .update(operation_id, status_code0, Duration::from_secs_f64(0.5))
            .unwrap();
        tracker
            .update(operation_id, status_code1, Duration::from_secs_f64(0.5))
            .unwrap();
        let key0 = RequestLatencyHistogram::key_for(operation_id, status_code0);
        let key1 = RequestLatencyHistogram::key_for(operation_id, status_code1);
        let latencies = tracker.latencies.lock().unwrap();
        assert_eq!(latencies.len(), 2);
        for key in [key0, key1] {
            let actual_hist = &latencies[&key].datum;
            assert_eq!(actual_hist.n_samples(), 1);
            let bins = actual_hist.iter().collect::<Vec<_>>();
            assert_eq!(bins[1].count, 1);
        }
    }
}
