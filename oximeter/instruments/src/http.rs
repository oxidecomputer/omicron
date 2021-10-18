//! Instrumentation tools for HTTP services.

// Copyright 2021 Oxide Computer Company

use dropshot::{HttpError, HttpResponse, RequestContext, ServerContext};
use futures::Future;
use http::{Request, StatusCode};
use oximeter::histogram::Histogram;
use oximeter::{Error, Metric, Producer, Sample, Target};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// The [`HttpService`] is an [`oximeter::Target`] for monitoring HTTP servers.
#[derive(Debug, Clone, Target)]
pub struct HttpService {
    pub name: String,
    pub id: Uuid,
}

/// An [`oximeter::Metric`] that tracks a histogram of the latency of requests to a specified HTTP
/// endpoint.
#[derive(Debug, Clone, Metric)]
pub struct RequestLatencyHistogram {
    pub route: String,
    pub method: String,
    pub status_code: i64,
    #[datum]
    pub latency: Histogram<f64>,
}

impl RequestLatencyHistogram {
    /// Build a new `RequestLatencyHistogram` with a specified histogram.
    pub fn new<T>(
        request: &Request<T>,
        status_code: StatusCode,
        histogram: Histogram<f64>,
    ) -> Self {
        Self {
            route: request.uri().path().to_string(),
            method: request.method().to_string(),
            status_code: status_code.as_u16().into(),
            latency: histogram,
        }
    }

    /// Build a `RequestLatencyHistogram` with a histogram whose bins span the given decades.
    ///
    /// See [`Histogram::span_decades`] for details about how the bins are specified and used.
    pub fn with_latency_decades<T>(
        request: &Request<T>,
        status_code: StatusCode,
        start_decade: i8,
        end_decade: i8,
    ) -> Result<Self, Error> {
        Ok(Self::new(
            request,
            status_code,
            Histogram::span_decades(start_decade, end_decade)?,
        ))
    }

    pub fn key_for<T>(request: &Request<T>, status_code: StatusCode) -> String {
        format!(
            "{}:{}:{}",
            request.uri().path(),
            request.method(),
            status_code.as_u16()
        )
    }
}

#[derive(Debug, Clone)]
pub struct LatencyTracker {
    pub service: HttpService,
    pub latencies: Arc<Mutex<BTreeMap<String, RequestLatencyHistogram>>>,
    histogram: Histogram<f64>,
}

impl LatencyTracker {
    pub fn new(service: HttpService, histogram: Histogram<f64>) -> Self {
        Self {
            service,
            latencies: Arc::new(Mutex::new(BTreeMap::new())),
            histogram,
        }
    }

    pub fn with_latency_decades(
        service: HttpService,
        start_decade: i8,
        end_decade: i8,
    ) -> Result<Self, Error> {
        Ok(Self::new(
            service,
            Histogram::span_decades(start_decade, end_decade)?,
        ))
    }

    pub fn update<T>(
        &self,
        request: &Request<T>,
        status_code: StatusCode,
        latency: Duration,
    ) -> Result<(), Error> {
        let key = RequestLatencyHistogram::key_for(request, status_code);
        let mut latencies = self.latencies.lock().unwrap();
        let entry = latencies.entry(key).or_insert_with(|| {
            RequestLatencyHistogram::new(
                request,
                status_code,
                self.histogram.clone(),
            )
        });
        entry.latency.sample(latency.as_secs_f64()).map_err(Error::from)
    }

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
        let status_code = match result {
            Ok(_) => R::metadata().success.unwrap(),
            Err(ref e) => e.status_code,
        };
        let request = context.request.lock().await;
        self.update(&request, status_code, latency).map_err(|e| {
            HttpError::for_internal_error(format!(
                "error instrumenting dropshot request handler: {}",
                e.to_string()
            ))
        })?;
        result
    }
}

impl Producer for LatencyTracker {
    fn produce(
        &mut self,
    ) -> Result<Box<(dyn Iterator<Item = Sample> + 'static)>, Error> {
        // Clippy isn't correct here. It recommends using the iterator
        // over the latencies directly, but there is a lifetime mismatch
        // in that case: '_ would have to be 'static. The point is that
        // we need to return an iterator whose data isn't behind the
        // mutex this types uses internally.
        #[allow(clippy::needless_collect)]
        let latencies: Vec<_> =
            self.latencies.lock().unwrap().values().cloned().collect();
        let service = self.service.clone();
        Ok(Box::new(
            latencies
                .into_iter()
                .map(move |latency| Sample::new(&service, &latency)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "cc7a22ab-bc69-42c4-a75e-f1d9156eb351";

    #[test]
    fn test_latency_tracker() {
        let service = HttpService {
            name: String::from("my-service"),
            id: ID.parse().unwrap(),
        };
        let hist = Histogram::new(&[0.0, 1.0]).unwrap();
        let tracker = LatencyTracker::new(service, hist.clone());
        let request = http::request::Builder::new()
            .method(http::Method::GET)
            .uri("/some/uri")
            .body(())
            .unwrap();
        let status_code = StatusCode::OK;
        tracker
            .update(&request, status_code, Duration::from_secs_f64(0.5))
            .unwrap();

        let key = "/some/uri:GET:200";
        let actual_hist =
            tracker.latencies.lock().unwrap()[key].latency.clone();
        assert_eq!(actual_hist.n_samples(), 1);
        let bins = actual_hist.iter().collect::<Vec<_>>();
        assert_eq!(bins[1].count, 1);
    }
}
