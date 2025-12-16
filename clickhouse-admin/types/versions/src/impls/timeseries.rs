// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for timeseries types.

use crate::latest::timeseries::{
    SystemTable, SystemTimeSeries, SystemTimeSeriesSettings, TimestampFormat,
};
use anyhow::Result;
use slog::{Logger, info};
use std::fmt;

impl fmt::Display for SystemTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let table = match self {
            SystemTable::MetricLog => "metric_log",
            SystemTable::AsynchronousMetricLog => "asynchronous_metric_log",
        };
        write!(f, "{}", table)
    }
}

impl fmt::Display for TimestampFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let table = match self {
            TimestampFormat::Utc => "iso",
            TimestampFormat::UnixEpoch => "unix_timestamp",
        };
        write!(f, "{}", table)
    }
}

impl SystemTimeSeriesSettings {
    fn interval(&self) -> u64 {
        self.retrieval_settings.interval
    }

    fn time_range(&self) -> u64 {
        self.retrieval_settings.time_range
    }

    fn timestamp_format(&self) -> TimestampFormat {
        self.retrieval_settings.timestamp_format
    }

    fn metric_name(&self) -> &str {
        &self.metric_info.metric
    }

    fn table(&self) -> SystemTable {
        self.metric_info.table
    }

    // TODO: Use more aggregate functions than just avg?
    pub fn query_avg(&self) -> String {
        let interval = self.interval();
        let time_range = self.time_range();
        let metric_name = self.metric_name();
        let table = self.table();
        let ts_fmt = self.timestamp_format();

        let avg_value = match table {
            SystemTable::MetricLog => metric_name,
            SystemTable::AsynchronousMetricLog => "value",
        };

        let mut query = format!(
            "SELECT toStartOfInterval(event_time, INTERVAL {interval} SECOND) AS time, avg({avg_value}) AS value
            FROM system.{table}
            WHERE event_date >= toDate(now() - {time_range}) AND event_time >= now() - {time_range}
            "
        );

        match table {
            SystemTable::MetricLog => (),
            SystemTable::AsynchronousMetricLog => query.push_str(
                format!(
                    "AND metric = '{metric_name}'
                "
                )
                .as_str(),
            ),
        };

        query.push_str(
            format!(
                "GROUP BY time
                ORDER BY time WITH FILL STEP {interval}
                FORMAT JSONEachRow
                SETTINGS date_time_output_format = '{ts_fmt}'"
            )
            .as_str(),
        );
        query
    }
}

impl SystemTimeSeries {
    pub fn parse(log: &Logger, data: &[u8]) -> Result<Vec<Self>> {
        let s = String::from_utf8_lossy(data);
        info!(
            log,
            "Retrieved data from `system` database";
            "output" => ?s
        );

        let mut m = vec![];

        for line in s.lines() {
            // serde_json deserialises f64 types with loss of precision at times.
            // For example, in our tests some of the values to serialize have a
            // fractional value of `.33333`, but once parsed, they become `.33331`.
            //
            // We do not require this level of precision, so we'll leave as is.
            // Just noting that we are aware of this slight inaccuracy.
            let item: SystemTimeSeries = serde_json::from_str(line)?;
            m.push(item);
        }

        Ok(m)
    }
}

#[cfg(test)]
mod tests {
    use crate::latest::timeseries::SystemTimeSeries;
    use slog::{Drain, o};
    use slog_term::{FullFormat, PlainDecorator, TestStdoutWriter};

    fn log() -> slog::Logger {
        let decorator = PlainDecorator::new(TestStdoutWriter);
        let drain = FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    }

    #[test]
    fn test_unix_epoch_system_timeseries_parse_success() {
        let log = log();
        let data = "{\"time\":\"1732494720\",\"value\":110220450825.75238}
{\"time\":\"1732494840\",\"value\":110339992917.33333}
{\"time\":\"1732494960\",\"value\":110421854037.33333}\n"
            .as_bytes();
        let timeseries = SystemTimeSeries::parse(&log, data).unwrap();

        let expected = vec![
            SystemTimeSeries {
                time: "1732494720".to_string(),
                value: 110220450825.75238,
            },
            SystemTimeSeries {
                time: "1732494840".to_string(),
                value: 110339992917.33331,
            },
            SystemTimeSeries {
                time: "1732494960".to_string(),
                value: 110421854037.33331,
            },
        ];

        assert_eq!(timeseries, expected);
    }

    #[test]
    fn test_utc_system_timeseries_parse_success() {
        let log = log();
        let data =
            "{\"time\":\"2024-11-25T00:34:00Z\",\"value\":110220450825.75238}
{\"time\":\"2024-11-25T00:35:00Z\",\"value\":110339992917.33333}
{\"time\":\"2024-11-25T00:36:00Z\",\"value\":110421854037.33333}\n"
                .as_bytes();
        let timeseries = SystemTimeSeries::parse(&log, data).unwrap();

        let expected = vec![
            SystemTimeSeries {
                time: "2024-11-25T00:34:00Z".to_string(),
                value: 110220450825.75238,
            },
            SystemTimeSeries {
                time: "2024-11-25T00:35:00Z".to_string(),
                value: 110339992917.33331,
            },
            SystemTimeSeries {
                time: "2024-11-25T00:36:00Z".to_string(),
                value: 110421854037.33331,
            },
        ];

        assert_eq!(timeseries, expected);
    }

    #[test]
    fn test_misshapen_system_timeseries_parse_fail() {
        let log = log();
        let data = "{\"bob\":\"1732494720\",\"value\":110220450825.75238}\n"
            .as_bytes();
        let result = SystemTimeSeries::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "missing field `time` at line 1 column 47",
        );
    }

    #[test]
    fn test_time_format_system_timeseries_parse_fail() {
        let log = log();
        let data = "{\"time\":2024,\"value\":110220450825.75238}\n".as_bytes();
        let result = SystemTimeSeries::parse(&log, data);

        let error = result.unwrap_err();
        let root_cause = error.root_cause();

        assert_eq!(
            format!("{}", root_cause),
            "invalid type: integer `2024`, expected a string at line 1 column 12",
        );
    }
}
