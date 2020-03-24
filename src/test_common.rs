/*!
 * Common utilities used by unit tests.
 * TODO-cleanup Is there a better pattern in Rust for where to put code like
 * this?
 */

use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use std::fs;
use std::iter::Iterator;

/**
 * Represents a Bunyan log record.  This form does not support any non-standard
 * fields.  "level" is not yet supported because we don't (yet) need it.
 */
#[derive(Deserialize)]
pub struct BunyanLogRecord {
    pub time: DateTime<Utc>,
    pub name: String,
    pub hostname: String,
    pub pid: u32,
    pub msg: String,
    pub v: usize,
}

/**
 * Read a file containing a Bunyan-format log, returning an array of records.
 */
pub fn read_bunyan_log(logpath: &str) -> Vec<BunyanLogRecord> {
    let log_contents = fs::read_to_string(logpath).unwrap();
    let log_records = log_contents
        .split("\n")
        .filter(|line| line.len() > 0)
        .map(|line| serde_json::from_str::<BunyanLogRecord>(line).unwrap())
        .collect::<Vec<BunyanLogRecord>>();
    log_records
}

/**
 * Analogous to a BunyanLogRecord, but where all fields are optional.
 */
pub struct BunyanLogRecordSpec {
    pub name: Option<String>,
    pub hostname: Option<String>,
    pub pid: Option<u32>,
    pub v: Option<usize>,
}

/**
 * Verify that the key fields of the log records emitted by `iter` match the
 * corresponding values in `expected`.  Fields that are `None` in `expected`
 * will not be checked.
 */
pub fn verify_bunyan_records<'a, 'b, I>(
    iter: I,
    expected: &'a BunyanLogRecordSpec,
) where
    I: Iterator<Item = &'b BunyanLogRecord>,
{
    for record in iter {
        if let Some(ref expected_name) = expected.name {
            assert_eq!(expected_name, &record.name);
        }
        if let Some(ref expected_hostname) = expected.hostname {
            assert_eq!(expected_hostname, &record.hostname);
        }
        if let Some(expected_pid) = expected.pid {
            assert_eq!(expected_pid, record.pid);
        }
        if let Some(expected_v) = expected.v {
            assert_eq!(expected_v, record.v);
        }
    }
}

/**
 * Verify that the Bunyan records emitted by `iter` are chronologically
 * sequential and after `maybe_time_before` and before `maybe_time_after`, if
 * those latter two parameters are specified.
 */
pub fn verify_bunyan_records_sequential<'a, 'b, I>(
    iter: I,
    maybe_time_before: Option<&'a DateTime<Utc>>,
    maybe_time_after: Option<&'a DateTime<Utc>>,
) where
    I: Iterator<Item = &'a BunyanLogRecord>,
{
    let mut maybe_should_be_before = maybe_time_before;

    for record in iter {
        if let Some(should_be_before) = maybe_should_be_before {
            assert!(should_be_before.timestamp() <= record.time.timestamp());
        }
        maybe_should_be_before = Some(&record.time);
    }

    if let Some(should_be_before) = maybe_should_be_before {
        if let Some(time_after) = maybe_time_after {
            assert!(should_be_before.timestamp() <= time_after.timestamp());
        }
    }
}

#[cfg(test)]
mod test {
    const T1_STR: &str = "2020-03-24T00:00:00Z";
    const T2_STR: &str = "2020-03-25T00:00:00Z";

    use super::verify_bunyan_records_sequential;
    use super::verify_bunyan_records;
    use super::BunyanLogRecord;
    use super::BunyanLogRecordSpec;
    use chrono::DateTime;
    use chrono::Utc;

    fn make_dummy_record() -> BunyanLogRecord {
        let t1: DateTime<Utc> =
            DateTime::parse_from_rfc3339(T1_STR).unwrap().into();
        BunyanLogRecord {
            time: t1.into(),
            name: "n1".to_string(),
            hostname: "h1".to_string(),
            pid: 1,
            msg: "msg1".to_string(),
            v: 0,
        }
    }

    /*
     * Tests various cases where verify_bunyan_records() should not panic.
     */
    #[test]
    fn test_bunyan_easy_cases() {
        let t1: DateTime<Utc> =
            DateTime::parse_from_rfc3339(T1_STR).unwrap().into();
        let r1 = make_dummy_record();
        let r2 = BunyanLogRecord {
            time: t1.into(),
            name: "n1".to_string(),
            hostname: "h2".to_string(),
            pid: 1,
            msg: "msg2".to_string(),
            v: 1
        };

        /* Test case: nothing to check. */
        let records: Vec<&BunyanLogRecord> = vec![ &r1 ];
        let iter = records.iter().map(|x| *x);
        verify_bunyan_records(iter, &BunyanLogRecordSpec {
            name: None,
            hostname: None,
            pid: None,
            v: None
        });

        /* Test case: check name, no problem. */
        let records: Vec<&BunyanLogRecord> = vec![ &r1 ];
        let iter = records.iter().map(|x| *x);
        verify_bunyan_records(iter, &BunyanLogRecordSpec {
            name: Some("n1".to_string()),
            hostname: None,
            pid: None,
            v: None
        });

        /* Test case: check hostname, no problem. */
        let records: Vec<&BunyanLogRecord> = vec![ &r1 ];
        let iter = records.iter().map(|x| *x);
        verify_bunyan_records(iter, &BunyanLogRecordSpec {
            name: None,
            hostname: Some("h1".to_string()),
            pid: None,
            v: None
        });

        /* Test case: check pid, no problem. */
        let records: Vec<&BunyanLogRecord> = vec![ &r1 ];
        let iter = records.iter().map(|x| *x);
        verify_bunyan_records(iter, &BunyanLogRecordSpec {
            name: None,
            hostname: None,
            pid: Some(1),
            v: None
        });

        /* Test case: check hostname, no problem. */
        let records: Vec<&BunyanLogRecord> = vec![ &r1 ];
        let iter = records.iter().map(|x| *x);
        verify_bunyan_records(iter, &BunyanLogRecordSpec {
            name: None,
            hostname: None,
            pid: None,
            v: Some(0),
        });

        /* Test case: check all, no problem. */
        let records: Vec<&BunyanLogRecord> = vec![ &r1 ];
        let iter = records.iter().map(|x| *x);
        verify_bunyan_records(iter, &BunyanLogRecordSpec {
            name: Some("n1".to_string()),
            hostname: Some("h1".to_string()),
            pid: Some(1),
            v: Some(0),
        });

        /* Test case: check multiple records, no problem. */
        let records: Vec<&BunyanLogRecord> = vec![ &r1, &r2 ];
        let iter = records.iter().map(|x| *x);
        verify_bunyan_records(iter, &BunyanLogRecordSpec {
            name: Some("n1".to_string()),
            hostname: None,
            pid: Some(1),
            v: None,
        });
    }

    /*
     * Test cases exercising violations of each of the fields.
     */

    #[test]
    #[should_panic(expected = "assertion failed")]
    fn test_bunyan_bad_name() {
        let r1 = make_dummy_record();
        let records: Vec<&BunyanLogRecord> = vec![ &r1 ];
        let iter = records.iter().map(|x| *x);
        verify_bunyan_records(iter, &BunyanLogRecordSpec {
            name: Some("n2".to_string()),
            hostname: None,
            pid: None,
            v: None,
        });
    }

    #[test]
    #[should_panic(expected = "assertion failed")]
    fn test_bunyan_bad_hostname() {
        let r1 = make_dummy_record();
        let records: Vec<&BunyanLogRecord> = vec![ &r1 ];
        let iter = records.iter().map(|x| *x);
        verify_bunyan_records(iter, &BunyanLogRecordSpec {
            name: None,
            hostname: Some("h2".to_string()),
            pid: None,
            v: None,
        });
    }

    #[test]
    #[should_panic(expected = "assertion failed")]
    fn test_bunyan_bad_pid() {
        let r1 = make_dummy_record();
        let records: Vec<&BunyanLogRecord> = vec![ &r1 ];
        let iter = records.iter().map(|x| *x);
        verify_bunyan_records(iter, &BunyanLogRecordSpec {
            name: None,
            hostname: None,
            pid: Some(2),
            v: None,
        });
    }

    #[test]
    #[should_panic(expected = "assertion failed")]
    fn test_bunyan_bad_v() {
        let r1 = make_dummy_record();
        let records: Vec<&BunyanLogRecord> = vec![ &r1 ];
        let iter = records.iter().map(|x| *x);
        verify_bunyan_records(iter, &BunyanLogRecordSpec {
            name: None,
            hostname: None,
            pid: None,
            v: Some(1),
        });
    }

    /*
     * These cases exercise 0, 1, and 2 records with every valid combination
     * of lower and upper bounds.
     */
    #[test]
    fn test_bunyan_seq_easy_cases() {
        let t1: DateTime<Utc> =
            DateTime::parse_from_rfc3339(T1_STR).unwrap().into();
        let t2: DateTime<Utc> =
            DateTime::parse_from_rfc3339(T2_STR).unwrap().into();
        let v0: Vec<BunyanLogRecord> = vec![];
        let v1: Vec<BunyanLogRecord> = vec![BunyanLogRecord {
            time: t1.into(),
            name: "dummy_name".to_string(),
            hostname: "dummy_hostname".to_string(),
            pid: 123,
            msg: "dummy_msg".to_string(),
            v: 0,
        }];
        let v2: Vec<BunyanLogRecord> = vec![
            BunyanLogRecord {
                time: t1.into(),
                name: "dummy_name".to_string(),
                hostname: "dummy_hostname".to_string(),
                pid: 123,
                msg: "dummy_msg".to_string(),
                v: 0,
            },
            BunyanLogRecord {
                time: t2.into(),
                name: "dummy_name".to_string(),
                hostname: "dummy_hostname".to_string(),
                pid: 123,
                msg: "dummy_msg".to_string(),
                v: 0,
            },
        ];

        verify_bunyan_records_sequential(v0.iter(), None, None);
        verify_bunyan_records_sequential(v0.iter(), Some(&t1), None);
        verify_bunyan_records_sequential(v0.iter(), None, Some(&t1));
        verify_bunyan_records_sequential(v0.iter(), Some(&t1), Some(&t2));
        verify_bunyan_records_sequential(v1.iter(), None, None);
        verify_bunyan_records_sequential(v1.iter(), Some(&t1), None);
        verify_bunyan_records_sequential(v1.iter(), None, Some(&t2));
        verify_bunyan_records_sequential(v1.iter(), Some(&t1), Some(&t2));
        verify_bunyan_records_sequential(v2.iter(), None, None);
        verify_bunyan_records_sequential(v2.iter(), Some(&t1), None);
        verify_bunyan_records_sequential(v2.iter(), None, Some(&t2));
        verify_bunyan_records_sequential(v2.iter(), Some(&t1), Some(&t2));
    }

    /*
     * Test case: no records, but the bounds themselves violate the constraint.
     */
    #[test]
    #[should_panic(expected = "assertion failed: should_be_before")]
    fn test_bunyan_seq_bounds_bad() {
        let t1: DateTime<Utc> =
            DateTime::parse_from_rfc3339(T1_STR).unwrap().into();
        let t2: DateTime<Utc> =
            DateTime::parse_from_rfc3339(T2_STR).unwrap().into();
        let v0: Vec<BunyanLogRecord> = vec![];
        verify_bunyan_records_sequential(v0.iter(), Some(&t2), Some(&t1));
    }

    /*
     * Test case: sole record appears before early bound.
     */
    #[test]
    #[should_panic(expected = "assertion failed: should_be_before")]
    fn test_bunyan_seq_lower_violated() {
        let t1: DateTime<Utc> =
            DateTime::parse_from_rfc3339(T1_STR).unwrap().into();
        let t2: DateTime<Utc> =
            DateTime::parse_from_rfc3339(T2_STR).unwrap().into();
        let v1: Vec<BunyanLogRecord> = vec![BunyanLogRecord {
            time: t1.into(),
            name: "dummy_name".to_string(),
            hostname: "dummy_hostname".to_string(),
            pid: 123,
            msg: "dummy_msg".to_string(),
            v: 0,
        }];
        verify_bunyan_records_sequential(v1.iter(), Some(&t2), None);
    }

    /*
     * Test case: sole record appears after late bound.
     */
    #[test]
    #[should_panic(expected = "assertion failed: should_be_before")]
    fn test_bunyan_seq_upper_violated() {
        let t1: DateTime<Utc> =
            DateTime::parse_from_rfc3339(T1_STR).unwrap().into();
        let t2: DateTime<Utc> =
            DateTime::parse_from_rfc3339(T2_STR).unwrap().into();
        let v1: Vec<BunyanLogRecord> = vec![BunyanLogRecord {
            time: t2.into(),
            name: "dummy_name".to_string(),
            hostname: "dummy_hostname".to_string(),
            pid: 123,
            msg: "dummy_msg".to_string(),
            v: 0,
        }];
        verify_bunyan_records_sequential(v1.iter(), None, Some(&t1));
    }

    /*
     * Test case: two records out of order.
     */
    #[test]
    #[should_panic(expected = "assertion failed: should_be_before")]
    fn test_bunyan_seq_bad_order() {
        let t1: DateTime<Utc> =
            DateTime::parse_from_rfc3339(T1_STR).unwrap().into();
        let t2: DateTime<Utc> =
            DateTime::parse_from_rfc3339(T2_STR).unwrap().into();
        let v2: Vec<BunyanLogRecord> = vec![
            BunyanLogRecord {
                time: t2.into(),
                name: "dummy_name".to_string(),
                hostname: "dummy_hostname".to_string(),
                pid: 123,
                msg: "dummy_msg".to_string(),
                v: 0,
            },
            BunyanLogRecord {
                time: t1.into(),
                name: "dummy_name".to_string(),
                hostname: "dummy_hostname".to_string(),
                pid: 123,
                msg: "dummy_msg".to_string(),
                v: 0,
            },
        ];
        verify_bunyan_records_sequential(v2.iter(), None, None);
    }
}
