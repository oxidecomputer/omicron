use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::time::Duration;

use chrono::{DateTime, Utc};

pub(crate) type TimeseriesName = String;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Query {
    pub(crate) ops: Vec<TableOp>,
}

impl Query {
    pub(crate) fn all_gets_at_query_start(&self) -> bool {
        fn valid(ops: &[TableOp]) -> bool {
            let Some((head, tail)) = ops.split_first() else {
                return false;
            };
            match head {
                TableOp::Basic(BasicTableOp::Get(_)) => {
                    !tail.iter().any(|op| {
                        matches!(op, TableOp::Basic(BasicTableOp::Get(_)))
                    })
                }
                TableOp::Basic(_) => false,
                TableOp::Grouped(grouped) => {
                    grouped.ops.iter().all(|query| valid(&query.ops))
                }
            }
        }
        valid(&self.ops)
    }

    pub(crate) fn all_timeseries_names(self) -> Vec<String> {
        fn collect(query: Query, output: &mut Vec<String>) {
            for op in query.ops {
                match op {
                    TableOp::Basic(BasicTableOp::Get(name)) => {
                        output.push(name)
                    }
                    TableOp::Grouped(grouped) => {
                        for query in grouped.ops {
                            collect(query, output);
                        }
                    }
                    TableOp::Basic(_) => {}
                }
            }
        }

        let mut output = Vec::new();
        collect(self, &mut output);
        output
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TableOp {
    Basic(BasicTableOp),
    Grouped(GroupedTableOp),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BasicTableOp {
    Get(TimeseriesName),
    Filter(Filter),
    GroupBy(GroupBy),
    Join(Join),
    Align(Align),
    Limit(Limit),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GroupedTableOp {
    pub(crate) ops: Vec<Query>,
}

#[allow(dead_code)]
pub(crate) struct Get {
    pub(crate) timeseries_name: TimeseriesName,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Filter {
    pub(crate) negated: bool,
    pub(crate) expr: FilterExpr,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FilterExpr {
    Simple(SimpleFilter),
    Compound(CompoundFilter),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SimpleFilter {
    pub(crate) ident: Ident,
    pub(crate) cmp: Comparison,
    pub(crate) value: Literal,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CompoundFilter {
    pub(crate) left: Box<Filter>,
    pub(crate) op: LogicalOp,
    pub(crate) right: Box<Filter>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Ident(pub(crate) String);

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Comparison {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Like,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum LogicalOp {
    And,
    Or,
    Xor,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Literal {
    Integer(i128),
    Double(f64),
    String(String),
    Boolean(bool),
    Uuid(String),
    Duration(Duration),
    Timestamp(DateTime<Utc>),
    IpAddr(IpAddr),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Align {
    pub(crate) method: AlignmentMethod,
    pub(crate) period: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum AlignmentMethod {
    Interpolate,
    MeanWithin,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GroupBy {
    pub(crate) identifiers: Vec<Ident>,
    pub(crate) reducer: Reducer,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) enum Reducer {
    #[default]
    Mean,
    Sum,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Join;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Limit {
    pub(crate) kind: LimitKind,
    pub(crate) count: NonZeroUsize,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum LimitKind {
    First,
    Last,
}

pub(crate) mod duration_consts {
    use std::time::Duration;

    pub(crate) const YEAR: Duration = Duration::from_secs(365 * 24 * 60 * 60);
    pub(crate) const MONTH: Duration = Duration::from_secs(30 * 24 * 60 * 60);
    pub(crate) const WEEK: Duration = Duration::from_secs(7 * 24 * 60 * 60);
    pub(crate) const DAY: Duration = Duration::from_secs(24 * 60 * 60);
    pub(crate) const HOUR: Duration = Duration::from_secs(60 * 60);
    pub(crate) const MINUTE: Duration = Duration::from_secs(60);
    pub(crate) const SECOND: Duration = Duration::from_secs(1);
    pub(crate) const MILLISECOND: Duration = Duration::from_millis(1);
    pub(crate) const MICROSECOND: Duration = Duration::from_micros(1);
    pub(crate) const NANOSECOND: Duration = Duration::from_nanos(1);
}
