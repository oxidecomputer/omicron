// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Grammar for the Oximeter Query Language (OxQL).

// Copyright 2024 Oxide Computer

peg::parser! {
    pub grammar query_parser() for str {
        use crate::oxql::ast::cmp::Comparison;
        use crate::oxql::ast::table_ops::align::Align;
        use crate::oxql::ast::table_ops::align::AlignmentMethod;
        use crate::oxql::ast::table_ops::filter::SimpleFilter;
        use crate::oxql::ast::table_ops::filter::FilterExpr;
        use crate::oxql::ast::table_ops::filter::Filter;
        use crate::oxql::ast::table_ops::filter::CompoundFilter;
        use crate::oxql::ast::table_ops::get::Get;
        use crate::oxql::ast::table_ops::group_by::GroupBy;
        use crate::oxql::ast::ident::Ident;
        use crate::oxql::ast::literal::Literal;
        use crate::oxql::ast::logical_op::LogicalOp;
        use crate::oxql::ast::Query;
        use crate::oxql::ast::table_ops::join::Join;
        use crate::oxql::ast::table_ops::GroupedTableOp;
        use crate::oxql::ast::table_ops::BasicTableOp;
        use crate::oxql::ast::table_ops::TableOp;
        use crate::oxql::ast::table_ops::group_by::Reducer;
        use crate::oxql::ast::table_ops::limit::Limit;
        use crate::oxql::ast::table_ops::limit::LimitKind;
        use crate::oxql::ast::literal::duration_consts;
        use oximeter::TimeseriesName;
        use std::time::Duration;
        use uuid::Uuid;
        use chrono::Utc;
        use chrono::DateTime;
        use chrono::NaiveDateTime;
        use chrono::NaiveDate;
        use chrono::NaiveTime;
        use std::net::IpAddr;
        use std::net::Ipv4Addr;
        use std::net::Ipv6Addr;

        rule _ = quiet!{[' ' | '\n' | '\t']+} / expected!("whitespace")

        // Parse boolean literals.
        rule true_literal() -> bool = "true" { true }
        rule false_literal() -> bool = "false" { false }
        pub(super) rule boolean_literal_impl() -> bool
            = quiet! { true_literal() / false_literal() } / expected!("boolean literal")

        pub rule boolean_literal() -> Literal
            = b:boolean_literal_impl() { Literal::Boolean(b) }

        // Parse duration literals.
        rule year() -> Duration
            = "Y" { duration_consts::YEAR }
        rule month() -> Duration
            = "M" { duration_consts::MONTH }
        rule week() -> Duration
            = "w" { duration_consts::WEEK }
        rule day() -> Duration
            = "d" { duration_consts::DAY }
        rule hour() -> Duration
            = "h" { duration_consts::HOUR }
        rule minute() -> Duration
            = "m" { duration_consts::MINUTE }
        rule second() -> Duration
            = "s" { duration_consts::SECOND }
        rule millisecond() -> Duration
            = "ms" { duration_consts::MILLISECOND }
        rule microsecond() -> Duration
            = "us" { duration_consts::MICROSECOND }
        rule nanosecond() -> Duration
            = "ns" { duration_consts::NANOSECOND }
        pub(super) rule duration_literal_impl() -> Duration
            = count:integer_literal_impl() base:(
                year() /
                month() /
                week() / day() /
                hour() /
                millisecond() /
                minute() /
                second() /
                microsecond() /
                nanosecond()
            )
        {?
            // NOTE: This count is the factor by which we multiply the base
            // unit. So it counts the number of nanos, millis, or days, etc. It
            // does not limit the total duration itself.
            let Ok(count) = u32::try_from(count) else {
                return Err("invalid count for duration literal");
            };
            base.checked_mul(count).ok_or("overflowed duration literal")
        }

        /// Parse a literal duration from a string.
        ///
        /// Durations are written as a positive integer multiple of a base time
        /// unit. For example, `7s` is interpreted as 7 seconds. Supported units
        /// are:
        ///
        /// - 'y': an approximate year, 365 days
        /// - 'M': an approximate month, 30 days
        /// - 'w': an approximate week, 7 days
        /// - 'h': an hour, 3600 seconds
        /// - 'm': a minute, 60 seconds
        /// - 's': seconds
        /// - 'ms': milliseconds
        /// - 'us': microseconds
        /// - 'ns': nanoseconds
        pub rule duration_literal() -> Literal
            = d:duration_literal_impl() { Literal::Duration(d) }

        /// Parse a literal timestamp.
        ///
        /// Timestamps are literals prefixed with `@`. They can be in one of
        /// several formats:
        ///
        /// - YYYY-MM-DD
        /// - HH:MM:SS[.f]
        /// - RFC 3339, `YYYY-MM-DDTHH:MM:SS.f`
        /// - The literal `now()`, possibly with some simple offset expression,
        /// such as `now() - 5m`. The offset must be a duration.
        ///
        /// All timestamps are in UTC.
        pub rule timestamp_literal() -> Literal
            = t:timestamp_literal_impl() { Literal::Timestamp(t) }

        rule timestamp_literal_impl() -> DateTime<Utc>
            = timestamp_string()
            / now_timestamp()

        pub(super) rule timestamp_string() -> DateTime<Utc>
            = "@" s:$(['0'..='9' | '-' | 'T' | ':' | '.']+)
        {?
            if let Ok(t) = NaiveDate::parse_from_str(s, "%F") {
                return Ok(t.and_hms_opt(0, 0, 0).unwrap().and_utc());
            }
            if let Ok(t) = NaiveTime::parse_from_str(s, "%H:%M:%S%.f") {
                return Ok(NaiveDateTime::new(Utc::now().date_naive(), t).and_utc());
            }
            if let Ok(t) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
                return Ok(t.and_utc());
            }
            Err("a recognized timestamp format")
        }

        rule now_offset() -> (bool, Duration)
            = _? sign:['+' | '-'] _? dur:duration_literal_impl()
        {
            let negative = matches!(sign, '-');
            (negative, dur)
        }

        pub(super) rule now_timestamp() -> DateTime<Utc>
            = "@now()" maybe_offset:now_offset()?
        {
            let now = Utc::now();
            if let Some((negative, offset)) = maybe_offset {
                if negative {
                    now - offset
                } else {
                    now + offset
                }
            } else {
                now
            }
        }

        /// Parse an IP address literal, either IPv4 or IPv6
        pub rule ip_literal() -> Literal
            = ip:ipv4_literal() { Literal::IpAddr(IpAddr::V4(ip)) }
            / ip:ipv6_literal() { Literal::IpAddr(IpAddr::V6(ip)) }

        pub(super) rule ipv4_literal() -> Ipv4Addr
            = "\"" s:$((['0'..='9']*<1,3>)**<4> ".") "\""
        {?
            s.parse().map_err(|_| "an IPv4 address")
        }

        pub(super) rule ipv6_literal() -> Ipv6Addr
            = "\"" s:$(['a'..='f' | '0'..='9' | ':']+) "\""
        {?
            s.parse().map_err(|_| "an IPv6 address")
        }

        rule dashed_uuid_literal() -> Uuid
            = s:$(
                "\""
                ['a'..='f' | 'A'..='F' | '0'..='9']*<8> "-"
                ['a'..='f' | 'A'..='F' | '0'..='9']*<4> "-"
                ['a'..='f' | 'A'..='F' | '0'..='9']*<4> "-"
                ['a'..='f' | 'A'..='F' | '0'..='9']*<4> "-"
                ['a'..='f' | 'A'..='F' | '0'..='9']*<12>
                "\""
            ) {?
                let Some(middle) = s.get(1..37) else {
                    return Err("invalid UUID literal");
                };
                middle.parse().or(Err("invalid UUID literal"))
            }
        rule undashed_uuid_literal() -> Uuid
            = s:$("\"" ['a'..='f' | 'A'..='F' | '0'..='9']*<32> "\"") {?
            let Some(middle) = s.get(1..33) else {
                return Err("invalid UUID literal");
            };
            middle.parse().or(Err("invalid UUID literal"))
        }
        pub(super) rule uuid_literal_impl() -> Uuid
            = dashed_uuid_literal() / undashed_uuid_literal()

        /// Parse UUID literals.
        ///
        /// UUIDs should be quoted with `"` and can include or omit dashes
        /// between the segments. Both of the following are equivalent.
        ///
        /// "fc59ab26-f1d8-44ca-abbc-dd8f61321433"
        /// "fc59ab26f1d844caabbcdd8f61321433"
        pub rule uuid_literal() -> Literal
            = id:uuid_literal_impl() { Literal::Uuid(id) }

        // Parse string literals.
        rule any_but_single_quote() -> String
            = s:$([^'\'']*)
        {?
            recognize_escape_sequences(s).ok_or("invalid single quoted string")
        }

        rule any_but_double_quote() -> String
            = s:$([^'"']*)
        {?
            recognize_escape_sequences(s).ok_or("invalid double quoted string")
        }

        rule single_quoted_string() -> String
            = "'" s:any_but_single_quote() "'" { s }

        rule double_quoted_string() -> String
            = "\"" s:any_but_double_quote() "\"" { s }

        pub(super) rule string_literal_impl() -> String
            = single_quoted_string() / double_quoted_string()

        /// Parse a string literal, either single- or double-quoted.
        ///
        /// Parsing string literals is pretty tricky, but we add several
        /// constraints to simplify things. First strings must be quoted, either
        /// with single- or double-quotes. E.g., the strings `"this"` and
        /// `'this'` parse the same way.
        ///
        /// We require that the string not _contain_ its quote-style, so there
        /// can't be any embedded single-quotes in a single-quoted string, or
        /// double-quotes in a double-quoted string. Each quote-style may contain
        /// the quote from the other style.
        ///
        /// We support the following common escape sequences:
        ///
        /// ```text
        /// \n
        /// \r
        /// \t
        /// \\
        /// \0
        /// ```
        ///
        /// Beyond this, any valid Unicode code point, written in the usual Rust
        /// style, is supported. For example, `\u{1234}` is accepted and mapped
        /// to `ሴ` upon parsing. This also allows users to write both quote
        /// styles if required, by writing them as their Unicode escape
        /// sequences. For example, this string:
        ///
        /// ```text
        /// "this string has \u{22} in it"
        /// ```
        ///
        /// Will be parsed as `this string has " in it`.
        pub rule string_literal() -> Literal
            = s:string_literal_impl() { Literal::String(s) }

        pub(super) rule hex_integer_literal_impl() -> i128
            = n:$("0x" ['0'..='9' | 'a'..='f' | 'A'..='F']+ !['.'])
        {?
            let Some((maybe_sign, digits)) = n.split_once("0x") else {
                return Err("hex literals should start with '0x'");
            };
            i128::from_str_radix(digits, 16).map_err(|_| "invalid hex literal")
        }

        pub(super) rule dec_integer_literal_impl() -> i128
            = n:$(['0'..='9']+ !['e' | 'E' | '.'])
        {?
            n.parse().map_err(|_| "integer literal")
        }

        pub(super) rule integer_literal_impl() -> i128
            = maybe_sign:$("-"?) n:(hex_integer_literal_impl() / dec_integer_literal_impl())
        {?
            let sign = if maybe_sign == "-" { -1 } else { 1 };
            let Some(x) = n.checked_mul(sign) else {
                return Err("negative overflow");
            };
            if x < i128::from(i64::MIN) {
                Err("negative overflow")
            } else if x > i128::from(u64::MAX) {
                Err("positive overflow")
            } else {
                Ok(x)
            }
        }

        /// Parse integer literals.
        pub rule integer_literal() -> Literal
            = n:integer_literal_impl() { Literal::Integer(n) }

        // We're being a bit lazy here, since the rule expression isn't exactly
        // right. But we rely on calling `f64`'s `FromStr` implementation to
        // actually verify the values can be parsed.
        pub(super) rule double_literal_impl() -> f64
            = n:$("-"? ['0'..='9']* "."? ['0'..='9']* (['e' | 'E'] "-"?  ['0'..='9']+)*) {?
                n.parse().or(Err("double literal"))
            }

        // Parse double literals.
        pub rule double_literal() -> Literal
            = d:double_literal_impl() { Literal::Double(d) }

        /// Parse a literal.
        ///
        /// Literals are typed, with support for bools, durations, integers and
        /// doubles, UUIDs, and general strings. See the rules for each type of
        /// literal for details on supported formats.
        pub rule literal() -> Literal
            = lit:(
                boolean_literal() /
                duration_literal() /
                integer_literal() /
                double_literal() /
                uuid_literal() /
                ip_literal() /
                string_literal() /
                timestamp_literal()
            )
        {
            lit
        }

        /// Parse a logical operator.
        pub(super) rule logical_op_impl() -> LogicalOp
            = "||" { LogicalOp::Or}
            / "&&" { LogicalOp::And }
            / "^" { LogicalOp::Xor }


        // NOTES:
        //
        // The rules below are all used to parse a filtering expression. This
        // turns out to be surprisingly complicated to express succinctly in
        // `peg`, but there are a few tricks. First, it's important that we do
        // not try to parse negation ("!") inside the filtering atoms -- it's a
        // higher-level concept, and not part of the atom itself.
        //
        // Second, it's not clear how to use `peg`'s precendence macro to
        // correctly describe the precedence. Things are recursive, but we
        // choose to define that in the rules themselves, rather than explicitly
        // with precedence levels. This is common in PEG definitions, and the
        // main trick is force things _not_ to be left-recursive, and use two
        // rules tried in sequence. The `factor` rule is a good example of this.
        //
        // Another example is the logical OR / AND / XOR parsing. We start with
        // OR, which is the lowest precedence, and move to the others in
        // sequence. Each is defined as parsing either the "thing itself", e.g.,
        // `foo || bar` for the OR rule; or the rule with next-higher
        // precedence.
        //
        // IMPORTANT: The #[cache] directives on the rules below are _critical_
        // to avoiding wildly exponential runtime with nested expressions.

        /// Parse a logical negation
        pub rule not() = "!"

        /// A factor is a logically negated expression, or a primary expression.
        #[cache]
        pub rule factor() -> Filter
            = not() _? factor:factor()
        {
            Filter {
                negated: !factor.negated,
                expr: factor.expr
            }
        }
            / p:primary() { p }

        /// A primary expression is either a comparison "atom", e.g., `foo ==
        /// "bar"`, or a grouping around a sequence of such things.
        #[cache]
        pub rule primary() -> Filter
            = atom:comparison_atom()
        {?
            if matches!(atom.cmp, Comparison::Like) && !matches!(atom.value, Literal::String(_)) {
                Err("~= comparison is only supported for string literals")
            } else {
                Ok(Filter { negated: false, expr: FilterExpr::Simple(atom) })
            }
        }
            / "(" _? or:logical_or_expr() _? ")" { or }

        /// A comparison atom is a base-case for all this recursion.
        ///
        /// It specifies a single comparison between an identifier and a value,
        /// using a specific comparison operator. For example, this parses `foo
        /// == "bar"`.
        pub rule comparison_atom() -> SimpleFilter
            = ident:ident() _? cmp:comparison() _? value:literal()
        {
            SimpleFilter { ident, cmp, value }
        }

        /// Two filtering expressions combined with a logical OR.
        ///
        /// An OR expression is two logical ANDs joined with "||", or just a
        /// bare logical AND expression.
        #[cache]
        pub rule logical_or_expr() -> Filter
            = left:logical_and_expr() _? "||" _? right:logical_or_expr()
        {
            let compound = CompoundFilter {
                left: Box::new(left),
                op: LogicalOp::Or,
                right: Box::new(right),
            };
            Filter { negated: false, expr: FilterExpr::Compound(compound) }
        }
            / logical_and_expr()

        /// Two filtering expressions combined with a logical AND.
        ///
        /// A logical AND expression is two logical XORs joined with "&&", or
        /// just a bare logical XOR expression.
        #[cache]
        pub rule logical_and_expr() -> Filter
            = left:logical_xor_expr() _? "&&" _? right:logical_and_expr()
        {
            let compound = CompoundFilter {
                left: Box::new(left),
                op: LogicalOp::And,
                right: Box::new(right),
            };
            Filter { negated: false, expr: FilterExpr::Compound(compound) }
        }
            / logical_xor_expr()

        /// Two filtering expressions combined with a logical XOR.
        ///
        /// A logical XOR expression is two logical XORs joined with "^ or
        /// just a bare factor. Note that this either hits the base case, if
        /// `factor` is actually an atom, or recurses again if its a logical OR
        /// expression.
        ///
        /// Note that this is the highest-precedence logical operator.
        #[cache]
        pub rule logical_xor_expr() -> Filter
            = left:factor() _? "^" _? right:logical_xor_expr()
        {
            let compound = CompoundFilter {
                left: Box::new(left),
                op: LogicalOp::Xor,
                right: Box::new(right),
            };
            Filter { negated: false, expr: FilterExpr::Compound(compound) }
        }
            / factor:factor() { factor }

        /// Parse the _logical expression_ part of a `filter` table operation.
        pub rule filter_expr() -> Filter = logical_or_expr()

        /// Parse a "filter" table operation.
        pub rule filter() -> Filter
            = "filter" _ expr:filter_expr() _?
        {
            expr
        }

        pub(super) rule ident_impl() -> &'input str
            = quiet!{ inner:$(['a'..='z']+ ['a'..='z' | '0'..='9']* ("_" ['a'..='z' | '0'..='9']+)*) } /
                expected!("A valid identifier")

        /// Parse an identifier, usually a column name.
        pub rule ident() -> Ident
            = inner:ident_impl() { Ident(inner.to_string()) }

        pub(super) rule comparison() -> Comparison
            = "==" { Comparison::Eq }
            / "!=" { Comparison::Ne }
            / ">=" { Comparison::Ge }
            / ">" { Comparison::Gt }
            / "<=" { Comparison::Le }
            / "<" { Comparison::Lt }
            / "~=" { Comparison::Like }

        pub rule timeseries_name() -> TimeseriesName
            = target_name:ident_impl() ":" metric_name:ident_impl()
        {?
            format!("{target_name}:{metric_name}")
                .try_into()
                .map_err(|_| "invalid timeseries name")
        }

        rule get_delim() = quiet!{ _? "," _? }

        /// Parse a "get" table operation.
        pub rule get() -> Vec<Get>
            = "get" _ names:(timeseries_name() **<1,> get_delim())
        {
            names.into_iter().map(|t| Get { timeseries_name: t }).collect()
        }

        /// Parse a reducing operation by name.
        pub rule reducer() -> Reducer
            = "mean" { Reducer::Mean }
            / "sum" { Reducer::Sum }
            / expected!("a reducer name")

        rule ws_with_comma() = _? "," _?
        pub rule group_by() -> GroupBy
            = "group_by"
                _
                "[" _? identifiers:(ident() ** ws_with_comma()) ","? _? "]"
                reducer:("," _? red:reducer() { red })?
        {
            GroupBy {
                identifiers,
                reducer: reducer.unwrap_or_default(),
            }
        }

        /// Parse a `join` table operation.
        pub rule join() = "join" {}

        pub(super) rule alignment_method() -> AlignmentMethod
            = "interpolate" { AlignmentMethod::Interpolate }
            / "mean_within" { AlignmentMethod::MeanWithin }
            / "rate" { AlignmentMethod::Rate }
            / "min" { AlignmentMethod::Min }
            / "max" { AlignmentMethod::Max }

        /// Parse an alignment table operation.
        pub rule align() -> Align
            = "align" _ method:alignment_method() "(" period:duration_literal_impl() ")"
        {
            Align { method, period }
        }

        /// Parse a limit kind
        pub rule limit_kind() -> LimitKind
            = "first" { LimitKind::First }
            / "last" { LimitKind::Last }

        /// Parse a limit table operation
        pub rule limit() -> Limit
            = kind:limit_kind() _ count:integer_literal_impl()
        {?
            if count <= 0 || count > usize::MAX as i128 {
                return Err("limit count must be a nonzero usize")
            };
            let count = std::num::NonZeroUsize::new(count.try_into().unwrap()).unwrap();
            Ok(Limit { kind, count })
        }

        pub(super) rule basic_table_op() -> TableOp
            = g:"get" _ t:timeseries_name() { TableOp::Basic(BasicTableOp::Get(t)) }
            / f:filter() { TableOp::Basic(BasicTableOp::Filter(f)) }
            / g:group_by() { TableOp::Basic(BasicTableOp::GroupBy(g)) }
            / join() { TableOp::Basic(BasicTableOp::Join(Join)) }
            / a:align() { TableOp::Basic(BasicTableOp::Align(a)) }
            / l:limit() { TableOp::Basic(BasicTableOp::Limit(l)) }

        pub(super) rule grouped_table_op() -> TableOp
            = "{" _? ops:(query() ++ grouped_table_op_delim()) _? "}"
        {
            TableOp::Grouped(GroupedTableOp { ops })
        }

        /// Parse a top-level OxQL query.
        ///
        /// Queries always start with a "get" operation, and may be followed by
        /// any number of other timeseries transformations
        pub rule query() -> Query
            = ops:(basic_table_op() / grouped_table_op()) ++ query_delim()
        {?
            let query = Query { ops };
            if query.all_gets_at_query_start() {
                Ok(query)
            } else {
                Err("every subquery must start with a `get` operation")
            }
        }

        rule grouped_table_op_delim() = quiet!{ _? ";" _? }
        rule query_delim() = quiet!{ _? "|" _? }
    }
}

// Recognize escape sequences and convert them into the intended Unicode point
// they represent.
//
// For example, the string containing ASCII "abcd" is returned unchanged.
//
// The string containing "\u{1234}" is returned as the string "ሴ". Note that the
// Unicode bytes must be enclosed in {}, and can have length 1-6.
//
// If the string contains an invalid escape sequence, such as "\uFFFF", or a
// control code, such as `\u07`, `None` is returned.
//
// Note that the main goal of this method is to _unescape_ relevant sequences.
// We will get queries that may contain escaped sequences, like `\\\n`, which
// this method will unescape to `\n`.
fn recognize_escape_sequences(s: &str) -> Option<String> {
    let mut out = String::with_capacity(s.len());

    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                let Some(next_ch) = chars.next() else {
                    // Escape at the end of the string
                    return None;
                };
                match next_ch {
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    '\\' => out.push('\\'),
                    '0' => out.push('\0'),
                    'u' => {
                        // We need this to be delimited by {}, and between 1 and
                        // 6 characters long.
                        if !matches!(chars.next(), Some('{')) {
                            return None;
                        }

                        let mut digits = String::with_capacity(6);
                        let mut found_closing_brace = false;
                        while !found_closing_brace && digits.len() < 7 {
                            // Take the next value, if it's a hex digit or the
                            // closing brace.
                            let Some(next) = chars.next_if(|ch| {
                                ch.is_ascii_hexdigit() || *ch == '}'
                            }) else {
                                break;
                            };
                            if next.is_ascii_hexdigit() {
                                digits.push(next);
                                continue;
                            }
                            found_closing_brace = true;
                        }
                        if !found_closing_brace {
                            return None;
                        }
                        let val = u32::from_str_radix(&digits, 16).ok()?;
                        let decoded = char::from_u32(val)?;
                        out.push(decoded)
                    }
                    _ => return None,
                }
            }
            _ => out.push(ch),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::query_parser;
    use crate::oxql::ast::cmp::Comparison;
    use crate::oxql::ast::grammar::recognize_escape_sequences;
    use crate::oxql::ast::ident::Ident;
    use crate::oxql::ast::literal::Literal;
    use crate::oxql::ast::logical_op::LogicalOp;
    use crate::oxql::ast::table_ops::align::Align;
    use crate::oxql::ast::table_ops::align::AlignmentMethod;
    use crate::oxql::ast::table_ops::filter::CompoundFilter;
    use crate::oxql::ast::table_ops::filter::Filter;
    use crate::oxql::ast::table_ops::filter::FilterExpr;
    use crate::oxql::ast::table_ops::filter::SimpleFilter;
    use crate::oxql::ast::table_ops::group_by::Reducer;
    use crate::oxql::ast::table_ops::limit::Limit;
    use crate::oxql::ast::table_ops::limit::LimitKind;
    use chrono::NaiveDate;
    use chrono::NaiveDateTime;
    use chrono::NaiveTime;
    use chrono::TimeZone;
    use chrono::Utc;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn test_boolean_literal() {
        assert_eq!(query_parser::boolean_literal_impl("true").unwrap(), true);
        assert_eq!(query_parser::boolean_literal_impl("false").unwrap(), false);
    }

    #[test]
    fn test_duration_literal() {
        for (as_str, dur) in [
            ("7Y", Duration::from_secs(60 * 60 * 24 * 365 * 7)),
            ("7M", Duration::from_secs(60 * 60 * 24 * 30 * 7)),
            ("7w", Duration::from_secs(60 * 60 * 24 * 7 * 7)),
            ("7d", Duration::from_secs(60 * 60 * 24 * 7)),
            ("7h", Duration::from_secs(60 * 60 * 7)),
            ("7m", Duration::from_secs(60 * 7)),
            ("7s", Duration::from_secs(7)),
            ("7ms", Duration::from_millis(7)),
            ("7us", Duration::from_micros(7)),
            ("7ns", Duration::from_nanos(7)),
        ] {
            assert_eq!(
                query_parser::duration_literal_impl(as_str).unwrap(),
                dur
            );
        }

        assert!(query_parser::duration_literal_impl("-1m").is_err());
        let too_big: i64 = i64::from(u32::MAX) + 1;
        assert!(
            query_parser::duration_literal_impl(&format!("{too_big}s"))
                .is_err()
        );
    }

    #[test]
    fn test_uuid_literal() {
        const ID: Uuid = uuid::uuid!("9f8900bd-886d-4988-b623-95b7fda36d23");
        let as_string = format!("\"{}\"", ID);
        assert_eq!(query_parser::uuid_literal_impl(&as_string).unwrap(), ID);
        let without_dashes = as_string.replace('-', "");
        assert_eq!(
            query_parser::uuid_literal_impl(&without_dashes).unwrap(),
            ID
        );

        assert!(
            query_parser::uuid_literal_impl(&as_string[1..as_string.len() - 2])
                .is_err()
        );
        assert!(
            query_parser::uuid_literal_impl(
                &without_dashes[1..without_dashes.len() - 2]
            )
            .is_err()
        );
    }

    #[test]
    fn test_uuid_literal_is_case_insensitive() {
        const ID: Uuid = uuid::uuid!("880D82A1-102F-4699-BE1A-7E2A6A469E8E");
        let as_str = format!("\"{ID}\"");
        let as_lower = as_str.to_lowercase();
        assert_eq!(query_parser::uuid_literal_impl(&as_str).unwrap(), ID,);
        assert_eq!(query_parser::uuid_literal_impl(&as_lower).unwrap(), ID,);
    }

    #[test]
    fn test_integer_literal() {
        assert_eq!(query_parser::integer_literal_impl("1").unwrap(), 1);
        assert_eq!(query_parser::integer_literal_impl("-1").unwrap(), -1);

        assert!(query_parser::integer_literal_impl("-1.0").is_err());
        assert!(query_parser::integer_literal_impl("-1.").is_err());
        assert!(query_parser::integer_literal_impl("1e3").is_err());
    }

    #[test]
    fn test_hex_integer_literal() {
        assert_eq!(query_parser::integer_literal_impl("0x1").unwrap(), 1);
        assert_eq!(query_parser::integer_literal_impl("-0x1").unwrap(), -1);
        assert_eq!(query_parser::integer_literal_impl("-0xa").unwrap(), -0xa);
        assert_eq!(
            query_parser::integer_literal_impl("0xfeed").unwrap(),
            0xfeed
        );
        assert_eq!(
            query_parser::integer_literal_impl("0xFEED").unwrap(),
            0xfeed
        );

        // Out of range in either direction
        assert!(
            query_parser::integer_literal_impl("0xFFFFFFFFFFFFFFFFFFFF")
                .is_err()
        );
        assert!(
            query_parser::integer_literal_impl("-0xFFFFFFFFFFFFFFFFFFFF")
                .is_err()
        );

        assert!(query_parser::integer_literal_impl("-0x1.0").is_err());
        assert!(query_parser::integer_literal_impl("-0x1.").is_err());
    }

    #[test]
    fn test_double_literal() {
        assert_eq!(query_parser::double_literal_impl("1.0").unwrap(), 1.0);
        assert_eq!(query_parser::double_literal_impl("-1.0").unwrap(), -1.0);
        assert_eq!(query_parser::double_literal_impl("1.").unwrap(), 1.0);
        assert_eq!(query_parser::double_literal_impl("-1.").unwrap(), -1.0);
        assert_eq!(query_parser::double_literal_impl(".5").unwrap(), 0.5);
        assert_eq!(query_parser::double_literal_impl("-.5").unwrap(), -0.5);
        assert_eq!(query_parser::double_literal_impl("1e3").unwrap(), 1e3);
        assert_eq!(query_parser::double_literal_impl("-1e3").unwrap(), -1e3);
        assert_eq!(query_parser::double_literal_impl("-1e-3").unwrap(), -1e-3);
        assert_eq!(
            query_parser::double_literal_impl("0.5e-3").unwrap(),
            0.5e-3
        );

        assert!(query_parser::double_literal_impl("-.e4").is_err());
        assert!(query_parser::double_literal_impl("-.e-4").is_err());
        assert!(query_parser::double_literal_impl("1e").is_err());
    }

    #[test]
    fn test_recognize_escape_sequences_with_none() {
        for each in ["", "abc", "$%("] {
            assert_eq!(recognize_escape_sequences(each).unwrap(), each);
        }
    }

    #[test]
    fn test_recognize_escape_sequence_with_valid_unicode_sequence() {
        // Welp, let's just test every possible code point.
        for x in 0..=0x10FFFF {
            let expected = char::from_u32(x);
            let as_hex = format!("{x:0x}");
            let sequence = format!("\\u{{{as_hex}}}");
            let recognized = recognize_escape_sequences(&sequence)
                .map(|s| s.chars().next().unwrap());
            assert_eq!(
                expected, recognized,
                "did not correctly recognized Unicode escape sequence"
            );
        }
    }

    #[test]
    fn test_recognize_escape_sequences_with_invalid_unicode_sequence() {
        for each in [
            r#"\uFFFF"#,       // Valid, but not using {} delimiters
            r#"\u{}"#,         // Not enough characters.
            r#"\u{12345678}"#, // Too many characters
            r#"\u{ZZZZ}"#,     // Not hex digits
            r#"\u{d800}"#,     // A surrogate code point, not valid.
            r#"\u{1234"#,      // Valid, but missing closing brace.
        ] {
            println!("{each}");
            assert!(recognize_escape_sequences(each).is_none());
        }
    }

    #[test]
    fn test_recognize_escape_sequences_with_valid_escape_sequence() {
        for (as_str, expected) in [
            (r#"\n"#, '\n'),
            (r#"\r"#, '\r'),
            (r#"\t"#, '\t'),
            (r#"\0"#, '\0'),
            (r#"\\"#, '\\'),
        ] {
            let recognized = recognize_escape_sequences(as_str).unwrap();
            assert_eq!(recognized.chars().next().unwrap(), expected);
        }
    }

    #[test]
    fn test_single_quoted_string_literal() {
        for (input, expected) in [
            ("''", String::new()),
            ("'simple'", String::from("simple")),
            ("'袈►♖'", String::from("袈►♖")),
            (r#"'escapes \n handled'"#, String::from("escapes \n handled")),
            (r#"'may contain " in it'"#, String::from("may contain \" in it")),
            (
                r#"'may contain "\u{1234}" in it'"#,
                String::from("may contain \"ሴ\" in it"),
            ),
        ] {
            assert_eq!(
                query_parser::string_literal_impl(input).unwrap(),
                expected
            );
        }
        assert!(
            query_parser::string_literal_impl(r#"' cannot have ' in it'"#)
                .is_err()
        );
    }

    #[test]
    fn test_double_quoted_string_literal() {
        for (input, expected) in [
            ("\"\"", String::new()),
            ("\"simple\"", String::from("simple")),
            ("\"袈►♖\"", String::from("袈►♖")),
            (r#""escapes \n handled""#, String::from("escapes \n handled")),
            (r#""may contain ' in it""#, String::from("may contain ' in it")),
            (
                r#""may contain '\u{1234}' in it""#,
                String::from("may contain 'ሴ' in it"),
            ),
        ] {
            assert_eq!(
                query_parser::string_literal_impl(input).unwrap(),
                expected
            );
        }

        assert!(
            query_parser::string_literal_impl(r#"" cannot have " in it""#)
                .is_err()
        );
    }

    #[test]
    fn test_comparison() {
        for (as_str, cmp) in [
            ("==", Comparison::Eq),
            ("!=", Comparison::Ne),
            (">=", Comparison::Ge),
            (">", Comparison::Gt),
            ("<=", Comparison::Le),
            ("<", Comparison::Lt),
            ("~=", Comparison::Like),
        ] {
            assert_eq!(query_parser::comparison(as_str).unwrap(), cmp);
        }
    }

    #[test]
    fn test_filter_expr_single_simple_expression() {
        let expr = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("a".to_string()),
                cmp: Comparison::Eq,
                value: Literal::Boolean(true),
            }),
        };
        assert_eq!(query_parser::filter_expr("a == true").unwrap(), expr);
        assert_eq!(query_parser::filter_expr("(a == true)").unwrap(), expr);

        assert!(query_parser::filter_expr("(a == true").is_err());
    }

    #[test]
    fn test_filter_expr_single_negated_simple_expression() {
        let expr = Filter {
            negated: true,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("a".to_string()),
                cmp: Comparison::Gt,
                value: Literal::Double(1.0),
            }),
        };
        assert_eq!(query_parser::filter_expr("!(a > 1.)").unwrap(), expr,);

        assert!(query_parser::filter_expr("!(a > 1.0").is_err());
    }

    #[test]
    fn test_filter_expr_two_simple_filter_expressions() {
        let left = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("a".to_string()),
                cmp: Comparison::Eq,
                value: Literal::Boolean(true),
            }),
        };

        for op in [LogicalOp::And, LogicalOp::Or] {
            let expected = left.merge(&left, op);
            // Match with either parenthesized.
            let as_str = format!("a == true {op} (a == true)");
            assert_eq!(query_parser::filter_expr(&as_str).unwrap(), expected);
            let as_str = format!("(a == true) {op} a == true");
            assert_eq!(query_parser::filter_expr(&as_str).unwrap(), expected);
            let as_str = format!("(a == true) {op} (a == true)");
            assert_eq!(query_parser::filter_expr(&as_str).unwrap(), expected);
        }
    }

    #[test]
    fn test_filter_expr_operator_precedence() {
        // We'll combine the following simple expression in a number of
        // different sequences, to check that we correctly group by operator
        // precedence.
        let atom = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("a".to_string()),
                cmp: Comparison::Eq,
                value: Literal::Boolean(true),
            }),
        };
        let as_str = "a == true || a == true && a == true ^ a == true";
        let parsed = query_parser::filter_expr(as_str).unwrap();
        assert_eq!(
            parsed.to_string(),
            "((a == true) || ((a == true) && ((a == true) ^ (a == true))))"
        );

        // This should bind most tighty from right to left: XOR, then AND, then
        // OR. Since we're destructuring from out to in, though, we check in the
        // opposite order, weakest to strongest, or left to right.
        //
        // Start with OR, which should bind the most weakly.
        assert!(!parsed.negated);
        let FilterExpr::Compound(CompoundFilter { left, op, right }) =
            parsed.expr
        else {
            unreachable!();
        };
        assert!(!left.negated);
        assert!(!right.negated);
        assert_eq!(op, LogicalOp::Or);
        assert_eq!(atom, *left);

        // && should bind next-most tightly
        let FilterExpr::Compound(CompoundFilter { left, op, right }) =
            right.expr
        else {
            unreachable!();
        };
        assert!(!left.negated);
        assert!(!right.negated);
        assert_eq!(op, LogicalOp::And);
        assert_eq!(atom, *left);

        // Followed by XOR, the tightest binding operator.
        let FilterExpr::Compound(CompoundFilter { left, op, right }) =
            right.expr
        else {
            unreachable!();
        };
        assert!(!left.negated);
        assert!(!right.negated);
        assert_eq!(op, LogicalOp::Xor);
        assert_eq!(atom, *left);
        assert_eq!(atom, *right);
    }

    #[test]
    fn test_filter_expr_overridden_precedence() {
        // Similar to above, we'll test with a single atom, and group in a
        // number of ways.
        let atom = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("a".to_string()),
                cmp: Comparison::Eq,
                value: Literal::Boolean(true),
            }),
        };
        let as_str = "(a == true || a == true) && a == true";
        let parsed = query_parser::filter_expr(as_str).unwrap();

        // Now, || should bind more tightly, so we should have (a && b) at the
        // top-level, where b is the test atom. We're comparing the atom at the
        // _right_ now with the original expressions.
        assert!(!parsed.negated);
        let FilterExpr::Compound(CompoundFilter { left, op, right }) =
            parsed.expr
        else {
            unreachable!();
        };
        assert!(!left.negated);
        assert!(!right.negated);
        assert_eq!(op, LogicalOp::And);
        assert_eq!(atom, *right);

        // Destructure the LHS and check it.
        let FilterExpr::Compound(CompoundFilter { left, op, right }) =
            left.expr
        else {
            unreachable!();
        };
        assert!(!left.negated);
        assert!(!right.negated);
        assert_eq!(op, LogicalOp::Or);
        assert_eq!(atom, *left);
        assert_eq!(atom, *right);
    }

    #[test]
    fn test_negated_filter_expr() {
        let left = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("a".into()),
                cmp: Comparison::Eq,
                value: Literal::Boolean(true),
            }),
        };
        let right = left.negate();
        let top = left.merge(&right, LogicalOp::Xor).negate();
        let as_str = "!(a == true ^ !(a == true))";
        let parsed = query_parser::filter_expr(as_str).unwrap();
        assert_eq!(top, parsed);
    }

    #[test]
    fn test_filter_table_op() {
        for expr in [
            "filter field == 0",
            "filter baz == 'quux'",
            "filter other_field != 'yes'",
            "filter id != \"45c937fb-5e99-4a86-a95b-22bf30bf1507\"",
            "filter (foo == 'bar') || ((yes != \"no\") && !(maybe > 'so'))",
        ] {
            let parsed = query_parser::filter(expr).unwrap_or_else(|_| {
                panic!("failed to parse query: '{}'", expr)
            });
            println!("{parsed:#?}");
        }
    }

    #[test]
    fn test_get_table_op() {
        for expr in [
            "get foo:bar",
            "get target_name:metric_name",
            "get target_name_0:metric_name000",
        ] {
            let parsed = query_parser::get(expr).unwrap_or_else(|_| {
                panic!("failed to parse get expr: '{}'", expr)
            });
            println!("{parsed:#?}");
        }

        assert!(query_parser::get("get foo").is_err());
        assert!(query_parser::get("get foo:").is_err());
        assert!(query_parser::get("get :bar").is_err());
        assert!(query_parser::get("get 0:0").is_err());
    }

    #[test]
    fn test_ident() {
        for id in ["foo", "foo0", "foo_0_1_2"] {
            query_parser::ident(id)
                .unwrap_or_else(|_| panic!("failed to identifier: '{id}'"));
        }

        for id in ["0foo", "0", "A", "", "%", "foo_"] {
            query_parser::ident(id).expect_err(&format!(
                "should not have parsed as identifier: '{}'",
                id
            ));
        }
    }

    #[test]
    fn test_group_by() {
        for q in [
            "group_by []",
            "group_by [baz]",
            "group_by [baz,]",
            "group_by [baz,another_field]",
            "group_by [baz,another_field,]",
        ] {
            let parsed = query_parser::group_by(q)
                .unwrap_or_else(|_| panic!("failed to parse group_by: '{q}'"));
            println!("{parsed:#?}");
        }
    }

    #[test]
    fn test_query() {
        for q in [
            "get foo:bar",
            "get foo:bar | group_by []",
            "get foo:bar | group_by [baz]",
            "get foo:bar | filter baz == 'quuz'",
            "get foo:bar | filter (some == 0) && (id == false || a == -1.0)",
            "get foo:bar | group_by [baz] | filter baz == 'yo'",
            "{ get foo:bar | filter x == 0; get x:y } | join",
            "{ get foo:bar ; get x:y } | join | filter baz == 0",
            "get foo:bar | align interpolate(10s)",
        ] {
            let parsed = query_parser::query(q)
                .unwrap_or_else(|_| panic!("failed to parse query: '{q}'"));
            println!("{parsed:#?}");
        }
    }

    #[test]
    fn test_reducer() {
        assert_eq!(query_parser::reducer("mean").unwrap(), Reducer::Mean);
        assert!(query_parser::reducer("foo").is_err());
    }

    #[test]
    fn test_parse_literal_timestamp_string() {
        assert_eq!(
            query_parser::timestamp_string("@2020-01-01").unwrap(),
            Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
        );
        assert_eq!(
            query_parser::timestamp_string("@01:01:01").unwrap().time(),
            NaiveTime::from_hms_opt(1, 1, 1).unwrap(),
        );
        assert_eq!(
            query_parser::timestamp_string("@01:01:01.123456").unwrap().time(),
            NaiveTime::from_hms_micro_opt(1, 1, 1, 123456).unwrap(),
        );
        assert_eq!(
            query_parser::timestamp_string("@2020-01-01T01:01:01.123456")
                .unwrap(),
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                NaiveTime::from_hms_micro_opt(1, 1, 1, 123456).unwrap(),
            )
            .and_utc(),
        );
    }

    #[test]
    fn test_parse_ipv4_literal() {
        let check = |s: &str, addr: IpAddr| {
            let Literal::IpAddr(ip) = query_parser::ip_literal(s).unwrap()
            else {
                panic!("expected '{}' to be parsed into {}", s, addr);
            };
            assert_eq!(ip, addr);
        };
        check("\"100.100.100.100\"", Ipv4Addr::new(100, 100, 100, 100).into());
        check("\"1.2.3.4\"", Ipv4Addr::new(1, 2, 3, 4).into());
        check("\"0.0.0.0\"", Ipv4Addr::UNSPECIFIED.into());

        assert!(query_parser::ip_literal("\"abcd\"").is_err());
        assert!(query_parser::ip_literal("\"1.1.1.\"").is_err());
        assert!(query_parser::ip_literal("\"1.1.1.1.1.1\"").is_err());
        assert!(query_parser::ip_literal("\"2555.1.1.1\"").is_err());
        assert!(query_parser::ip_literal("1.2.3.4").is_err()); // no quotes
    }

    #[test]
    fn test_parse_ipv6_literal() {
        let check = |s: &str, addr: IpAddr| {
            let Literal::IpAddr(ip) = query_parser::ip_literal(s).unwrap()
            else {
                panic!("expected '{}' to be parsed into {}", s, addr);
            };
            assert_eq!(ip, addr);
        };

        // IPv6 is nuts, let's just check a few common patterns.
        check("\"::1\"", Ipv6Addr::LOCALHOST.into());
        check("\"::\"", Ipv6Addr::UNSPECIFIED.into());
        check("\"fd00::1\"", Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1).into());
        check(
            "\"fd00:1:2:3:4:5:6:7\"",
            Ipv6Addr::new(0xfd00, 1, 2, 3, 4, 5, 6, 7).into(),
        );

        // Don't currently support IPv6-mapped IPv4 addresses
        assert!(query_parser::ip_literal("\"::ffff:127.0.0.1\"").is_err());

        // Other obviously bad patterns.
        assert!(query_parser::ip_literal("\"1\"").is_err());
        assert!(query_parser::ip_literal("\":1::1::1\"").is_err());
        assert!(query_parser::ip_literal("\"::g\"").is_err());
        assert!(query_parser::ip_literal("\":::\"").is_err());
        assert!(query_parser::ip_literal("::1").is_err()); // no quotes
    }

    #[test]
    fn test_query_starts_with_get() {
        assert!(
            query_parser::query("{ get a:b }")
                .unwrap()
                .all_gets_at_query_start()
        );
        assert!(
            query_parser::query("{ get a:b; get a:b } | join")
                .unwrap()
                .all_gets_at_query_start()
        );
        assert!(
            query_parser::query(
                "{ { get a:b ; get a:b } | join; get c:d } | join"
            )
            .unwrap()
            .all_gets_at_query_start()
        );

        assert!(query_parser::query("{ get a:b; filter foo == 0 }").is_err());
        assert!(query_parser::query("{ get a:b; filter foo == 0 }").is_err());
        assert!(query_parser::query("get a:b | get a:b").is_err());
    }

    #[test]
    fn test_like_only_available_for_strings() {
        assert!(query_parser::filter_expr("foo ~= 0").is_err());
        assert!(query_parser::filter_expr("foo ~= \"something\"").is_ok());
    }

    #[test]
    fn test_align_table_op() {
        assert_eq!(
            query_parser::align("align interpolate(1m)").unwrap(),
            Align {
                method: AlignmentMethod::Interpolate,
                period: Duration::from_secs(60)
            }
        );
        assert_eq!(
            query_parser::align("align mean_within(100s)").unwrap(),
            Align {
                method: AlignmentMethod::MeanWithin,
                period: Duration::from_secs(100)
            }
        );

        assert!(query_parser::align("align whatever(100s)").is_err());
        assert!(query_parser::align("align interpolate('foo')").is_err());

        assert_eq!(
            query_parser::align("align rate(100s)").unwrap(),
            Align {
                method: AlignmentMethod::Rate,
                period: Duration::from_secs(100)
            }
        );

        assert_eq!(
            query_parser::align("align min(100s)").unwrap(),
            Align {
                method: AlignmentMethod::Min,
                period: Duration::from_secs(100)
            }
        );

        assert_eq!(
            query_parser::align("align max(100s)").unwrap(),
            Align {
                method: AlignmentMethod::Max,
                period: Duration::from_secs(100)
            }
        );
    }

    #[test]
    fn test_complicated_logical_combinations() {
        let parsed =
            query_parser::logical_or_expr("a == 'b' ^ !(c == 0) && d == false")
                .unwrap();

        // Build up this expected expression from its components.
        let left = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("a".to_string()),
                cmp: Comparison::Eq,
                value: Literal::String("b".into()),
            }),
        };
        let middle = Filter {
            negated: true,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("c".to_string()),
                cmp: Comparison::Eq,
                value: Literal::Integer(0),
            }),
        };
        let right = Filter {
            negated: false,
            expr: FilterExpr::Simple(SimpleFilter {
                ident: Ident("d".to_string()),
                cmp: Comparison::Eq,
                value: Literal::Boolean(false),
            }),
        };

        // The left and right are bound most tightly, by the XOR operator.
        let xor = Filter {
            negated: false,
            expr: FilterExpr::Compound(CompoundFilter {
                left: Box::new(left),
                op: LogicalOp::Xor,
                right: Box::new(middle),
            }),
        };

        // And then those two together are joined with the AND.
        let expected = Filter {
            negated: false,
            expr: FilterExpr::Compound(CompoundFilter {
                left: Box::new(xor),
                op: LogicalOp::And,
                right: Box::new(right),
            }),
        };
        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_multiple_negation() {
        let negated =
            query_parser::filter_expr("(a == 0) || !!!(a == 0 && a == 0)")
                .unwrap();
        let expected =
            query_parser::filter_expr("(a == 0) || !(a == 0 && a == 0)")
                .unwrap();
        assert_eq!(negated, expected, "Failed to handle multiple negations");
    }

    #[test]
    fn test_limiting_table_ops() {
        assert_eq!(
            query_parser::limit("first 100").unwrap(),
            Limit { kind: LimitKind::First, count: 100.try_into().unwrap() },
        );
        assert_eq!(
            query_parser::limit("last 100").unwrap(),
            Limit { kind: LimitKind::Last, count: 100.try_into().unwrap() },
        );

        assert!(
            query_parser::limit(&format!("first {}", usize::MAX as i128 + 1))
                .is_err()
        );
        assert!(query_parser::limit("first 0").is_err());
        assert!(query_parser::limit("first -1").is_err());
        assert!(query_parser::limit("first \"foo\"").is_err());
    }
}
