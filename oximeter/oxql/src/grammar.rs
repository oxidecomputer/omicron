// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Grammar for the Oximeter Query Language (OxQL).

// This is a temporary parity copy of oximeter-db's grammar for the parse-only
// feasibility checkpoint. The shared corpus fails if their accepted/rejected
// behavior diverges. The next extraction stage should make this copy canonical
// and delete the legacy grammar.

peg::parser! {
    pub grammar query_parser() for str {
        use crate::ast::{
            Align, AlignmentMethod, BasicTableOp, Comparison, CompoundFilter,
            Filter, FilterExpr, Get, GroupBy, GroupedTableOp, Ident, Join,
            Limit, LimitKind, Literal, LogicalOp, Query, Reducer, SimpleFilter,
            TableOp, TimeseriesName, duration_consts,
        };
        use std::time::Duration;
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

        rule dashed_uuid_literal() -> String
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
                Ok(middle.to_owned())
            }
        rule undashed_uuid_literal() -> String
            = s:$("\"" ['a'..='f' | 'A'..='F' | '0'..='9']*<32> "\"") {?
            let Some(middle) = s.get(1..33) else {
                return Err("invalid UUID literal");
            };
            Ok(middle.to_owned())
        }
        pub(super) rule uuid_literal_impl() -> String
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
            = start:position!()
              target_name:ident_impl() ":" metric_name:ident_impl()
              end:position!()
        {
            TimeseriesName {
                value: format!("{target_name}:{metric_name}"),
                start,
                end,
            }
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
            = start:position!()
              ops:(basic_table_op() / grouped_table_op()) ++ query_delim()
              end:position!()
        {?
            let query = Query { ops, start, end };
            if query.all_gets_at_query_start() {
                Ok(query)
            } else {
                Err("every subquery must start with a `get` operation")
            }
        }

        pub rule document() -> Query
            = _? query:query() _? { query }

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
