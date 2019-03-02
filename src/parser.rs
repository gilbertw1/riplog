use nom;
use nom::types::CompleteStr;
use chrono::prelude::*;
use regex::Regex;


////////////
// FILTER //
////////////

named!(parse_filter_operator<CompleteStr, QueryFilterBinaryOp>,
       alt!(map!(tag_s!("<"), |_| QueryFilterBinaryOp::Lt) |
            map!(tag_s!(">"), |_| QueryFilterBinaryOp::Gt) |
            map!(tag_s!("="), |_| QueryFilterBinaryOp::Eq) |
            map!(tag_s!("!="), |_| QueryFilterBinaryOp::Ne) |
            map!(tag_s!("~"), |_| QueryFilterBinaryOp::Re) |
            map!(tag_s!("!~"), |_| QueryFilterBinaryOp::Nr)));

named!(parse_null_operand<CompleteStr, QueryValue>,
       map!(tag_no_case_s!("null"),
            |_| QueryValue::Null));

named!(parse_boolean_operand<CompleteStr, QueryValue>,
       map!(alt!(tag_no_case_s!("true") | tag_no_case_s!("false")),
            |b| QueryValue::Boolean(b.to_lowercase().parse::<bool>().unwrap())));

named!(parse_text_operand<CompleteStr, QueryValue>,
       map!(delimited!(char!('"'), take_until_s!("\""), char!('"')),
            |t| QueryValue::Text(t.to_string(), t.to_string().into_bytes())));

named!(parse_regex_operand<CompleteStr, QueryValue>,
       map!(tuple!(tag!("r"), delimited!(char!('"'), take_until_s!("\""), char!('"'))),
            |t| QueryValue::Regex(Regex::new(&t.1.to_string()).unwrap())));
       
named!(parse_symbol_operand<CompleteStr, QueryValue>,
       map!(nom::alpha,
            |s| QueryValue::Symbol(s.to_string().to_lowercase())));

named!(parse_int_operand<CompleteStr, QueryValue>,
       map!(nom::digit,
            |i| QueryValue::Int(i.parse::<i64>().unwrap(), i.to_string().into_bytes())));

named!(parse_double_operand<CompleteStr, QueryValue>,
       map!(tuple!(nom::digit, tag_s!("."), nom::digit),
            |d| QueryValue::Double(format!("{}.{}", d.0, d.2).to_string().parse::<f64>().unwrap(), format!("{}.{}", d.0, d.2).to_string().into_bytes())));

named!(parse_filter_operand<CompleteStr, QueryValue>,
       alt!(parse_text_operand |
            parse_regex_operand |
            parse_boolean_operand |
            parse_null_operand |
            parse_symbol_operand |
            parse_double_operand |
            parse_int_operand));

named!(parse_binary_op_filter<CompleteStr, QueryFilter>,
       map!(ws!(tuple!(parse_filter_operand,
                       parse_filter_operator,
                       parse_filter_operand)),
       |t| QueryFilter::BinaryOpFilter(t.0, t.2, t.1)));

named!(parse_parenthetical_filter<CompleteStr, QueryFilter>,
       map!(ws!(tuple!(tag_s!("("),
                       parse_filter,
                       tag_s!(")"))),
            |f| f.1));

named!(parse_unit_filter<CompleteStr, QueryFilter>,
       alt_complete!(parse_parenthetical_filter | parse_binary_op_filter));

named!(parse_and_filter<CompleteStr, QueryFilter>,
       map!(ws!(tuple!(parse_unit_filter,
                       alt!(tag_no_case_s!("and") | tag_no_case_s!("&&")),
                       parse_and_fallback_filter)),
            |f| QueryFilter::AndFilter(Box::new(f.0), Box::new(f.2))));

named!(parse_and_fallback_filter<CompleteStr, QueryFilter>,
       alt_complete!(parse_and_filter | parse_unit_filter));

named!(parse_or_filter<CompleteStr, QueryFilter>,
       map!(ws!(tuple!(parse_and_fallback_filter,
                       alt!(tag_no_case_s!("or") | tag_no_case_s!("||")),
                       parse_or_fallback_filter)),
            |f| QueryFilter::OrFilter(Box::new(f.0), Box::new(f.2))));

named!(parse_or_fallback_filter<CompleteStr, QueryFilter>,
       alt_complete!(parse_or_filter | parse_and_fallback_filter));

named!(parse_filter<CompleteStr, QueryFilter>,
       ws!(parse_or_fallback_filter));

//////////////
// GROUPING //
//////////////

named!(parse_grouping<CompleteStr, QueryGrouping>,
       map!(tuple!(tag_no_case_s!("group"), separated_list!(tag!(","), ws!(map!(nom::alpha, |s| s.to_string().to_lowercase())))),
            |groupings| QueryGrouping { groupings: groupings.1 }));

//////////
// SHOW //
//////////

named!(parse_show<CompleteStr, QueryShow>,
       map!(tuple!(tag_no_case_s!("show"), separated_list!(tag!(","), ws!(parse_show_element))),
            |elements| QueryShow { elements: elements.1 }));

named!(parse_show_element<CompleteStr, QueryShowElement>,
       alt!(parse_show_all | parse_show_reducer | parse_show_symbol));

named!(parse_show_all<CompleteStr, QueryShowElement>,
       map!(tag_no_case_s!("*"),
            |s| QueryShowElement::All));

named!(parse_show_symbol<CompleteStr, QueryShowElement>,
       map!(nom::alpha,
            |s| QueryShowElement::Symbol(s.to_string().to_lowercase())));

named!(parse_show_reducer<CompleteStr, QueryShowElement>,
       map!(tuple!(parse_reducer, delimited!(char!('('), take_until_s!(")"), char!(')'))),
            |s| QueryShowElement::Reducer(s.0, s.1.to_string().to_lowercase())));

named!(parse_reducer<CompleteStr, Reducer>,
       alt!(map!(tag_s!("count"), |_| Reducer::Count) |
            map!(tag_s!("sum"), |_| Reducer::Sum) |
            map!(tag_s!("max"), |_| Reducer::Max) |
            map!(tag_s!("avg"), |_| Reducer::Avg)));

//////////
// SORT //
//////////

named!(parse_sort<CompleteStr, QuerySort>,
       map!(tuple!(tag_no_case_s!("sort"), separated_list!(tag!(","), ws!(map!(nom::alpha, |s| s.to_string().to_lowercase())))),
            |sortings| QuerySort { sortings: sortings.1 }));

///////////
// LIMIT //
///////////

named!(parse_limit<CompleteStr, QueryLimit>,
       map!(tuple!(tag_no_case_s!("limit"), nom::digit),
            |limit| QueryLimit { limit: limit.1.parse::<u32>().unwrap() }));

///////////
// QUERY //
///////////

named!(parse_riplog_query<CompleteStr, RipLogQuery>,
       map!(tuple!(opt!(ws!(parse_filter)),
                   opt!(tag_no_case_s!("|")),
                   opt!(ws!(parse_grouping)),
                   opt!(tag_no_case_s!("|")),
                   opt!(ws!(parse_show)),
                   opt!(tag_no_case_s!("|")),
                   opt!(ws!(parse_sort)),
                   opt!(tag_no_case_s!("|")),
                   opt!(ws!(parse_limit))),
            |f| RipLogQuery { filter: f.0, grouping: f.2, show: f.4, sort: f.6, limit: f.8 }));

pub fn parse_query(query: String) -> RipLogQuery {
    parse_riplog_query(CompleteStr(&query)).unwrap().1
}


#[derive(Debug, Clone)]
pub struct RipLogQuery {
    pub filter: Option<QueryFilter>,
    pub grouping: Option<QueryGrouping>,
    pub show: Option<QueryShow>,
    pub sort: Option<QuerySort>,
    pub limit: Option<QueryLimit>,
}

#[derive(Debug, Clone)]
pub enum QueryFilter {
    BinaryOpFilter(QueryValue, QueryValue, QueryFilterBinaryOp),
    AndFilter(Box<QueryFilter>, Box<QueryFilter>),
    OrFilter(Box<QueryFilter>, Box<QueryFilter>),
}

#[derive(Debug, Clone)]
pub enum QueryValue {
    Symbol(String),
    Text(String, Vec<u8>),
    Regex(Regex),
    Int(i64, Vec<u8>),
    Double(f64, Vec<u8>),
    Boolean(bool),
    Date(DateTime<FixedOffset>),
    Null,
}

#[derive(Debug, Clone)]
pub enum QueryFilterBinaryOp {
    Lt, Gt, Eq, Ne, Re, Nr
}

#[derive(Debug, Clone)]
pub struct QueryGrouping {
    pub groupings: Vec<String>
}

#[derive(Debug, Clone)]
pub struct QueryShow {
    pub elements: Vec<QueryShowElement>
}

#[derive(Debug, Clone)]
pub enum QueryShowElement {
    All,
    Symbol(String),
    Reducer(Reducer, String)
}

#[derive(Debug, Clone)]
pub enum Reducer {
    Count,
    Sum,
    Max,
    Avg,
}

#[derive(Debug, Clone)]
pub struct QuerySort {
    pub sortings: Vec<String>
}

#[derive(Debug, Clone)]
pub struct QueryLimit {
    pub limit: u32
}
