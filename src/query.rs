use std::result;
use std::collections::HashMap;
use std::rc::Rc;
use chrono::prelude::*;
use parser::*;

const EMPTY_BYTES: &[u8] = &[];

pub struct TableDefinition<T> {
    pub column_map: HashMap<String, ColumnDefinition<T>>,
}

pub enum ColumnDefinition<T> {
    Integer { name: &'static str,
              binary_extractor: Box<Fn(&T) -> Option<&[u8]>>,
              extractor: Box<Fn(&mut T) -> Option<u64>> },
    Double { name: &'static str,
             binary_extractor: Box<Fn(&T) -> Option<&[u8]>>,
             extractor: Box<Fn(&mut T) -> Option<f64>> },
    Text { name: &'static str,
           binary_extractor: Box<Fn(&T) -> Option<&[u8]>>,
           extractor: Box<Fn(&mut T) -> Option<&str>> },
    Date { name: &'static str,
           binary_extractor: Box<Fn(&T) -> Option<&[u8]>>,
           extractor: Box<Fn(&mut T) -> Option<DateTime<FixedOffset>>> },
    Boolean { name: &'static str,
              binary_extractor: Box<Fn(&T) -> Option<&[u8]>>,
              extractor: Box<Fn(&mut T) -> Option<bool>> }
}

impl<T> ColumnDefinition<T> {
    pub fn name(&self) -> &str {
        match self {
            ColumnDefinition::Integer { name, .. } => name,
            ColumnDefinition::Double { name, .. } => name,
            ColumnDefinition::Text { name, .. } => name,
            ColumnDefinition::Date { name, .. } => name,
            ColumnDefinition::Boolean { name, .. } => name,
        }
    }

    pub fn extract_binary<'b>(&self, item: &'b T) -> Option<&'b [u8]> {
        match self {
            ColumnDefinition::Text { binary_extractor, ..} => binary_extractor(item),
            ColumnDefinition::Double { binary_extractor, ..} => binary_extractor(item),
            ColumnDefinition::Integer { binary_extractor, ..} => binary_extractor(item),
            ColumnDefinition::Boolean { binary_extractor, ..} => binary_extractor(item),
            ColumnDefinition::Date { binary_extractor, ..} => binary_extractor(item),
        }
    }
}

pub fn validate_riplog_query<T>(query: &RipLogQuery, definition: &TableDefinition<T>) -> Result<()> {
    validate_riplog_filter(&query.filter, &definition)
}

fn validate_riplog_filter<T>(filter: &QueryFilter, definition: &TableDefinition<T>) -> Result<()> {
    match filter {
        QueryFilter::BinaryOpFilter(operand1, operand2, op) =>
            validate_filter_operand(&operand1, &definition).and(validate_filter_operand(&operand2, &definition)),
        QueryFilter::AndFilter(filter1, filter2) =>
            validate_riplog_filter(&filter1, &definition).and(validate_riplog_filter(&filter2, &definition)),
        QueryFilter::OrFilter(filter1, filter2) =>
            validate_riplog_filter(&filter1, &definition).and(validate_riplog_filter(&filter2, &definition)),
    }
}

fn validate_filter_operand<T>(operand: &QueryValue, definition: &TableDefinition<T>) -> Result<()> {
    match operand {
        QueryValue::Symbol(symbol) => validate_symbol(&symbol, &definition),
        QueryValue::Text(text, _) => Ok(()),
        QueryValue::Int(int, _) => Ok(()),
        QueryValue::Double(dbl, _) => Ok(()),
        QueryValue::Boolean(boolvalue) => Ok(()),
        QueryValue::Regex(regex) => Ok(()),
        QueryValue::Date(date) => Ok(()),
        QueryValue::Null => Ok(()),
    }
}

fn validate_symbol<T>(symbol: &str, definition: &TableDefinition<T>) -> Result<()> {
    if definition.column_map.contains_key(symbol) {
        Ok(())
    } else {
        Err(QueryValidationError { msg: format!("Symbol '{}' is not a valid column", symbol) })
    }
}

pub struct QueryEvaluator<T> {
    pub query: Rc<RipLogQuery>,
    pub definition: Rc<TableDefinition<T>>
}

impl<T> QueryEvaluator<T> {

    pub fn apply_filters(&mut self, item: &mut T) -> bool {
        let filter = &self.query.clone().filter;
        self.evaluate_filter(filter, item)
    }

    fn evaluate_filter(&mut self, filter: &QueryFilter, item: &mut T) -> bool {
        match filter {
            QueryFilter::BinaryOpFilter(operand1, operand2, op) =>
                self.evaluate_binary_filter(&operand1, &operand2, op, item),
            QueryFilter::AndFilter(filter1, filter2) =>
                self.evaluate_filter(&filter1, item) && self.evaluate_filter(&filter2, item),
            QueryFilter::OrFilter(filter1, filter2) =>
                self.evaluate_filter(&filter1, item) || self.evaluate_filter(&filter2, item),
        }
    }

    fn evaluate_binary_filter(&mut self, operand1: &QueryValue, operand2: &QueryValue, op: &QueryFilterBinaryOp, item: &mut T) -> bool {
        match op {
            QueryFilterBinaryOp::Lt => self.evaluate_lt(operand1, operand2, item),
            QueryFilterBinaryOp::Gt => self.evaluate_gt(operand1, operand2, item),
            QueryFilterBinaryOp::Eq => self.evaluate_eq(operand1, operand2, item),
            QueryFilterBinaryOp::Ne => !self.evaluate_eq(operand1, operand2, item),
            QueryFilterBinaryOp::Re => self.evaluate_re(operand1, operand2, item),
            QueryFilterBinaryOp::Nr => !self.evaluate_re(operand1, operand2, item),
        }
    }

    fn evaluate_eq(&mut self, operand1: &QueryValue, operand2: &QueryValue, item: &T) -> bool {
        let op1bytes = self.resolve_byte_value(operand1, item);
        let op2bytes = self.resolve_byte_value(operand2, item);
        op1bytes.is_some() && op2bytes.is_some() && op1bytes.unwrap() == op2bytes.unwrap()
    }

    fn evaluate_lt(&mut self, operand1: &QueryValue, operand2: &QueryValue, item: &T) -> bool {
        let op1bytes = self.resolve_byte_value(operand1, item);
        let op2bytes = self.resolve_byte_value(operand2, item);
        op1bytes.is_some() && op2bytes.is_some() && op1bytes.unwrap() < op2bytes.unwrap()
    }

    fn evaluate_gt(&mut self, operand1: &QueryValue, operand2: &QueryValue, item: &T) -> bool {
        let op1bytes = self.resolve_byte_value(operand1, item);
        let op2bytes = self.resolve_byte_value(operand2, item);
        op1bytes.is_some() && op2bytes.is_some() && op1bytes.unwrap() > op2bytes.unwrap()
    }

    // TODO: Make work with arbitrary values (borrow checker woes)
    fn evaluate_re(&mut self, operand1: &QueryValue, operand2: &QueryValue, item: &mut T) -> bool {
        match (operand1, operand2) {
            (QueryValue::Symbol(symbol), QueryValue::Regex(regex)) => {
                let string_value = self.get_symbol_string(symbol, item);
                string_value.is_some() && regex.is_match(string_value.unwrap())
            },
            (QueryValue::Symbol(symbol), QueryValue::Text(value, _)) => {
                let string_value1 = self.get_symbol_string(symbol, item);
                string_value1.is_some() &&  string_value1.unwrap().contains(value)
            }
            _ => false
        }
    }

    fn get_symbol_bytes<'b>(&self, symbol: &str, item: &'b T) -> Option<&'b [u8]> {
        self.get_symbol_definition(symbol).extract_binary(item)
    }

    fn get_symbol_string<'b>(&self, symbol: &str, item: &'b mut T) -> Option<&'b str> {
        match self.get_symbol_definition(symbol) {
            ColumnDefinition::Text { extractor, .. } => extractor(item),
            _ => None
        }
    }

    fn get_symbol_definition(&self, symbol: &str) -> &ColumnDefinition<T> {
        self.definition.column_map.get(symbol).unwrap()
    }

    fn resolve_byte_value<'a>(&self, value: &'a QueryValue, item: &'a T) -> Option<&'a [u8]> {
        match value {
            QueryValue::Text(_, bytes) => Some(bytes),
            QueryValue::Int(_, bytes) => Some(bytes),
            QueryValue::Double(_, bytes) => Some(bytes),
            QueryValue::Null => Some(EMPTY_BYTES),
            QueryValue::Symbol(symbol) => self.get_symbol_bytes(symbol, item),
            QueryValue::Date(date) => None,
            _ => None
        }
    }

    fn resolve_string_value<'a>(&self, value: &'a QueryValue, item: &'a mut T) -> Option<&'a str> {
        match value {
            QueryValue::Text(value, _) => Some(&value),
            QueryValue::Symbol(symbol) => self.get_symbol_string(symbol, item),
            _ => None
        }
    }
}

type Result<T> = result::Result<T, QueryValidationError>;

#[derive(Debug, Clone)]
pub struct QueryValidationError { msg: String }
