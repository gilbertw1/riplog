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
    if query.filter.is_some() {
        validate_riplog_filter(query.filter.as_ref().unwrap(), &definition)?
    } 

    if query.grouping.is_some() {
        validate_riplog_grouping(query.grouping.as_ref().unwrap(), &definition)?
    }

    if query.show.is_some() {
        validate_riplog_show(query.show.as_ref().unwrap(), &definition, query.grouping.is_some())?
    }

    if query.sort.is_some() {
        validate_riplog_sort(query.sort.as_ref().unwrap(), &definition, query.show.as_ref())?
    }

    Ok(())
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

fn validate_riplog_grouping<T>(grouping: &QueryGrouping, definition: &TableDefinition<T>) -> Result<()> {
    for s in &grouping.groupings {
        validate_symbol(s, definition)?
    }
    Ok(())
}

fn validate_riplog_show<T>(show: &QueryShow, definition: &TableDefinition<T>, grouped: bool) -> Result<()> {
    for element in &show.elements {
        match element {
            QueryShowElement::Symbol(symbol) => {
                    validate_symbol(symbol, definition)?
            },
            QueryShowElement::Reducer(_, symbol) =>  {
                if symbol != "*" {
                    validate_symbol(symbol, definition)?
                }
            }
            _ => ()
        }
    }
    Ok(())
}

// TODO: Validate sorts are valid
fn validate_riplog_sort<T>(sort: &QuerySort, definition: &TableDefinition<T>, show: Option<&QueryShow>) -> Result<()> {
    for sorting in &sort.sortings {
        ();
    }
    Ok(())
}

pub struct QueryEvaluator<T> {
    pub query: Rc<RipLogQuery>,
    pub definition: Rc<TableDefinition<T>>,
    pub group_map: HashMap<Vec<String>,u64>
}

impl<T> QueryEvaluator<T> {

    fn print_row(&self, item: &mut T) {
        if let Some(show) = &self.query.show {
            for element in &show.elements {
                match element {
                    QueryShowElement::Symbol(symbol) => {
                        let value = self.get_symbol_as_string(symbol, item);
                        if value.is_some() {
                            print!("{}", value.unwrap());
                        } else {
                            print!("null");
                        }
                        print!(" - ");
                    },
                    _ => ()
                }
            }
        } else {
            for coldef in self.definition.column_map.values() {
                let value = self.get_column_value_as_string(coldef, item);
                if value.is_some() {
                    print!("{}", value.unwrap());
                } else {
                    print!("null");
                }
                print!(" - ");
            }
        }
        println!("");
    }

    pub fn evaluate(&mut self, item: &mut T) {
        if self.apply_filters(item) {
            //self.print_row(item);
            self.group(item)
        }
    }

    fn group(&mut self, item: &mut T) {
        //let key = vec![self.get_symbol_as_string("ip", item).unwrap_or("null".to_owned()), self.get_symbol_as_string("status", item).unwrap_or("null".to_owned())];
        let key = vec![self.get_symbol_as_string("status", item).unwrap_or("null".to_owned())];
        let entry = self.group_map.entry(key).or_insert(0);
        *entry += 1;
    }

    pub fn finalize(&self) {
        for (keys, value) in &self.group_map {
            for key in keys {
                print!("{} - ", key);
            }
            println!("{} - ", value);
        }
    }

    pub fn apply_filters(&mut self, item: &mut T) -> bool {
        if self.query.filter.is_some() {
            let query = &self.query.clone();
            let filter = query.filter.as_ref().unwrap();
            self.evaluate_filter(filter, item)
        } else {
            true
        }
    }

    // ip = "1.1.1.1" | group method | show sum(bytes)
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

    fn get_column_value_as_string<'b>(&self, cdef: &ColumnDefinition<T>, item: &'b mut T) -> Option<String> {
        match cdef {
            ColumnDefinition::Integer { extractor, .. } => extractor(item).map(|i| i.to_string()),
            ColumnDefinition::Double { extractor, .. } => extractor(item).map(|i| i.to_string()),
            ColumnDefinition::Text { extractor, .. } => extractor(item).map(|i| i.to_string()),
            ColumnDefinition::Date { extractor, .. } => extractor(item).map(|i| i.to_string()),
            ColumnDefinition::Boolean { extractor, .. } => extractor(item).map(|i| i.to_string()),
        }
    }

    fn get_symbol_as_string<'b>(&self, symbol: &str, item: &'b mut T) -> Option<String> {
        self.get_column_value_as_string(self.get_symbol_definition(symbol), item)
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


/*
ubuntu@api-dev--001 80# cat /var/log/nginx/access.log | grep 1.1.1.1 | awk '{print $9}' | sort | uniq -c | sort -rn                                                                                                                          ~  15:02:35
6672 200
14 404
3 400
1 182

ip = "1.1.1.1" && path ~ "userid1234" | group status | show status, avg(bytes) | sort avg(bytes) | limit 100


*/
