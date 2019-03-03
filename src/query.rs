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

    pub fn extract_binary<'b>(&self, record: &'b T) -> Option<&'b [u8]> {
        match self {
            ColumnDefinition::Text { binary_extractor, ..} => binary_extractor(record),
            ColumnDefinition::Double { binary_extractor, ..} => binary_extractor(record),
            ColumnDefinition::Integer { binary_extractor, ..} => binary_extractor(record),
            ColumnDefinition::Boolean { binary_extractor, ..} => binary_extractor(record),
            ColumnDefinition::Date { binary_extractor, ..} => binary_extractor(record),
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
    group_map: HashMap<Vec<String>,Reducer<T>>,
    global_reducer: Reducer<T>,
    aggregate: bool,
}

impl<T> QueryEvaluator<T> {

    pub fn new<N>(query: Rc<RipLogQuery>, definition: Rc<TableDefinition<N>>) -> QueryEvaluator<N> {
        QueryEvaluator {
            query: query.clone(),
            definition: definition.clone(),
            group_map: HashMap::new(),
            global_reducer: create_reducer(&query),
            aggregate: is_aggregate_query(&query)
        }
    }

    pub fn evaluate(&mut self, item: &mut T) {
        let mut record = Record { definition: self.definition.clone(), item: item };
        if self.apply_filters(&mut record) {
            if self.aggregate {
                self.aggregate(&mut record);
            } else {
                self.print_row(&mut record);
            }
        }
    }

    fn aggregate(&mut self, record: &mut Record<T>) {
        //let key = vec![self.get_symbol_as_string("ip", record).unwrap_or("null".to_owned()), self.get_symbol_as_string("status", record).unwrap_or("null".to_owned())];
        // let key = vec![record.get_symbol_as_string("status").unwrap_or("null".to_owned())];
        // let entry = self.group_map.entry(key).or_insert(0);
        // *entry += 1;
        if self.query.grouping.is_some() {
            // todo
        } else {
            self.global_reducer.apply_record(record);
        }
    }

    pub fn finalize(&self) {
        // for (keys, value) in &self.group_map {
        //     for key in keys {
        //         print!("{} - ", key);
        //     }
        //     println!("{} - ", value);
        // }


        for field_reducer in &self.global_reducer.field_reducers {
            print!("{} - {}, ", field_reducer.get_symbol(), field_reducer.result());
        }
    }

    fn print_row(&self, record: &mut Record<T>) {
        if let Some(show) = &self.query.show {
            for element in &show.elements {
                match element {
                    QueryShowElement::Symbol(symbol) => {
                        let value = record.get_symbol_as_string(symbol);
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
                let value = record.get_column_value_as_string(coldef);
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

    pub fn apply_filters(&mut self, record: &mut Record<T>) -> bool {
        if self.query.filter.is_some() {
            let query = &self.query.clone();
            let filter = query.filter.as_ref().unwrap();
            self.evaluate_filter(filter, record)
        } else {
            true
        }
    }

    // ip = "1.1.1.1" | group method | show sum(bytes)
    fn evaluate_filter(&mut self, filter: &QueryFilter, record: &mut Record<T>) -> bool {
        match filter {
            QueryFilter::BinaryOpFilter(operand1, operand2, op) =>
                self.evaluate_binary_filter(&operand1, &operand2, op, record),
            QueryFilter::AndFilter(filter1, filter2) =>
                self.evaluate_filter(&filter1, record) && self.evaluate_filter(&filter2, record),
            QueryFilter::OrFilter(filter1, filter2) =>
                self.evaluate_filter(&filter1, record) || self.evaluate_filter(&filter2, record),
        }
    }

    fn evaluate_binary_filter(&mut self, operand1: &QueryValue, operand2: &QueryValue, op: &QueryFilterBinaryOp, record: &mut Record<T>) -> bool {
        match op {
            QueryFilterBinaryOp::Lt => self.evaluate_lt(operand1, operand2, record),
            QueryFilterBinaryOp::Gt => self.evaluate_gt(operand1, operand2, record),
            QueryFilterBinaryOp::Eq => self.evaluate_eq(operand1, operand2, record),
            QueryFilterBinaryOp::Ne => !self.evaluate_eq(operand1, operand2, record),
            QueryFilterBinaryOp::Re => self.evaluate_re(operand1, operand2, record),
            QueryFilterBinaryOp::Nr => !self.evaluate_re(operand1, operand2, record),
        }
    }

    fn evaluate_eq(&mut self, operand1: &QueryValue, operand2: &QueryValue, record: &Record<T>) -> bool {
        let op1bytes = record.resolve_byte_value(operand1);
        let op2bytes = record.resolve_byte_value(operand2);
        op1bytes.is_some() && op2bytes.is_some() && op1bytes.unwrap() == op2bytes.unwrap()
    }

    fn evaluate_lt(&mut self, operand1: &QueryValue, operand2: &QueryValue, record: &Record<T>) -> bool {
        let op1bytes = record.resolve_byte_value(operand1);
        let op2bytes = record.resolve_byte_value(operand2);
        op1bytes.is_some() && op2bytes.is_some() && op1bytes.unwrap() < op2bytes.unwrap()
    }

    fn evaluate_gt(&mut self, operand1: &QueryValue, operand2: &QueryValue, record: &Record<T>) -> bool {
        let op1bytes = record.resolve_byte_value(operand1);
        let op2bytes = record.resolve_byte_value(operand2);
        op1bytes.is_some() && op2bytes.is_some() && op1bytes.unwrap() > op2bytes.unwrap()
    }

    // TODO: Make work with arbitrary values (borrow checker woes)
    fn evaluate_re(&mut self, operand1: &QueryValue, operand2: &QueryValue, record: &mut Record<T>) -> bool {
        match (operand1, operand2) {
            (QueryValue::Symbol(symbol), QueryValue::Regex(regex)) => {
                let string_value = record.get_symbol_string(symbol);
                string_value.is_some() && regex.is_match(string_value.unwrap())
            },
            (QueryValue::Symbol(symbol), QueryValue::Text(value, _)) => {
                let string_value1 = record.get_symbol_string(symbol);
                string_value1.is_some() &&  string_value1.unwrap().contains(value)
            }
            _ => false
        }
    }
}

fn is_aggregate_query(query: &RipLogQuery) -> bool {
    query.grouping.is_some() ||
        (query.show.is_some() && query.show.as_ref().unwrap().elements.iter().any(|e| e.is_reducer()))
}

fn create_reducer<T>(query: &RipLogQuery) -> Reducer<T> {
    if query.show.is_some() {
        let mut field_reducers: Vec<Box<FieldReducer<T>>> = Vec::new();
        for element in &query.show.as_ref().unwrap().elements {
            match element {
                QueryShowElement::Reducer(QueryReducer::Count, symbol) =>
                    field_reducers.push(Box::new(CountReducer { symbol: symbol.to_owned(), count: 0 })),
                QueryShowElement::Reducer(QueryReducer::Sum, symbol) =>
                    field_reducers.push(Box::new(SumReducer { symbol: symbol.to_owned(), sum: 0 })),
                QueryShowElement::Reducer(QueryReducer::Max, symbol) =>
                    field_reducers.push(Box::new(MaxReducer { symbol: symbol.to_owned(), max: 0 })),
                QueryShowElement::Reducer(QueryReducer::Avg, symbol) =>
                    field_reducers.push(Box::new(AvgReducer { symbol: symbol.to_owned(), count: 0, sum: 0 })),
                _ => (),
            }
        }
        Reducer { field_reducers }
    } else {
        Reducer { field_reducers: Vec::with_capacity(0) }
    }
}

type Result<T> = result::Result<T, QueryValidationError>;

#[derive(Debug, Clone)]
pub struct QueryValidationError { msg: String }

struct Record<'i, T> {
    item: &'i mut T,
    definition: Rc<TableDefinition<T>>,
}

impl<'i, T> Record<'i, T> {

    fn get_symbol_bytes<'b>(&'b self, symbol: &str) -> Option<&'b [u8]> {
        get_symbol_definition(&self.definition, symbol).extract_binary(&self.item)
    }

    fn resolve_byte_value<'a>(&'a self, value: &'a QueryValue) -> Option<&'a [u8]> {
        match value {
            QueryValue::Text(_, bytes) => Some(bytes),
            QueryValue::Int(_, bytes) => Some(bytes),
            QueryValue::Double(_, bytes) => Some(bytes),
            QueryValue::Null => Some(EMPTY_BYTES),
            QueryValue::Symbol(symbol) => self.get_symbol_bytes(symbol),
            QueryValue::Date(date) => None,
            _ => None
        }
    }

    fn resolve_string_value<'a>(&'a mut self, value: &'a QueryValue) -> Option<&'a str> {
        match value {
            QueryValue::Text(value, _) => Some(&value),
            QueryValue::Symbol(symbol) => self.get_symbol_string(symbol),
            _ => None
        }
    }

    fn get_symbol_string<'b>(&'b mut self, symbol: &str) -> Option<&'b str> {
        match get_symbol_definition(&self.definition, symbol) {
            ColumnDefinition::Text { extractor, .. } => extractor(self.item),
            _ => None
        }
    }

    fn get_column_value_as_string(&mut self, cdef: &ColumnDefinition<T>) -> Option<String> {
        get_column_value_as_string(cdef, self.item)
    }

    fn get_symbol_as_string(&mut self, symbol: &str) -> Option<String> {
        get_symbol_as_string(&self.definition, self.item, symbol)
    }

    fn get_symbol_as_integer(&mut self, symbol: &str) -> Option<u64> {
        get_symbol_as_integer(&self.definition, self.item, symbol)
    }
}

fn get_symbol_definition<'a, T>(tdef: &'a TableDefinition<T>, symbol: &str) -> &'a ColumnDefinition<T> {
    tdef.column_map.get(symbol).unwrap()
}

fn get_symbol_as_string<T>(tdef: &TableDefinition<T>, item: &mut T, symbol: &str) -> Option<String> {
    let definition = get_symbol_definition(tdef, symbol);
    get_column_value_as_string(definition, item)
}

fn get_symbol_as_integer<T>(tdef: &TableDefinition<T>, item: &mut T, symbol: &str) -> Option<u64> {
    let definition = get_symbol_definition(tdef, symbol);
    get_column_value_as_integer(definition, item)
}

fn get_column_value_as_string<T>(cdef: &ColumnDefinition<T>, item: &mut T) -> Option<String> {
    match cdef {
        ColumnDefinition::Integer { extractor, .. } => extractor(item).map(|i| i.to_string()),
        ColumnDefinition::Double { extractor, .. } => extractor(item).map(|i| i.to_string()),
        ColumnDefinition::Text { extractor, .. } => extractor(item).map(|i| i.to_string()),
        ColumnDefinition::Date { extractor, .. } => extractor(item).map(|i| i.to_string()),
        ColumnDefinition::Boolean { extractor, .. } => extractor(item).map(|i| i.to_string()),
    }
}

fn get_column_value_as_integer<T>(cdef: &ColumnDefinition<T>, item: &mut T) -> Option<u64> {
    match cdef {
        ColumnDefinition::Integer { extractor, .. } => extractor(item),
        _ => None
    }
}

struct Reducer<T> {
    field_reducers: Vec<Box<FieldReducer<T>>>
}

impl<T> Reducer<T> {
    fn apply_record(&mut self, record: &mut Record<T>) {
        for reducer in &mut self.field_reducers {
            reducer.apply_record(record);
        }
    }
}

trait FieldReducer<T> {
    fn apply_record(&mut self, record: &mut Record<T>);
    fn result(&self) -> u64;
    fn get_symbol(&self) -> &str;
}
            
struct CountReducer {
    symbol: String,
    count: u64,
}

impl<T> FieldReducer<T> for CountReducer {
    fn apply_record(&mut self, record: &mut Record<T>) {
        if self.symbol == "*" {
            self.count += 1;
        } else {
            let value = record.get_symbol_bytes(&self.symbol);
            if value.is_some() {
                self.count += 1;
            }
        }
    }

    fn result(&self) -> u64 {
        self.count
    }

    fn get_symbol(&self) -> &str {
        &self.symbol
    }
}
            
struct SumReducer {
    symbol: String,
    sum: u64
}

impl<T> FieldReducer<T> for SumReducer {
    fn apply_record(&mut self, record: &mut Record<T>) {
        let value = record.get_symbol_as_integer(&self.symbol);
        if value.is_some() {
            self.sum += value.unwrap();
        }
    }

    fn result(&self) -> u64 {
        self.sum
    }

    fn get_symbol(&self) -> &str {
        &self.symbol
    }
}

struct AvgReducer {
    symbol: String,
    count: u64,
    sum: u64
}

impl<T> FieldReducer<T> for AvgReducer {
    fn apply_record(&mut self, record: &mut Record<T>) {
        let value = record.get_symbol_as_integer(&self.symbol);
        if value.is_some() {
            self.sum += value.unwrap();
            self.count += 1;
        }
    }

    fn result(&self) -> u64 {
        if self.count > 0 {
            self.sum / self.count
        } else {
            0
        }
    }
    
    fn get_symbol(&self) -> &str {
        &self.symbol
    }
}

struct MaxReducer {
    symbol: String,
    max: u64
}

impl<T> FieldReducer<T> for MaxReducer {
    fn apply_record(&mut self, record: &mut Record<T>) {
        let value = record.get_symbol_as_integer(&self.symbol);
        if value.is_some() && value.unwrap() > self.max {
            self.max = value.unwrap();
        }
    }

    fn result(&self) -> u64 {
        self.max
    }

    fn get_symbol(&self) -> &str {
        &self.symbol
    }
}

/*
ubuntu@api-dev--001 80# cat /var/log/nginx/access.log | grep 1.1.1.1 | awk '{print $9}' | sort | uniq -c | sort -rn                                                                                                                          ~  15:02:35
6672 200
14 404
3 400
1 182

ip = "1.1.1.1" && path ~ "userid1234" | group status | show status, avg(bytes) | sort avg(bytes) | limit 100


*/
