use std::result;
use std::collections::HashMap;
use std::rc::Rc;
use chrono::prelude::*;
use parser::*;

const EMPTY_BYTES: &[u8] = &[];

pub struct TableDefinition<T> {
    pub column_map: HashMap<String, ColumnDefinition<T>>,
    pub ordered_columns: Vec<String>,
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
    query: Rc<RipLogQuery>,
    definition: Rc<TableDefinition<T>>,
    group_map: HashMap<Vec<String>,Reducer<T>>,
    global_reducer: Reducer<T>,
    aggregate: bool,
    record_formatter: RecordFormatter<T>,
}

impl<T> QueryEvaluator<T> {

    pub fn new<N>(query: RipLogQuery, definition: TableDefinition<N>) -> QueryEvaluator<N> {
        let mut rquery = query;
        rquery.compute_show(&definition);
        let query_rc = Rc::new(rquery);
        let mut evaluator =
            QueryEvaluator {
                query: query_rc.clone(),
                definition: Rc::new(definition),
                group_map: HashMap::new(),
                global_reducer: create_reducer(&query_rc),
                aggregate: is_aggregate_query(&query_rc),
                record_formatter: RecordFormatter::new(&query_rc),
            };
        if !evaluator.aggregate {
            evaluator.record_formatter.format_header_row();
        }
        evaluator
    }

    pub fn evaluate(&mut self, item: &mut T) {
        let mut record = Record { definition: self.definition.clone(), item: item };
        if self.apply_filters(&mut record) {
            if self.aggregate {
                self.aggregate(&mut record);
            } else {
                self.record_formatter.format_record(&mut record);
            }
        }
    }

    fn aggregate(&mut self, record: &mut Record<T>) {
        if self.query.grouping.is_some() {
            // todo
            let key = create_group_key(&self.query.grouping.as_ref().unwrap().groupings, record);
            let entry = self.group_map.entry(key).or_insert(create_reducer(&self.query));
            entry.apply_record(record);
        } else {
            self.global_reducer.apply_record(record);
        }
    }

    pub fn finalize(&mut self) {
        if self.aggregate {
            self.record_formatter.format_header_row();
            if self.query.grouping.is_some() {
                for (keys, reducer) in &self.group_map {
                    self.record_formatter.format_grouped_record(keys, reducer);
                }
            } else {
                self.record_formatter.format_reduced_record(&self.global_reducer);
            }
        }
        self.record_formatter.format_closing_row();
    }

    fn apply_filters(&mut self, record: &mut Record<T>) -> bool {
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

fn create_group_key<T>(groupings: &Vec<String>, record: &mut Record<T>) -> Vec<String> {
    let mut key = Vec::with_capacity(groupings.len());
    for grouping in groupings {
        key.push(record.get_symbol_as_string(grouping).unwrap_or("null".to_owned()));
    }
    key
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
            
#[derive(Debug, Clone)]
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
            
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

struct ResultsPrinter<T> {
    definition: Rc<TableDefinition<T>>,
    query: RipLogQuery,
}

impl<T> ResultsPrinter<T> {

    fn print_header_row(&self) {
        if self.query.show.is_some() {
            
        } else if self.query.grouping.is_some() {

        } else {
            
        }
    }
}

struct RecordFormatter<T> {
    fields: Vec<Box<OutputField<T>>>
}

impl<T> RecordFormatter<T> {

    pub fn new(query: &RipLogQuery) -> RecordFormatter<T> {
        let mut fields: Vec<Box<OutputField<T>>> = Vec::new();
        for element in &query.computed_show.as_ref().unwrap().elements {
            match element {
                QueryShowElement::Symbol(symbol) => {
                    let group_idx = get_group_idx(&symbol, query);
                    if group_idx.is_some() {
                        fields.push(Box::new(GroupOutputField { symbol: symbol.clone(), idx: group_idx.unwrap(), size: 10 }));
                    } else {
                        fields.push(Box::new(SymbolOutputField { symbol: symbol.clone(), size: 10 }));
                    }
                },
                QueryShowElement::Reducer(reducer, symbol) => {
                    let reduce_idx = get_reduce_idx(&symbol, &reducer, query);
                    if reduce_idx.is_some() {
                        fields.push(Box::new(ReducedOutputField { reducer: reducer.to_string().to_owned(), symbol: symbol.clone(), idx: reduce_idx.unwrap(), size: 10 }));
                    }
                }
                _ => ()
            }
        }
        RecordFormatter { fields }
    }
    
    pub fn format_record(&mut self, record: &mut Record<T>) {
        print!("|");
        for field in &mut self.fields {
            print!("{}|", field.format_field(Some(record), None, None));
        }
        println!("");
    }

    pub fn format_grouped_record(&mut self, key: &Vec<String>, reducer: &Reducer<T>) {
        print!("|");
        for field in &mut self.fields {
            print!("{}|", field.format_field(None, Some(key), Some(reducer)));
        }
        println!("");
    }

    pub fn format_reduced_record(&mut self, reducer: &Reducer<T>) {
        print!("|");
        for field in &mut self.fields {
            print!("{}|", field.format_field(None, None, Some(reducer)));
        }
        println!("");
    }

    pub fn format_header_row(&mut self) {
        let mut header_row = "|".to_owned();
        for field in &mut self.fields {
            header_row += &format!("{}|", field.header());
        }
        let pad = (0..header_row.len()-2).map(|_| "-").collect::<String>();
        println!("+{}+", pad);
        println!("{}", header_row);
        println!("|{}|", pad);
    }

    pub fn format_closing_row(&mut self) {
        let mut len = 1;
        for field in &mut self.fields {
            len += field.size()+3
        }
        let pad = (0..len-2).map(|_| "-").collect::<String>();
        println!("+{}+", pad);
    }
}

// TODO: better way to line up indexes
fn get_group_idx(symbol: &str, query: &RipLogQuery) -> Option<usize> {
    if query.grouping.is_some() {
        let mut idx = 0;
        let mut found_idx: Option<usize> = None;
        for group in &query.grouping.as_ref().unwrap().groupings {
            if group == symbol {
                found_idx = Some(idx);
                break;
            }
            idx += 1;
        }
        found_idx
    } else {
        None
    }
}

// TODO: better way to line up indexes
fn get_reduce_idx(symbol: &str, reducer: &QueryReducer, query: &RipLogQuery) -> Option<usize> {
    if query.show.is_some() {
        let mut idx = 0;
        let mut found_idx: Option<usize> = None;
        for element in query.show.as_ref().unwrap().elements.iter().filter(|e| e.is_reducer()) {
            match element {
                QueryShowElement::Reducer(curr_reducer, curr_symbol) => {
                    if curr_reducer.to_string() == reducer.to_string() && (symbol == "*" || curr_symbol == symbol) {
                        found_idx = Some(idx);
                        break;
                    }
                },
                _ => ()
            }
            idx += 1;
        }
        found_idx
    } else {
        None
    }
}

trait OutputField<T> {
    fn header(&mut self) -> String;
    fn format_field(&mut self, record: Option<&mut Record<T>>, group_key: Option<&Vec<String>>, reducer: Option<&Reducer<T>>) -> String;
    fn size(&self) -> usize;
}

struct SymbolOutputField {
    symbol: String,
    size: usize,
}

impl<T> OutputField<T> for SymbolOutputField {
    fn header(&mut self) -> String {
        if self.size < self.symbol.len()+2 {
            self.size = self.symbol.len()+2;
        }
        format!(" {:width$} ", self.symbol, width = self.size)
    }

    fn format_field(&mut self, record: Option<&mut Record<T>>, group_key: Option<&Vec<String>>, reducer: Option<&Reducer<T>>) -> String {
        let output =
            if record.is_some() {
                record.unwrap().get_symbol_as_string(&self.symbol).unwrap_or("null".to_owned())
            } else {
                "null".to_owned()
            };
        if self.size < output.len()+2 && self.size < 50 {
            self.size = output.len()+2;
        }
        format!(" {:width$} ", output, width = self.size)
    }

    fn size(&self) -> usize {
        self.size
    }
}

struct GroupOutputField {
    symbol: String,
    idx: usize,
    size: usize,
}

impl<T> OutputField<T> for GroupOutputField {
    fn header(&mut self) -> String {
        if self.size < self.symbol.len()+2 {
            self.size = self.symbol.len()+2;
        }
        format!(" {:width$} ", self.symbol, width = self.size)
    }

    fn format_field(&mut self, record: Option<&mut Record<T>>, group_key: Option<&Vec<String>>, reducer: Option<&Reducer<T>>) -> String {
        let output =
            if group_key.is_some() && group_key.unwrap().len() >= (self.idx+1) {
                group_key.unwrap()[self.idx].clone()
            } else {
                "null".to_owned()
            };
        if self.size < output.len()+2 && self.size < 50 {
            self.size = output.len()+2;
        }
        format!(" {:width$} ", output, width = self.size)
    }

    fn size(&self) -> usize {
        self.size
    }
}

struct ReducedOutputField {
    reducer: String,
    symbol: String,
    idx: usize,
    size: usize,
}

impl<T> OutputField<T> for ReducedOutputField {
    fn header(&mut self) -> String {
        let name = format!("{}({})", self.reducer, self.symbol);
        if self.size < name.len()+2 {
            self.size = name.len()+2;
        }
        format!(" {:width$} ", name, width = self.size)
    }

    fn format_field(&mut self, record: Option<&mut Record<T>>, group_key: Option<&Vec<String>>, reducer: Option<&Reducer<T>>) -> String {
        let output =
            if reducer.is_some() && reducer.unwrap().field_reducers.len() >= (self.idx+1) {
                reducer.unwrap().field_reducers[self.idx].result().to_string()
            } else {
                "null".to_owned()
            };
        if self.size < output.len()+2 && self.size < 50 {
            self.size = output.len()+2;
        }
        format!(" {:width$} ", output, width = self.size)
    }

    fn size(&self) -> usize {
        self.size
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
