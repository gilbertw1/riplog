use std::str;
use std::collections::HashMap;

use chrono::prelude::*;
use query::{TableDefinition, ColumnDefinition};
use byteorder::{BigEndian, ReadBytesExt};

pub fn read_log_record_binary(buf: &Vec<u8>, len: usize, record: &mut BinaryNginxLogRecord) {
    let empty: &[u8] = &[];
    let working = &buf[0..len];

    let space_idx = index_of(working, b' ').unwrap();
    let ip = &working[0..space_idx];
    let working = &working[space_idx+1..working.len()];

    let space_idx = index_of(working, b' ').unwrap();
    let working = &working[space_idx+1..working.len()];
    let space_idx = index_of(working, b' ').unwrap();
    let working = &working[space_idx+1..working.len()];

    let brace_idx = index_of(working, b']').unwrap();
    let date = &working[1..brace_idx];
    let working = &working[brace_idx+3..working.len()];

    let quote_idx = index_of(working, b'"').unwrap();
    let request = &working[0..quote_idx];
    let working = &working[quote_idx+2..working.len()];

    let req_space_idx = index_of(request, b' ');
    let (method, path) =
        if req_space_idx.is_some() {
            let method = &request[0..req_space_idx.unwrap()];
            let req_working = &request[req_space_idx.unwrap()+1..request.len()];
            let req_space_idx = index_of(req_working, b' ');
            let path =
                if req_space_idx.is_some() {
                    &req_working[0..req_space_idx.unwrap()]
                } else {
                    req_working
                };
            (method, path)
        } else {
            (empty, request)
        };
    
    let space_idx = index_of(working, b' ').unwrap();
    let status = &working[0..space_idx];
    let working = &working[space_idx+1..working.len()];

    let space_idx = index_of(working, b' ').unwrap();
    let bytes = &working[0..space_idx];
    let working = &working[space_idx+1..working.len()];

    let space_idx = index_of(working, b' ').unwrap();

    let referrer = &working[1..space_idx-1];
    let working = &working[space_idx+1..working.len()];

    let user_agent = &working[1..working.len()-1];

    record.ip = ip.to_vec();
    record.date = date.to_vec();
    record.method = method.to_vec();
    record.path = path.to_vec();
    record.status = status.to_vec();
    record.bytes = bytes.to_vec();
    record.referrer = referrer.to_vec();
    record.user_agent = user_agent.to_vec();

    record.parsed_record.ip = None;
    record.parsed_record.date = None;
    record.parsed_record.method = None;
    record.parsed_record.path = None;
    record.parsed_record.status = None;
    record.parsed_record.bytes = None;
    record.parsed_record.referrer = None;
    record.parsed_record.user_agent = None;
}

fn index_of(vec: &[u8], char: u8) -> Option<usize> {
    let mut idx = 0;
    let mut found_idx = None;
    while idx < vec.len() {
        if vec[idx] == char {
            found_idx = Some(idx);
            break;
        }
        idx += 1;
    }
    found_idx
}

pub fn parse_log_record(line: &str) -> NginxLogRecord {
    unsafe {
        let space_idx = line.find(" ").unwrap();
        let ip = line.get_unchecked(0..space_idx);
        let rest = line.get_unchecked(space_idx+1..line.len());
        
        let space_idx = rest.find(" ").unwrap();
        let rest = rest.get_unchecked(space_idx+1..rest.len());
        let space_idx = rest.find(" ").unwrap();
        let rest = rest.get_unchecked(space_idx+1..rest.len());

        let brace_idx = rest.find("]").unwrap();
        let date = rest.get_unchecked(1..brace_idx);
        let rest = rest.get_unchecked(brace_idx+3..line.len());
        
        let quote_idx = rest.find("\"").unwrap();
        let request = rest.get_unchecked(0..quote_idx);
        let rest = rest.get_unchecked(quote_idx+2..rest.len());

        let req_space_idx = request.find(" ");
        let (method,path) =
            if req_space_idx.is_some() {
                let method = request.get_unchecked(0..req_space_idx.unwrap());
                let req_rest = request.get_unchecked(req_space_idx.unwrap()+1..request.len());
                let req_space_idx = req_rest.find(" ");
                let path =
                    if req_space_idx.is_some() {
                        req_rest.get_unchecked(0..req_space_idx.unwrap())
                    } else {
                        req_rest
                    };
                (Some(method), path)
            } else {
                (None, request)
            };

        let space_idx = rest.find(" ").unwrap();
        let status = rest.get_unchecked(0..space_idx);
        let rest = rest.get_unchecked(space_idx+1..rest.len());

        let space_idx = rest.find(" ").unwrap();
        let bytes_sent = rest.get_unchecked(0..space_idx);
        let rest = rest.get_unchecked(space_idx+1..rest.len());

        let space_idx = rest.find(" ").unwrap();
        let referrer = rest.get_unchecked(1..space_idx-1);
        let rest = rest.get_unchecked(space_idx+1..rest.len());

        let user_agent = rest.get_unchecked(1..rest.len()-1);

        NginxLogRecord {
            ip: ip,
            date: DateTime::parse_from_str(date, "%d/%b/%Y:%H:%M:%S %z").unwrap(),
            method: method,
            path: path,
            status: if is_empty(status) { None } else { Some(status.parse::<u64>().unwrap()) },
            bytes: if is_empty(bytes_sent) { None } else { Some(bytes_sent.parse::<u64>().unwrap()) },
            referrer: if is_empty(referrer) { None } else { Some(referrer) },
            user_agent: if is_empty(user_agent) { None } else { Some(user_agent) },
        }
    }
}

fn is_empty(value: &str) -> bool {
    value == "-" || value == "\"-\""
}

fn empty_opt(bytes: &[u8]) -> Option<&[u8]> {
    if bytes.len() < 1 {
        None
    } else {
        Some(bytes)
    }
}

#[derive(Debug, Clone)]
pub struct NginxLogRecord<'a> {
    ip: &'a str,
    date: DateTime<FixedOffset>,
    method: Option<&'a str>,
    path: &'a str,
    status: Option<u64>,
    bytes: Option<u64>,
    referrer: Option<&'a str>,
    user_agent: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct BinaryNginxLogRecord {
    pub ip: Vec<u8>,
    pub date: Vec<u8>,
    pub method: Vec<u8>,
    pub path: Vec<u8>,
    pub status: Vec<u8>,
    pub bytes: Vec<u8>,
    pub referrer: Vec<u8>,
    pub user_agent: Vec<u8>,
    parsed_record: ParsedNginxLogRecord,
}

impl BinaryNginxLogRecord {
    pub fn empty() -> BinaryNginxLogRecord {
        BinaryNginxLogRecord {
            ip: Vec::new(),
            date: Vec::new(),
            method: Vec::new(),
            path: Vec::new(),
            status: Vec::new(),
            bytes: Vec::new(),
            referrer: Vec::new(),
            user_agent: Vec::new(),
            parsed_record: ParsedNginxLogRecord::empty(),
        }
    }

    pub fn parsed_ip(&mut self) -> &str {
        unsafe {
            if self.parsed_record.ip.is_some() {
                &self.parsed_record.ip.as_ref().unwrap()
            } else {
                self.parsed_record.ip = Some(String::from_utf8_unchecked(self.ip.clone()));
                &self.parsed_record.ip.as_ref().unwrap()
            }
        }
    }

    pub fn parsed_date(&mut self) -> DateTime<FixedOffset> {
        unsafe {
            if self.parsed_record.date.is_some() {
                self.parsed_record.date.unwrap()
            } else {
                self.parsed_record.date = DateTime::parse_from_str(&String::from_utf8_unchecked(self.date.clone()), "%d/%b/%Y:%H:%M:%S %z").ok();
                self.parsed_record.date.unwrap()
            }
        }
    }

    pub fn parsed_method(&mut self) -> Option<&str> {
        unsafe {
            if self.parsed_record.method.is_some() {
                self.parsed_record.method.as_ref().unwrap().as_ref().map(|s| s.as_str())
            } else {
                self.parsed_record.method =
                    if self.method.len() < 1 { Some(None) }
                else { Some(Some(String::from_utf8_unchecked(self.method.clone()))) };
                self.parsed_record.method.as_ref().unwrap().as_ref().map(|s| s.as_str())
            }
        }
    }

    pub fn parsed_path(&mut self) -> &str {
        unsafe {
            if self.parsed_record.path.is_some() {
                &self.parsed_record.path.as_ref().unwrap()
            } else {
                self.parsed_record.path = Some(String::from_utf8_unchecked(self.path.clone()));
                &self.parsed_record.path.as_ref().unwrap()
            }
        }
    }

    pub fn parsed_status(&mut self) -> Option<u64> {
        unsafe {
            if self.parsed_record.status.is_some() {
                self.parsed_record.status.unwrap()
            } else {
                self.parsed_record.status =
                    if self.status.len() < 1 { Some(None) }
                else { Some(String::from_utf8_unchecked(self.status.clone()).parse::<u64>().ok()) };
                self.parsed_record.status.unwrap()
            }
        }
    }

    pub fn parsed_bytes(&mut self) -> Option<u64> {
        unsafe {
            if self.parsed_record.bytes.is_some() {
                self.parsed_record.bytes.unwrap()
            } else {
                self.parsed_record.bytes =
                    if self.bytes.len() < 1 { Some(None) }
                else { Some(String::from_utf8_unchecked(self.bytes.clone()).parse::<u64>().ok()) };
                self.parsed_record.status.unwrap()
            }
        }
    }

    pub fn parsed_referrer(&mut self) -> Option<&str> {
        unsafe {
            if self.parsed_record.referrer.is_some() {
                self.parsed_record.referrer.as_ref().unwrap().as_ref().map(|s| s.as_str())
            } else {
                self.parsed_record.referrer =
                    if self.referrer.len() < 1 { Some(None) }
                else { Some(Some(String::from_utf8_unchecked(self.referrer.clone()))) };
                self.parsed_record.referrer.as_ref().unwrap().as_ref().map(|s| s.as_str())
            }
        }
    }

    pub fn parsed_user_agent(&mut self) -> Option<&str> {
        unsafe {
            if self.parsed_record.user_agent.is_some() {
                self.parsed_record.user_agent.as_ref().unwrap().as_ref().map(|s| s.as_str())
            } else {
                self.parsed_record.user_agent =
                    if self.user_agent.len() < 1 { Some(None) }
                else { Some(Some(String::from_utf8_unchecked(self.user_agent.clone()))) };
                self.parsed_record.user_agent.as_ref().unwrap().as_ref().map(|s| s.as_str())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParsedNginxLogRecord {
    ip: Option<String>,
    date: Option<DateTime<FixedOffset>>,
    method: Option<Option<String>>,
    path: Option<String>,
    status: Option<Option<u64>>,
    bytes: Option<Option<u64>>,
    referrer: Option<Option<String>>,
    user_agent: Option<Option<String>>,
}

impl ParsedNginxLogRecord {
    pub fn empty() -> ParsedNginxLogRecord {
        ParsedNginxLogRecord {
            ip: None,
            date: None,
            method: None,
            path: None,
            status: None,
            bytes: None,
            referrer: None,
            user_agent: None,
        }
    }
}

pub fn create_nginx_log_record_table_definition<'a>() -> TableDefinition<BinaryNginxLogRecord> {
    let columns = vec![
            ColumnDefinition::Text { name: "ip",
                                     binary_extractor: Box::new(|r: &BinaryNginxLogRecord| empty_opt(&r.ip)),
                                     extractor: Box::new(|r: &mut BinaryNginxLogRecord| Some(r.parsed_ip())) },
            ColumnDefinition::Date { name: "date",
                                     binary_extractor: Box::new(|r: &BinaryNginxLogRecord| empty_opt(&r.date)),
                                     extractor: Box::new(|r: &mut BinaryNginxLogRecord| Some(r.parsed_date())) },
            ColumnDefinition::Text { name: "method",
                                     binary_extractor: Box::new(|r: &BinaryNginxLogRecord| empty_opt(&r.method)),
                                     extractor: Box::new(|r: &mut BinaryNginxLogRecord| r.parsed_method()) },
            ColumnDefinition::Text { name: "path",
                                     binary_extractor: Box::new(|r: &BinaryNginxLogRecord| empty_opt(&r.path)),
                                     extractor: Box::new(|r: &mut BinaryNginxLogRecord| Some(r.parsed_path())) },
            ColumnDefinition::Integer { name: "status",
                                        binary_extractor: Box::new(|r: &BinaryNginxLogRecord| empty_opt(&r.status)),
                                        extractor: Box::new({ |r: &mut BinaryNginxLogRecord| r.parsed_status() }) },
            ColumnDefinition::Integer { name: "bytes",
                                        binary_extractor: Box::new(|r: &BinaryNginxLogRecord| empty_opt(&r.bytes)),
                                        extractor: Box::new({ |r: &mut BinaryNginxLogRecord| r.parsed_bytes() }) },
            ColumnDefinition::Text { name: "referrer",
                                     binary_extractor: Box::new(|r: &BinaryNginxLogRecord| empty_opt(&r.referrer)),
                                     extractor: Box::new(|r: &mut BinaryNginxLogRecord| r.parsed_referrer()) },
            ColumnDefinition::Text { name: "user_agent",
                                     binary_extractor: Box::new(|r: &BinaryNginxLogRecord| empty_opt(&r.user_agent)),
                                     extractor: Box::new(|r: &mut BinaryNginxLogRecord| r.parsed_user_agent()) },
        ];

    let mut column_map = HashMap::new();

    for c in columns {
        column_map.insert(c.name().to_string(), c);
    }

    TableDefinition {
        column_map: column_map,
    }
}
