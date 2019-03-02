#[macro_use]
extern crate nom;
extern crate regex;
extern crate chrono;
extern crate byteorder;

use std::fs::{self, File};
use std::path::Path;
use std::env;
use std::io::{self, BufRead, BufReader};
use std::time::Instant;
use std::str;
use std::collections::HashMap;
use std::rc::Rc;

mod query;
mod log;
mod parser;

use log::BinaryNginxLogRecord;
use query::QueryEvaluator;

fn main() {
    let args: Vec<String> = env::args().collect();
    let start = Instant::now();
    run_query(args[2].to_string(), args[1].to_string());
    let end = Instant::now();
    println!("Duration: {:?}", end - start);
}

fn run_query(query: String, dir: String) {
    let definition = Rc::new(log::create_nginx_log_record_table_definition());
    let query = Rc::new(parser::parse_query(query));
    println!("Query: {:?}", query);
    let result = query::validate_riplog_query(&query, &definition);
    result.unwrap();
    let mut evaluator = QueryEvaluator { query: query.clone(), definition: definition.clone(), group_map: HashMap::new() };

    let dir = Path::new(&dir);
    let start = Instant::now();
    let count = evaluate_query_log_dir(dir, &mut evaluator);
    evaluator.finalize();
    let end = Instant::now();
    println!("Count: {:?}", count);
}

fn evaluate_query_log_dir(dir: &Path, evaluator: &mut QueryEvaluator<BinaryNginxLogRecord>) -> io::Result<u64> {
    let mut count = 0;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            count += evaluate_query_log_dir(&path, evaluator)?;
        } else if path.file_name().unwrap().to_str().unwrap().contains("access.log") {
            let file = File::open(path)?;
            let mut reader = BufReader::new(file);
            let mut buf = vec![];
            let mut record = BinaryNginxLogRecord::empty();
           
            loop {
                buf.clear();
                let size = reader.read_until(b'\n', &mut buf).unwrap();
                if size <= 0 {
                    break;
                }
                log::read_log_record_binary(&buf, size, &mut record);
                evaluator.evaluate(&mut record);
                // if evaluator.apply_filters(&mut record) {
                //     count += 1;
                // }
                // if record.ip == b"192.210.160.130" {
                //     count += 1;
                // }
            }
        }
    }
    Ok(count)
}

// fn search_log_dir(dir: &Path, group: &mut HashMap<Vec<u8>, u32>, regex: &Regex) -> io::Result<u64> {
//     let mut count = 0;
//     for entry in fs::read_dir(dir)? {
//         let entry = entry?;
//         let path = entry.path();

//         if path.is_dir() {
//             count += search_log_dir(&path, group, &regex)?;
//         } else if path.file_name().unwrap().to_str().unwrap().contains("access.log") {
//             let file = File::open(path)?;
//             let mut reader = BufReader::new(file);
//             let mut buf = vec![];
           
//             loop {
//                 buf.clear();
//                 let size = reader.read_until(b'\n', &mut buf).unwrap();
//                 if size <= 0 {
//                     break;
//                 }
//                 let mut record = log::read_log_record_binary(&buf, size);
//                 // if record.method == b"POST" && regex.is_match(record.parsed_method().unwrap()) {
//                 //     group.entry(record.ip.to_vec()).and_modify(|v| { *v += 1 }).or_insert(0);
//                 // }
//                 if record.ip == b"192.210.160.130" {
//                     count += 1;
//                 }
//             }
//         }
//     }
//     Ok(count)
// }


// rl '_.path = "/v1/user" | group(_.ip) | count | _.ip, count(_) | _'

// path = "/v1/user" | group(ip) | count > 100 | ip, count, sum(status)

// method = "POST" | group(ip) | ll

// f(path = "/v1/user" and code = 200) | g(ip) | f(count > 100) | s(ip, count, sum(status))

// path = "/v1/user" and code = 200 | group(ip, path)

// select ip, count(*) as req_count 
//     from logs 
// where method = 'POST' 
//     and path = '/v1/user' 
//     group by ip 
//     order by req_count desc
