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
    let definition = log::create_nginx_log_record_table_definition();
    let query = parser::parse_query(query);
    //println!("Query: {:?}", query);
    let result = query::validate_riplog_query(&query, &definition);
    result.unwrap();
    let mut evaluator = QueryEvaluator::<BinaryNginxLogRecord>::new(query, definition);

    let dir = Path::new(&dir);
    evaluate_query_log_dir(dir, &mut evaluator);
    evaluator.finalize();
}

fn evaluate_query_log_dir(dir: &Path, evaluator: &mut QueryEvaluator<BinaryNginxLogRecord>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            evaluate_query_log_dir(&path, evaluator)?;
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
            }
        }
    }
    Ok(())
}
