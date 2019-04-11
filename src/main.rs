#[macro_use]
extern crate nom;
extern crate regex;
extern crate chrono;
extern crate byteorder;
extern crate flate2;

use std::fs::{self, File};
use std::path::Path;
use std::env;
use std::io::{self, BufRead, BufReader};
use std::time::Instant;
use flate2::read::GzDecoder;

mod query;
mod nginx;
mod parser;
mod table;

use nginx::BinaryNginxLogRecord;
use query::QueryEvaluator;

fn main() { 
    let args: Vec<String> = env::args().collect();
    let start = Instant::now();
    run_query(args[2].to_string(), args[1].to_string());
    let end = Instant::now();
    println!("Duration: {:?}", end - start);
}

fn run_query(query: String, path: String) {
    let definition = nginx::create_nginx_log_record_table_definition();
    let query = parser::parse_query(query);
    let result = query::validate_riplog_query(&query, &definition);
    result.unwrap();
    let mut evaluator = QueryEvaluator::<BinaryNginxLogRecord>::new(query, definition);

    let path = Path::new(&path);
    evaluate_query_log_file_or_dir(path, &mut evaluator).unwrap();
    evaluator.finalize();
}

fn evaluate_query_log_file_or_dir(path: &Path, evaluator: &mut QueryEvaluator<BinaryNginxLogRecord>) -> io::Result<()> {
    if path.is_dir() {
        evaluate_query_log_dir(&path, evaluator)?;
    } else {
        evaluate_query_log_file(&path, evaluator)?;
    }
    Ok(())
}

fn evaluate_query_log_dir(dir: &Path, evaluator: &mut QueryEvaluator<BinaryNginxLogRecord>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        if evaluator.should_stop() {
            break;
        }
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            evaluate_query_log_dir(&path, evaluator)?;
        } else {
            evaluate_query_log_file(&path, evaluator);
        }
    }
    Ok(())
}

fn evaluate_query_log_file(file: &Path, evaluator: &mut QueryEvaluator<BinaryNginxLogRecord>) -> io::Result<()> {
    if !file.file_name().unwrap().to_str().unwrap().contains("error") && file.file_name().unwrap().to_str().unwrap().ends_with(".gz") {
        let file = File::open(file)?;
        let mut reader = BufReader::new(GzDecoder::new(file));
        let mut buf = vec![];
        let mut record = BinaryNginxLogRecord::empty();

        loop {
            if evaluator.should_stop() {
                break;
            }
            buf.clear();
            let size = reader.read_until(b'\n', &mut buf).unwrap();
            if size <= 0 {
                break;
            }
            nginx::read_log_record_binary(&buf, size, &mut record);
            evaluator.evaluate(&mut record);
        }
    } else if file.file_name().unwrap().to_str().unwrap().contains("access.log") {
        let file = File::open(file)?;
        let mut reader = BufReader::new(file);
        let mut buf = vec![];
        let mut record = BinaryNginxLogRecord::empty();
        
        loop {
            if evaluator.should_stop() {
                break;
            }
            buf.clear();
            let size = reader.read_until(b'\n', &mut buf).unwrap();
            if size <= 0 {
                break;
            }
            nginx::read_log_record_binary(&buf, size, &mut record);
            evaluator.evaluate(&mut record);
        }
    }
    Ok(())
}
