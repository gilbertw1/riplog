use std::collections::HashMap;
use chrono::prelude::*;

pub struct TableDefinition<T> {
    pub column_map: HashMap<String, ColumnDefinition<T>>,
    pub ordered_columns: Vec<String>,
}

pub enum ColumnDefinition<T> {
    Integer { name: &'static str,
              size: usize,
              binary_extractor: Box<Fn(&T) -> Option<&[u8]>>,
              extractor: Box<Fn(&mut T) -> Option<u64>> },
    Double { name: &'static str,
             size: usize,
             binary_extractor: Box<Fn(&T) -> Option<&[u8]>>,
             extractor: Box<Fn(&mut T) -> Option<f64>> },
    Text { name: &'static str,
           size: usize,
           binary_extractor: Box<Fn(&T) -> Option<&[u8]>>,
           extractor: Box<Fn(&mut T) -> Option<&str>> },
    Date { name: &'static str,
           size: usize,
           binary_extractor: Box<Fn(&T) -> Option<&[u8]>>,
           extractor: Box<Fn(&mut T) -> Option<&DateTime<Local>>> },
    Boolean { name: &'static str,
              size: usize,
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

    pub fn get_size(&self) -> &usize {
        match self {
            ColumnDefinition::Text { size, ..} => size,
            ColumnDefinition::Double { size, ..} => size,
            ColumnDefinition::Integer { size, ..} => size,
            ColumnDefinition::Boolean { size, ..} => size,
            ColumnDefinition::Date { size, ..} => size,
        }
    }
}
