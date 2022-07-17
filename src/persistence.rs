#![allow(dead_code, unused_imports, unused_variables)]

use rusqlite::{Connection, Result, ToSql};
use std::cell::RefCell;
use std::collections::HashMap;

// See also https://stackoverflow.com/questions/40559931/vector-store-mixed-types-of-data-in-rust

#[derive(Debug)]
pub enum SQLDataType {
    Text(String),
    Integer(isize),
}

#[derive(Debug)]
struct CachedTable {
    data: RefCell<Vec<Vec<SQLDataType>>>,
    fields: Vec<String>,
}

#[derive(Debug)]
pub struct DataBase {
    name: String,
    conn: Connection,
    cache: HashMap<String, CachedTable>,
    batch_size: usize,
}

impl DataBase {
    pub fn new(db_name: &str) -> Self {
        let db_conn = Connection::open(db_name).unwrap();
        let mut db = DataBase {
            name: db_name.to_owned(),
            conn: db_conn,
            cache: HashMap::new(),
            batch_size: 50,
        };

        db.cache.insert(
            String::from("Table1"),
            CachedTable {
                data: RefCell::new(Vec::new()),
                fields: vec![
                    String::from("Field11"),
                    String::from("Field12"),
                    String::from("Field13"),
                ],
            },
        );
        db.cache.insert(
            String::from("Table2"),
            CachedTable {
                data: RefCell::new(Vec::new()),
                fields: vec![
                    String::from("Field21"),
                    String::from("Field22"),
                    String::from("Field23"),
                ],
            },
        );
        db
    }

    fn add_to_cache(&mut self, table_name: &str, record: Vec<SQLDataType>) {
        if let Some(chached_table) = self.cache.get_mut(table_name) {
            chached_table.data.borrow_mut().push(record);
        }
    }

    pub fn commit_writes(&mut self) {
        // collect all keys to then iterate over the cache
        // collecting all keys avoids the "move issue" of iterators
        // over a mutable reference to the 'cache' HashMap
        let mut tables: Vec<String> = Vec::new();
        for key in self.cache.keys() {
            tables.push(key.to_owned());
        }
        // process all cached tables and write to the DB
        for table in &tables {
            // only process cached tables that do contain data
            let no_of_records = self.cache[table].data.borrow().len();
            if no_of_records > 0 {
                // create the field list
                let field_list = self.cache[table].fields.join(", ");
                // get the number of elements and create the params part of the SQL
                let no_elems = self.cache[table].fields.len();
                let params_string = vec!["?"; no_elems].join(", ").repeat(no_of_records);
                // create the SQL statement and prepare it
                let sql_ins = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    table, field_list, params_string
                );
                let stmt = self.conn.prepare_cached(sql_ins.as_str()).unwrap();

                // create the param values vector
                let mut param_values: Vec<_> = Vec::new();
                let mut int_value: isize = 0;
                let mut string_value: String = "default".to_string();
                for record in self.cache[table].data.borrow().iter() {
                    for item_value in record.iter() {
                        match item_value {
                            SQLDataType::Integer(v) => {
                                int_value = *v;
                                param_values.push(&int_value as &dyn ToSql);
                            }
                            SQLDataType::Text(v) => {
                                string_value = *v;
                                param_values.push(&string_value as &dyn ToSql);
                            }
                        }
                    }
                }

                // fianlly executed the batch of inserts
                stmt.execute(&*param_values).unwrap();

                // now clear the cached table's data
                self.cache[table].data.borrow_mut().clear();
            }
        }
    }
}
