#![allow(dead_code, unused_imports, unused_variables)]

extern crate yaml_rust;

use rusqlite::{Connection, Result, ToSql};
use std::collections::HashMap;
use std::fs;
use yaml_rust::{yaml, YamlLoader};

// See also https://stackoverflow.com/questions/40559931/vector-store-mixed-types-of-data-in-rust

#[derive(Debug)]
pub enum SQLDataType {
    Text(String),
    Integer(isize),
}

#[derive(Debug)]
struct CachedTable {
    data: Vec<Vec<SQLDataType>>,
    fields: Vec<String>,
}

impl CachedTable {
    pub fn new(field_names: Vec<String>) -> Self {
        let cached_table = CachedTable {
            data: Vec::new(),
            fields: field_names,
        };
        cached_table
    }
}

#[derive(Debug)]
pub struct DataBase {
    name: String,
    config_file: String,
    conn: Connection,
    cache: HashMap<String, CachedTable>,
    batch_size: usize,
}

impl DataBase {
    pub fn new(db_name: &str, config_file_name: &str) -> Self {
        let db_conn = Connection::open(db_name).unwrap();
        let mut db = DataBase {
            name: db_name.to_owned(),
            config_file: config_file_name.to_owned(),
            conn: db_conn,
            cache: HashMap::new(),
            batch_size: 50,
        };

        db.conn
            .execute_batch(
                "PRAGMA journal_mode = OFF;
                  PRAGMA synchronous = 0;
                  PRAGMA temp_store = MEMORY;
                  PRAGMA cache_size = 1000000;
                  PRAGMA locking_mode = EXCLUSIVE;",
            )
            .expect("PRAGMA");

        db.create_tables().unwrap();

        db.initialize_cache().unwrap();

        db
    }

    fn create_tables(&mut self) -> Result<()> {
        // let conn = Connection::open("cats.db")?;

        // println!("Creating user table!");
        self.conn.execute(
            "create table if not exists user (
                 name text,
                 age integer,
                 gender text
             )",
            [],
        )?;

        // println!("Creating scores table!");
        self.conn.execute(
            "create table if not exists scores (
                 user_name text,
                 score integer
             )",
            [],
        )?;

        // println!("Deleting data from both tables!");
        self.conn.execute("delete from user", [])?;
        self.conn.execute("delete from scores", [])?;

        println!("Database tables created / cleaned!");

        Ok(())
    }

    fn initialize_cache(&mut self) -> Result<()> {
        // db.cache.insert(
        //     String::from("user"),
        //     CachedTable {
        //         data: Vec::new(),
        //         fields: vec![
        //             String::from("Name"),
        //             String::from("Age"),
        //             String::from("Gender"),
        //         ],
        //     },
        // );
        // db.cache.insert(
        //     String::from("scores"),
        //     CachedTable {
        //         data: Vec::new(),
        //         fields: vec![String::from("user_name"), String::from("score")],
        //     },
        // );

        // get the config
        let data =
            fs::read_to_string(&self.config_file).expect("Unable to read database config file");

        let docs = YamlLoader::load_from_str(&data).unwrap();

        // multi document support, doc is a yaml::Yaml
        let doc = &docs[0];

        // process the document
        let my_filter = "tables";
        self.process_config(doc, Some(my_filter), None, 0);

        Ok(())
    }

    fn process_config(
        &mut self,
        doc: &yaml::Yaml,
        filter: Option<&str>,
        current_elem: Option<&str>,
        level: usize,
    ) {
        match *doc {
            yaml::Yaml::Array(ref v) => {
                // this is the list of fields - create the field_list vector
                let mut field_list: Vec<String> = Vec::new();
                for elem in v {
                    field_list.push(elem.as_str().unwrap().to_owned());
                }

                // create a new cashed table
                if let Some(table_name) = current_elem {
                    let cached_table = CachedTable::new(field_list);
                    self.cache.insert(table_name.to_owned(), cached_table);
                } else {
                    panic!("No table name provided, cached table cannot be added to cache! Field list provided is: {:?}", field_list);
                }
            }
            yaml::Yaml::Hash(ref h) => {
                for (k, v) in h {
                    // thesese are the tables if filtered and on level 2
                    match filter {
                        Some(f) => {
                            if f.eq_ignore_ascii_case(k.as_str().unwrap()) {
                                // remove the filter and process the items in the filtered section
                                self.process_config(v, None, Some(k.as_str().unwrap()), level + 1)
                            }
                        }
                        _ => {
                            // process the single table entries
                            self.process_config(v, filter, Some(k.as_str().unwrap()), level + 1)
                        }
                    };
                }
            }
            _ => {
                println!("process_config: Unhandled 'else case' in match *doc")
            }
        }
    }

    pub fn add_to_cache(&mut self, table_name: &str, record: Vec<SQLDataType>) {
        if let Some(chached_table) = self.cache.get_mut(table_name) {
            chached_table.data.push(record);
        } else {
            panic!("Cached table '{}' not found in cache!", table_name);
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
            let no_of_records = self.cache[table].data.len();
            if no_of_records > 0 {
                // create the field list
                let field_list = self.cache[table].fields.join(", ");
                // get the number of elements and create the params part of the SQL
                let no_elems = self.cache[table].fields.len();
                let single_param_string = format!("({}), ", vec!["?"; no_elems].join(", "));
                let mut params_string = single_param_string.repeat(no_of_records);
                let keep_chars = params_string.len() - 2;
                params_string.truncate(keep_chars);
                // create the SQL statement and prepare it
                let sql_ins = format!(
                    "INSERT INTO {} ({}) VALUES {}",
                    table, field_list, params_string
                );
                // println!("INSERT STATEMENT:\n{}", sql_ins);
                let mut stmt = self.conn.prepare_cached(sql_ins.as_str()).unwrap();

                // create the param values vector
                let mut param_values: Vec<rusqlite::types::Value> = Vec::new();
                for record in self.cache[table].data.iter() {
                    for item_value in record.iter() {
                        match item_value {
                            SQLDataType::Integer(v) => {
                                param_values.push((*v).into());
                            }
                            SQLDataType::Text(v) => {
                                param_values.push(v.clone().into());
                            }
                        }
                    }
                }
                // println!("PARAMS vector:\n{:?}", param_values);

                // fianlly executed the batch of inserts
                stmt.execute(rusqlite::params_from_iter(param_values))
                    .unwrap();

                // now clear the cached table's data
                self.cache.get_mut(table).unwrap().data.clear();
            }
        }
    }
}

#[test]
fn create_db() {
    let mut db = DataBase::new("test.db", "C:/LocalData/Rust/yaml/tabledef.yaml");
    let record: Vec<SQLDataType> = vec![
        SQLDataType::Text("John Doe".to_string()),
        SQLDataType::Integer(35),
        SQLDataType::Text("male".to_string()),
    ];
    db.add_to_cache("user", record);

    let record_1: Vec<SQLDataType> = vec![
        SQLDataType::Text("Peter Parker".to_string()),
        SQLDataType::Integer(20),
        SQLDataType::Text("male".to_string()),
    ];
    db.add_to_cache("user", record_1);

    let record_2: Vec<SQLDataType> = vec![
        SQLDataType::Text("Lois Lane".to_string()),
        SQLDataType::Integer(30),
        SQLDataType::Text("female".to_string()),
    ];
    db.add_to_cache("user", record_2);

    let record_scores: Vec<SQLDataType> = vec![
        SQLDataType::Text("Peter Parker".to_string()),
        SQLDataType::Integer(8),
    ];
    db.add_to_cache("scores", record_scores);

    db.commit_writes();

    println!("-- Records inserted into database --");
}
