#![allow(dead_code, unused_imports, unused_variables)]

use rusqlite::{named_params, Connection, Result, ToSql};
use serde_json;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use yaml_rust::{yaml, YamlLoader};

use super::xmlparser;

// See also https://stackoverflow.com/questions/40559931/vector-store-mixed-types-of-data-in-rust

#[derive(Debug, Copy, Clone)]
pub enum IndexGroup {
    ProcessLog = 0,
    DocStore = 1,
    XmlStore = 2,
}

#[derive(Debug, Copy, Clone)]
pub enum LogLevel {
    Info = 0,
    Warning = 1,
    Error = 2,
    All = 4,
}

#[derive(Debug, Copy, Clone)]
pub enum DocValidity {
    Valid = 1,
    Invalid = 0,
}

#[derive(Debug, Copy, Clone)]
pub enum XmlAttribute {
    DocID = 1,
    Type = 2,
    ParsedXml = 3,
    SoupNoOfTags = 4,
    SourceNoOfTags = 5,
    Tags = 6,
    TagsAndValues = 7,
    TopNode = 8,
}

impl fmt::Display for XmlAttribute {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::DocID => write!(f, "DocID"),
            Self::Type => write!(f, "Type"),
            Self::ParsedXml => write!(f, "ParsedXml"),
            Self::SoupNoOfTags => write!(f, "SoupNoOfTags"),
            Self::SourceNoOfTags => write!(f, "SourceNoOfTags"),
            Self::Tags => write!(f, "Tags"),
            Self::TagsAndValues => write!(f, "TagsAndValues"),
            Self::TopNode => write!(f, "TopNode"),
        }
    }
}

#[derive(Debug)]
pub enum SQLDataType {
    Text(String),
    Integer(isize),
}

#[derive(Debug)]
pub struct TableProcessLog {
    pub doc_id: isize,
    pub log_level: isize,
    pub log_entry: String,
}

#[derive(Debug)]
pub struct TableDocList {
    pub doc_id: isize,
    pub doc_validity: isize,
    pub doc_text: String,
    pub doc_invalid_reason: String,
}

#[derive(Debug)]
pub struct TableParsedXmlStore {
    pub doc_id: isize,
    pub doc_type: String,
    pub parsed_xml: String,
    pub soup_no_of_tags: isize,
    pub source_no_of_tags: isize,
    pub tags: String,
    pub topnode: String,
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
    cache_size: usize,
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
            cache_size: 0,
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

        // db.create_tables().unwrap();

        db.initialize_cache().unwrap();

        db
    }

    /*
    Database Management
    */

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

        self.cache_size += 1;

        if self.cache_size == self.batch_size {
            self.commit_writes();
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
                // println!("SQL: {sql_ins}");
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

                // fianlly executed the batch of inserts
                stmt.execute(rusqlite::params_from_iter(param_values))
                    .unwrap();

                // now clear the cached table's data
                self.cache.get_mut(table).unwrap().data.clear();
            }
        }
        // reset the cach_size
        self.cache_size = 0;
    }

    /// Log an event (Success, Warning, Error) into the ProcessLog table of the database.
    pub fn log_event(&mut self, doc_id: isize, log_entry_text: &str, log_level: LogLevel) {
        let record: Vec<SQLDataType> = vec![
            SQLDataType::Integer(doc_id),
            SQLDataType::Integer(log_level as isize),
            SQLDataType::Text(log_entry_text.to_string()),
        ];
        self.add_to_cache("ProcessLog", record);
    }

    pub fn truncate_process_log(&mut self) -> Result<()> {
        self.conn.execute("DELETE FROM ProcessLog", [])?;
        Ok(())
    }

    /*
    Document Management
    */

    /// Store a raw xml document string to the database
    pub fn store_doc(
        &mut self,
        doc_id: isize,
        doc_validity: DocValidity,
        doc_text: &str,
        doc_invalid_reason: &str,
    ) {
        let record: Vec<SQLDataType> = vec![
            SQLDataType::Integer(doc_id),
            SQLDataType::Integer(doc_validity as isize),
            SQLDataType::Text(doc_text.to_string()),
            SQLDataType::Text(doc_invalid_reason.to_string()),
        ];
        self.add_to_cache("DocList", record);
    }

    /// Get a single document by its document id
    pub fn get_single_doc(&mut self, doc_id: isize) -> TableDocList {
        // create the SQL statement and prepare it
        let sql = "SELECT * FROM DocList where DocID=:id";
        let mut stmt = self.conn.prepare_cached(sql).unwrap();
        let row: TableDocList = stmt
            .query_row(named_params! { ":name": doc_id }, |r| {
                Ok(TableDocList {
                    doc_id: r.get(0).unwrap(),
                    doc_validity: r.get(1).unwrap(),
                    doc_text: r.get(2).unwrap(),
                    doc_invalid_reason: r.get(3).unwrap(),
                })
            })
            .unwrap();

        row
    }

    pub fn get_all_docs(&mut self, doc_validity: DocValidity) -> Vec<TableDocList> {
        // create the SQL statement and prepare it
        let sql = "SELECT * FROM DocList where DocValidity=:doc_validity";
        let mut stmt = self.conn.prepare_cached(sql).unwrap();
        let row_iter = stmt
            .query_map(
                named_params! { ":doc_validity": doc_validity as isize },
                |row| {
                    Ok(TableDocList {
                        doc_id: row.get(0).unwrap(),
                        doc_validity: row.get(1).unwrap(),
                        doc_text: row.get(2).unwrap(),
                        doc_invalid_reason: row.get(3).unwrap(),
                    })
                },
            )
            .unwrap();

        let mut rows: Vec<TableDocList> = Vec::new();
        for table_row in row_iter {
            rows.push(table_row.unwrap());
        }
        rows
    }

    pub fn get_doc_count(&mut self, doc_validity: DocValidity) -> usize {
        //add condition to handle DocValidity::All
        // create the SQL statement and prepare it
        let sql = "SELECT COUNT(*) AS NoOfDocs FROM DocList where DocValidity=:doc_validity";
        let mut stmt = self.conn.prepare_cached(sql).unwrap();
        let doc_count: usize = stmt
            .query_row(
                named_params! { ":doc_validity": doc_validity as isize },
                |r| Ok(r.get(0).unwrap()),
            )
            .unwrap();

        doc_count
    }

    pub fn truncate_doc_store(&mut self) -> Result<()> {
        self.conn.execute("DELETE FROM DocList", [])?;
        Ok(())
    }

    /*
    XML Management
     */
    pub fn get_xml_count(&mut self) -> usize {
        //add condition to handle DocValidity::All
        // create the SQL statement and prepare it
        let sql = "SELECT COUNT(*) AS NoOfXMLs FROM ParsedXmlStore";
        let mut stmt = self.conn.prepare_cached(sql).unwrap();
        let xml_count: usize = stmt.query_row([], |r| Ok(r.get(0).unwrap())).unwrap();

        xml_count
    }

    pub fn truncate_xml_store(&mut self) -> Result<()> {
        self.conn.execute("DELETE FROM ParsedXmlStore", [])?;
        self.conn.execute("DELETE FROM XmlTagsAndValues", [])?;
        self.conn.execute("DELETE FROM XmlFStarAttributes", [])?;
        Ok(())
    }

    /// Store the parsed xml document to the database
    pub fn store_xml_parsed(&mut self, doc_id: isize, parsed_xml: &xmlparser::XmlDoc) {
        // unused attributes - needed for backwards compatibility with DB structure
        let soup_no_of_tags: isize = 0;
        let source_no_of_tags: isize = 0;
        let top_node = "".to_string();
        let tags = "".to_string();

        // used attributes
        let doc_type = &parsed_xml.doc_type;
        let tags_and_values = serde_json::to_string(&parsed_xml.xml_parsed).unwrap();

        // write the xml data to the cache
        let record: Vec<SQLDataType> = vec![
            SQLDataType::Integer(doc_id),
            SQLDataType::Text(doc_type.to_owned()),
            SQLDataType::Text(tags_and_values),
            SQLDataType::Integer(soup_no_of_tags),
            SQLDataType::Integer(source_no_of_tags),
            SQLDataType::Text(tags),
            SQLDataType::Text(top_node),
        ];
        self.add_to_cache("ParsedXmlStore", record);

        // write the forward star data to the cache
        let first_link = serde_json::to_string(parsed_xml.fstar.get_first_links()).unwrap();
        let to_node = serde_json::to_string(parsed_xml.fstar.get_to_nodes()).unwrap();
        let node_caption = serde_json::to_string(parsed_xml.fstar.get_node_captions()).unwrap();

        let record: Vec<SQLDataType> = vec![
            SQLDataType::Integer(doc_id),
            SQLDataType::Integer(parsed_xml.fstar.num_links as isize),
            SQLDataType::Integer(parsed_xml.fstar.num_nodes as isize),
            SQLDataType::Integer(parsed_xml.fstar.selected_node as isize),
            SQLDataType::Text(first_link),
            SQLDataType::Text(to_node),
            SQLDataType::Text(node_caption),
        ];
        self.add_to_cache("XmlFStarAttributes", record);

        // write the tags and value data to the cache
        let mut tag_idx: isize = 0;

        for map_entry in parsed_xml.xml_parsed.iter() {
            tag_idx += 1;
            let tag_name = map_entry.0;
            let tag_value = map_entry.1;

            for (idx, value_entry) in tag_value.iter().enumerate() {
                let tag_id = value_entry.0;
                let depth = value_entry.1;
                let tag_value = &value_entry.2;
                let tag_type = value_entry.3;

                let record: Vec<SQLDataType> = vec![
                    SQLDataType::Integer(doc_id),
                    SQLDataType::Text(doc_type.to_owned()),
                    SQLDataType::Integer(tag_idx),
                    SQLDataType::Text(tag_name.to_owned()),
                    SQLDataType::Integer(tag_type as isize),
                    SQLDataType::Integer(depth as isize),
                    SQLDataType::Integer(tag_id as isize),
                    SQLDataType::Integer(idx as isize),
                    SQLDataType::Text(tag_value.to_owned()),
                ];
                self.add_to_cache("XmlTagsAndValues", record);
            }
        }
    }

    /*
    Index Management
    */
    pub fn create_indices(&mut self, index_group: IndexGroup) -> Result<()> {
        match index_group {
            IndexGroup::ProcessLog => {
                // the ProcessLog index
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS IdxProcessLogDocID ON ProcessLog (DocID)",
                    [],
                )?;

                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS IdxProcessLogLogLevel ON ProcessLog (LogLevel)",
                    [],
                )?;
            }
            IndexGroup::DocStore => {
                // the DocList indices
                self.conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS IdxDocListDocID ON DocList (DocID)",
                    [],
                )?;

                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS IdxDocListDocValidity ON DocList (DocValidity)",
                    [],
                )?;
            }
            IndexGroup::XmlStore => {
                // the ParsedXmlStore indices
                self.conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS IdxParsedXmlStoreDocID ON ParsedXmlStore (DocID)",
                    [],
                )?;

                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS IdxParsedXmlStoreType ON ParsedXmlStore (Type)",
                    [],
                )?;

                // the XmlTagsAndValues indices
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS IdxXmlTagsAndValuesDocID ON XmlTagsAndValues (DocID)",
                    [],
                )?;

                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS IdxXmlTagsAndValuesTag ON XmlTagsAndValues (Tag)",
                    [],
                )?;

                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS IdxXmlTagsAndValuesType ON XmlTagsAndValues (Type)",
                    [],
                )?;

                // the XmlFStarAttributes index
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS IdxXmlFStarAttributes ON XmlFStarAttributes (DocID)",
                    [],
                )?;
            }
        }

        Ok(())
    }

    pub fn drop_indices(&mut self, index_group: IndexGroup) -> Result<()> {
        match index_group {
            IndexGroup::ProcessLog => {
                // the ProcessLog index
                self.conn
                    .execute("DROP INDEX IF EXISTS IdxProcessLogDocID", [])?;

                self.conn
                    .execute("DROP INDEX IF EXISTS IdxProcessLogLogLevel", [])?;
            }
            IndexGroup::DocStore => {
                // the DocList indices
                self.conn
                    .execute("DROP INDEX IF EXISTS IdxDocListDocID", [])?;

                self.conn
                    .execute("DROP INDEX IF EXISTS IdxDocListDocValidity", [])?;
            }
            IndexGroup::XmlStore => {
                // the ParsedXmlStore indices
                self.conn
                    .execute("DROP INDEX IF EXISTS IdxParsedXmlStoreDocID", [])?;

                self.conn
                    .execute("DROP INDEX IF EXISTS IdxParsedXmlStoreType", [])?;

                // the XmlTagsAndValues indices
                self.conn
                    .execute("DROP INDEX IF EXISTS IdxXmlTagsAndValuesDocID", [])?;

                self.conn
                    .execute("DROP INDEX IF EXISTS IdxXmlTagsAndValuesTag", [])?;

                self.conn
                    .execute("DROP INDEX IF EXISTS IdxXmlTagsAndValuesType", [])?;

                // the XmlFStarAttributes index
                self.conn
                    .execute("DROP INDEX IF EXISTS IdxXmlFStarAttributes", [])?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
