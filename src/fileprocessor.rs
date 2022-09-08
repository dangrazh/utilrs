#![allow(dead_code, unused_imports, unused_variables)]
use std::time::Instant;

#[path = "filesplit.rs"]
mod filesplit;
pub use filesplit::*;

#[path = "xmlparser.rs"]
mod xmlparser;
pub use xmlparser::*;

#[path = "persistence.rs"]
mod persistence;
pub use persistence::*;

pub fn split_file(
    databasename: &str,
    configfilename: &str,
    filetoprocess: &str,
    reg_ex: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let out: bool = true;

    let mut db = DataBase::new(databasename, configfilename);
    let documents = filesplit::split_file(filetoprocess, reg_ex);

    let docs_to_process = documents.len();
    for (idx, doc) in documents.into_iter().enumerate() {
        // status update
        // calculate the pct and then round to 2 decimal places
        let mut progress_pct: f32 = (idx as f32 * 100f32) / docs_to_process as f32;
        progress_pct = (progress_pct * 100.0).round() / 100.0;
        // display update if new progress is an integer value
        if progress_pct.fract() == 0.0 {
            print!("\rProcessing split file is at {:.0}%...", progress_pct);
        }
        // TODO: check doc valididty
        db.store_doc(idx as isize, persistence::DocValidity::Valid, &doc, "none");
        // db.commit_writes();
    }

    db.commit_writes();
    db.create_indices(persistence::IndexGroup::ProcessLog)?;
    db.create_indices(persistence::IndexGroup::DocStore)?;

    println!("writes comitted and incices created. value of out is: {out}");
    match out {
        false => db.log_event(
            0,
            "File successfully split into single documents with invalid docutments found.",
            persistence::LogLevel::Warning,
        ),
        true => db.log_event(
            0,
            "File successfully split into single documents without any errors.",
            persistence::LogLevel::Info,
        ),
    }
    Ok(out)
}

pub fn process_file(
    databasename: &str,
    configfilename: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut out = "success".to_string();

    let mut db = DataBase::new(databasename, configfilename);

    // truncate the process log and remove indices
    db.drop_indices(persistence::IndexGroup::ProcessLog)?;
    db.truncate_process_log()?;

    // check if file has already been processed
    // if yes, truncate all xlm related tables and remove indices
    if db.get_xml_count() > 0 {
        db.drop_indices(persistence::IndexGroup::XmlStore)?;
        db.truncate_xml_store()?;
    }

    // process the data
    let docs_to_process = db.get_doc_count(persistence::DocValidity::Valid);
    println!("No of docs to process: {}", docs_to_process);

    let documents = db.get_all_docs(persistence::DocValidity::Valid);
    for doc in documents {
        // status update
        // calculate the pct and then round to 2 decimal places
        let mut progress_pct: f32 = (doc.doc_id as f32 * 100f32) / docs_to_process as f32;
        progress_pct = (progress_pct * 100.0).round() / 100.0;
        // display update if new progress is an integer value
        if progress_pct.fract() == 0.0 {
            print!("\rProcessing documents is at {:.0}%...", progress_pct);
        }

        match xmlparser::XmlDoc::new(
            doc.doc_id as usize,
            &doc.doc_text,
            xmlparser::AttributeUsage::AddSeparateTag,
            None,
            None,
        ) {
            Ok(xml_parsed) => {
                db.store_xml_parsed(doc.doc_id, &xml_parsed);
                let log_text = format!("Document successfully loaded");
                db.log_event(doc.doc_id, &log_text, persistence::LogLevel::Info);
            }
            Err(err) => {
                let log_text = format!(
                    "The following error ocurred while parsing the xml document: {}",
                    err
                );
                db.log_event(doc.doc_id, &log_text, persistence::LogLevel::Error);
                out = "error".to_string();
            }
        }
    }

    // final commit of changes to the database
    db.commit_writes();

    // once all data is processed and stored, create the indices
    db.create_indices(persistence::IndexGroup::ProcessLog)?;
    db.create_indices(persistence::IndexGroup::XmlStore)?;

    Ok(out)
}

pub fn process_single_document(
    doc_content: &str,
) -> Result<Vec<xmlparser::Tag>, Box<dyn std::error::Error>> {
    // let xml = r#"<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>yd5oBwTm19W2rZG3</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>56465384.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>7158637412</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>56465384.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH23885378935554937471</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH13546501204560291467</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">49975405.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>5497683033</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH89549400409945581319</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">6489979.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>7159672956</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH89549400409945581319</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>"#;
    // let xml_inv = r#"<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">"#;
    // process the valid doc
    // let doc_tags_n_values = parse_xml(xml);
    // process the invalid doc
    // let inv_doc = parse_xml(xml_inv);
    // let doc_tags_n_values = xmlparser::XmlDoc::parse_xml(doc_content);

    let parsed_xml = xmlparser::XmlDoc::new(
        1,
        doc_content,
        xmlparser::AttributeUsage::AddSeparateTag,
        None,
        None,
    )
    .unwrap();
    let doc_tags_n_values: Vec<Tag> = parsed_xml.tags_n_values.unwrap();
    Ok(doc_tags_n_values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_store_xml() {
        let mut db = DataBase::new("xml_parser_test.db", "tabledef.yaml");
        db.truncate_doc_store().unwrap();
        db.truncate_process_log().unwrap();
        db.truncate_xml_store().unwrap();
        db.drop_indices(persistence::IndexGroup::DocStore).unwrap();
        db.drop_indices(persistence::IndexGroup::ProcessLog)
            .unwrap();
        db.drop_indices(persistence::IndexGroup::XmlStore).unwrap();
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>yd5oBwTm19W2rZG3</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>56465384.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>7158637412</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>56465384.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH23885378935554937471</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH13546501204560291467</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">49975405.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>5497683033</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH89549400409945581319</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">6489979.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>7159672956</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH89549400409945581319</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>"#;
        let parsed_xml = XmlDoc::new(1, xml, AttributeUsage::AddSeparateTag, None, None).unwrap();
        db.store_xml_parsed(1, &parsed_xml);
        db.commit_writes();
    }
    #[test]
    fn split_and_process_file() {
        let mut db = DataBase::new("xml_parser_test.db", "tabledef.yaml");
        db.truncate_doc_store().unwrap();
        db.truncate_process_log().unwrap();
        db.truncate_xml_store().unwrap();
        db.drop_indices(persistence::IndexGroup::DocStore).unwrap();
        db.drop_indices(persistence::IndexGroup::ProcessLog)
            .unwrap();
        db.drop_indices(persistence::IndexGroup::XmlStore).unwrap();
        drop(db);

        // Start the timer
        let mut start = Instant::now();

        let file_to_process = "P:/Programming/Python/xml_examples/xml_test_data_large.xml";
        let reg_ex = r"(<\?xml .*?>)";

        let res = split_file(
            "xml_parser_test.db",
            "tabledef.yaml",
            file_to_process,
            reg_ex,
        )
        .unwrap();
        assert_eq!(res, true);

        // Stop the timer
        let mut duration = start.elapsed();

        println!("file split and stored to DB in {:?} ", duration);

        start = Instant::now();

        let res1 = process_file("xml_parser_test.db", "tabledef.yaml").unwrap();
        assert_eq!(res1, "success".to_string());

        // Stop the timer
        duration = start.elapsed();

        println!("file content parsed and stored to DB in {:?} ", duration);
    }

    #[test]
    fn purge_database() {
        let mut db = DataBase::new("xml_parser_test.db", "tabledef.yaml");
        db.truncate_doc_store().unwrap();
        db.truncate_process_log().unwrap();
        db.truncate_xml_store().unwrap();
        db.drop_indices(persistence::IndexGroup::DocStore).unwrap();
        db.drop_indices(persistence::IndexGroup::ProcessLog)
            .unwrap();
        db.drop_indices(persistence::IndexGroup::XmlStore).unwrap();
        drop(db);
    }
}
