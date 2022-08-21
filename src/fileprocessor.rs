#![allow(dead_code, unused_imports, unused_variables)]

#[path = "xmlparser.rs"]
mod xmlparser;
pub use xmlparser::*;

#[path = "persistence.rs"]
mod persistence;
pub use persistence::*;

// use persistence::*;

pub fn process_file(
    databasename: &str,
    configfilename: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello from process_file");
    // let db_conn = connect_db(databasename).unwrap();
    let db = DataBase::new(databasename, configfilename);

    // TODO: fetch documents from DB
    // TODO: loop through documents and process each document
    // let doc_content = "this is my constant";
    // let doc_tags_n_values = parse_xml(doc_content);
    //TODO: store processed information back to database

    Ok(())
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

    let parsed_xml =
        xmlparser::XmlDoc::new(doc_content, xmlparser::AttributeUsage::AddSeparateTag).unwrap();
    let doc_tags_n_values: Vec<Tag> = parsed_xml.tags_n_values.unwrap();
    Ok(doc_tags_n_values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_file() {}
}
