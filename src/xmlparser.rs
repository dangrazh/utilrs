#![allow(dead_code, unused_imports, unused_variables)]

use pyo3::prelude::*;
use quick_xml::events::Event;
use quick_xml::Reader;

#[derive(Debug, Clone)]
#[pyclass]
pub struct Tag {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub value: String,
    has_data: bool,
    #[pyo3(get)]
    pub attributes: Option<Vec<Attributes>>,
}

impl Tag {
    pub fn new() -> Tag {
        Tag {
            name: String::new(),
            value: String::new(),
            has_data: false,
            attributes: None,
        }
    }

    pub fn update_tag_and_value(&mut self, name: String, value: String) {
        self.name = name;
        self.value = value;
        self.has_data = true;
    }

    pub fn update_attributes(&mut self, attrs: Attributes) {
        match self.attributes {
            Some(ref mut my_attrs) => my_attrs.push(attrs),
            None => {
                let new_vec: Vec<Attributes> = vec![attrs];
                self.attributes = Some(new_vec);
            }
        };
        self.has_data = true;
    }

    pub fn clear_attributes(&mut self) {
        self.attributes = None;
    }
    pub fn clear_tag_and_value(&mut self) {
        self.name = String::new();
        self.value = String::new();
        self.has_data = false;
    }
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct Attributes {
    #[pyo3(get)]
    key: String,
    #[pyo3(get)]
    value: String,
}

impl Attributes {
    pub fn new() -> Attributes {
        Attributes {
            key: String::new(),
            value: String::new(),
        }
    }
    pub fn update_values(&mut self, key: String, value: String) {
        self.key = key;
        self.value = value;
    }

    pub fn clear_values(&mut self) {
        self.key = String::new();
        self.value = String::new();
    }
}

pub fn parse_xml(xml: &str) -> Vec<Tag> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    // let mut count = 0;
    let mut dom = Vec::new();
    let mut tags_n_vals = Vec::new();
    let mut buf = Vec::new();
    let mut elname;
    let mut curr_tag: Tag = Tag::new();
    let mut curr_attrs: Attributes = Attributes::new();

    // The `Reader` does not implement `Iterator` because it outputs borrowed data (`Cow`s)
    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Start(ref e)) => {
                // check if the previous tag had attributes but no values
                // i.e. curr_tag.attributes is some
                // if curr_tag.attributes.is_some() {
                if curr_tag.has_data {
                    tags_n_vals.push(curr_tag.clone());
                    curr_tag.clear_attributes();
                    curr_tag.clear_tag_and_value();
                }
                // }

                // get the element name
                elname = String::from_utf8_lossy(e.name()).to_string();
                dom.push(elname.to_owned());
                // add the element name and a __node__ value to the tag
                let curr_name = dom.join(".");
                curr_tag.update_tag_and_value(curr_name, "__node__".to_string());

                // println!("Start of element {}", elname);
                for att_result in e.attributes() {
                    let att_value = att_result.expect("There was an error getting the attributes!");
                    let att_inner_value = att_value
                        .unescape_and_decode_value(&reader)
                        .expect("Could not get the Attribute::value!");
                    // println!(
                    //     "    Element: {} | Attribute Key: {} | Value: {}",
                    //     elname,
                    //     String::from_utf8_lossy(att_value.key).to_string(),
                    //     att_inner_value
                    // );
                    curr_attrs.update_values(
                        String::from_utf8_lossy(att_value.key).to_string(),
                        att_inner_value,
                    );
                    curr_tag.update_attributes(curr_attrs.clone());
                }
            }
            Ok(Event::Text(ref e)) => {
                let curr_name = dom.join(".");
                let curr_value = e
                    .unescape_and_decode(&reader)
                    .expect("Error while getting element text!");
                curr_tag.update_tag_and_value(curr_name.to_owned(), curr_value.to_owned());
                // add the tag to the document tags
                tags_n_vals.push(curr_tag.clone());
            }
            Ok(Event::Empty(_e)) => {}
            Ok(Event::Comment(_e)) => {}
            Ok(Event::CData(_e)) => {}
            Ok(Event::Decl(_e)) => {}
            Ok(Event::PI(_e)) => {}
            Ok(Event::DocType(_e)) => {}
            Ok(Event::End(_e)) => {
                // do clean-up work at tag closure
                // re-initiate the attrs fields with empty strings
                curr_attrs.clear_values();
                // re-initiage the current tag
                curr_tag.clear_tag_and_value();
                curr_tag.clear_attributes();

                // go one item back in the dom tree
                let _last = dom.pop();

                // println!(
                //     "End of element {}",
                //     String::from_utf8_lossy(e.name()).to_string()
                // );
            }
            Ok(Event::Eof) => break, // exits the loop when reaching end of file
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            // _ => (), // All `Event`s are handled above
        }

        // if we don't keep a borrow elsewhere, we can clear the buffer to keep memory usage low
        buf.clear();
    }

    tags_n_vals
}

// fn process_event_empty(_e: quick_xml::events::BytesStart<'_>) {}

// #[cfg(test)]
// mod tests {
//     use crate::xmlparser::*;
//     // use std::time::Instant;

//     #[test]
//     pub fn process_doc() {
//         let xml = r#"<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>yd5oBwTm19W2rZG3</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>56465384.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>7158637412</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>56465384.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH23885378935554937471</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH13546501204560291467</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">49975405.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>5497683033</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH89549400409945581319</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">6489979.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>7159672956</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH89549400409945581319</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>"#;

//         // print the input
//         // println!("-----------------------------------");
//         // println!("Processing Document {}\n", xml);
//         // // Start the timer
//         // let timer = Instant::now();

//         // process the doc
//         let doc_tags: Vec<Tag> = parse_xml(xml);
//         // Stop the timer
//         // let duration = timer.elapsed();

//         // Print the timing
//         // println!("Document Tags vector created in {:?}\n", duration,);

//         // print the tags and values of the doc
//         for curr_tag in doc_tags {
//             match curr_tag.attributes {
//                 Some(atts) => {
//                     println!(
//                         "Tag: {} | Value: {} | Attributes: {:?}",
//                         curr_tag.name, curr_tag.value, atts
//                     );
//                     // println!("     {:?}", atts);
//                 }
//                 None => {
//                     println!(
//                         "Tag: {} | Value: {} | Attributes: none",
//                         curr_tag.name, curr_tag.value
//                     );
//                 }
//             }
//         }

//         // print the end of the processing
//         println!("-----------------------------------");
//     }
// }
