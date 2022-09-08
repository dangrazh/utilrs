#![allow(dead_code, unused_imports, unused_variables)]
use fxhash::FxBuildHasher;
use indexmap::IndexMap;
use pyo3::prelude::*;
use quick_xml::events::Event;
use quick_xml::Reader;
use std::fmt;

#[path = "forwardstar.rs"]
mod forwardstar;
use forwardstar::*;

#[derive(Debug, Copy, Clone)]
pub enum TagType {
    Node = 0,
    DataTag = 1,
    Unknown = 99,
}
#[derive(Debug, Copy, Clone)]
pub enum AttributeUsage {
    AddToTagName = 1,
    AddToTagValue = 2,
    AddSeparateTag = 3,
    Ignore = 4,
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct Tag {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub value: String,
    tag_id: usize,
    parent_tag_id: usize,
    level: usize,
    tag_type: TagType,
    has_data: bool,
    #[pyo3(get)]
    pub attributes: Option<Vec<Attribute>>,
}

impl Tag {
    pub fn new() -> Self {
        Tag {
            name: String::new(),
            value: String::new(),
            tag_id: 0,
            parent_tag_id: 0,
            level: 0,
            tag_type: TagType::Unknown,
            has_data: false,
            attributes: None,
        }
    }

    pub fn derive_new_without_attributes(
        &self,
        name: String,
        value: String,
        tag_id: usize,
    ) -> Self {
        Tag {
            name,
            value,
            tag_id,
            parent_tag_id: self.parent_tag_id,
            level: self.level,
            tag_type: self.tag_type,
            has_data: self.has_data,
            attributes: None,
        }
    }

    pub fn update_tag_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn update_tag_value(&mut self, value: String) {
        self.value = value;
    }

    pub fn update_tag_and_value(
        &mut self,
        name: String,
        value: String,
        tag_id: usize,
        parent_id: usize,
        level: usize,
        tag_type: TagType,
    ) {
        self.name = name;
        self.value = value;
        self.tag_id = tag_id;
        self.parent_tag_id = parent_id;
        self.level = level;
        self.tag_type = tag_type;
        self.has_data = true;
    }

    pub fn update_attributes(&mut self, attr: Attribute) {
        match self.attributes {
            Some(ref mut my_attrs) => my_attrs.push(attr),
            None => {
                let new_vec: Vec<Attribute> = vec![attr];
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
        self.tag_id = 0;
        self.level = 0;
        self.tag_type = TagType::Unknown;
        self.has_data = false;
    }
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct Attribute {
    #[pyo3(get)]
    key: String,
    #[pyo3(get)]
    value: String,
}

impl Attribute {
    pub fn new() -> Self {
        Attribute {
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

#[derive(Debug, Clone)]
pub struct XmlDoc {
    pub doc_id: usize,
    pub doc_type: String,
    pub tags_n_values: Option<Vec<Tag>>,
    pub fstar: ForwardStar,
    pub xml_parsed: IndexMap<String, Vec<(usize, usize, String, usize)>, FxBuildHasher>,
    attribute_usage: AttributeUsage,
    curr_tag_id: usize,
    top_node_tree_level: usize,
    type_distance_to_top: usize,
}

impl fmt::Display for XmlDoc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "** XmlDoc: [")?;
        if let Some(tnv) = &self.tags_n_values {
            write!(f, "## Tag Vector: {:?}\n", tnv)?;
        } else {
            write!(f, "## Tag Vector: None\n")?;
        }
        write!(f, "## ForwardStar: {:?}\n", self.fstar)?;
        write!(f, "## XmlParsed (IndexMap): {:?}\n", self.xml_parsed)?;
        write!(f, "## AttributeUsage: {:?}\n", self.attribute_usage)?;
        write!(f, "## curr_tag_id (usize): {:?}\n", self.curr_tag_id)?;
        write!(
            f,
            "## top_node_tree_level (usize): {:?}\n",
            self.top_node_tree_level
        )?;
        write!(
            f,
            "## type_distance_to_top (usize): {:?}\n",
            self.type_distance_to_top
        )?;
        write!(f, "## DocType (String): {:?}\n", self.doc_type)?;
        write!(f, "] End XmlDoc **")
    }
}

impl XmlDoc {
    pub fn new(
        doc_id: usize,
        xml: &str,
        attribute_usage: AttributeUsage,
        top_node_tree_level: Option<usize>,
        type_distance_to_top: Option<usize>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let hash_builder = FxBuildHasher::default();
        let mut xml_doc = XmlDoc {
            doc_id,
            doc_type: String::new(),
            tags_n_values: None,
            fstar: ForwardStar::new(),
            xml_parsed: IndexMap::with_hasher(hash_builder),
            attribute_usage,
            curr_tag_id: 0,
            top_node_tree_level: top_node_tree_level.unwrap_or(0),
            type_distance_to_top: type_distance_to_top.unwrap_or(1),
        };
        if let Err(e) = xml_doc.parse_xml(xml) {
            Err(e)
        } else {
            Ok(xml_doc)
        }
    }

    fn parse_xml(&mut self, xml: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut reader = Reader::from_str(xml);
        reader.trim_text(true);

        // let mut count = 0;
        let doc_type_level: isize = (self.top_node_tree_level + self.type_distance_to_top) as isize;
        let top_node_level: isize = self.top_node_tree_level as isize;
        let mut tag_level: isize = -1;
        let mut skip_tag = false;

        let mut dom = Vec::new();
        let mut dom_ids: Vec<usize> = Vec::new();
        let mut tags_n_vals = Vec::new();
        let mut buf = Vec::new();
        let mut elname: String;
        let mut curr_tag: Tag = Tag::new();
        let mut curr_attr: Attribute = Attribute::new();
        let mut parent_tag_id: usize;

        // The `Reader` does not implement `Iterator` because it outputs borrowed data (`Cow`s)
        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    // check if we need to skip this tag
                    tag_level += 1;
                    if tag_level < top_node_level {
                        skip_tag = true;
                    } else {
                        skip_tag = false;
                    }
                    /*
                    println!(
                        "skip_tag: {} | level: {} | top_node_level: {}",
                        skip_tag,
                        dom.len(),
                        top_node_level
                    );
                    */
                    // process only if not to be skipped
                    if !skip_tag {
                        // if we have data in the tag, the previous tag had attributes but no Text/CData
                        // process the attributes of the previous tag
                        // println!("curr_tag: {:?}", curr_tag);
                        if curr_tag.has_data {
                            // println!("process_tag invoked (has_data) for tag {}", curr_tag.name);
                            // add the tag to the document tags
                            tags_n_vals.push(curr_tag.clone());
                            // process the tag into the parsed xml index map
                            self.process_tag(&mut curr_tag);

                            curr_tag.clear_attributes();
                            curr_tag.clear_tag_and_value();
                        } else {
                            if curr_tag.tag_id > 0 {
                                // this is a node tag without attributes
                                // println!("process_tag invoked for tag {}", curr_tag.name);
                                self.process_tag(&mut curr_tag);
                            }
                        }

                        // get the element name
                        elname = String::from_utf8_lossy(e.name()).to_string();

                        // check if this is the document type level - if yes, assign the document type
                        if tag_level == doc_type_level {
                            self.doc_type = elname.to_owned();
                        }

                        // add the tag to dom tree
                        dom.push(elname);
                        // println!("dom: {:?}", dom);

                        // increcment tag_id and add the incremented tag_id to the dom_ids tree
                        self.curr_tag_id += 1;
                        dom_ids.push(self.curr_tag_id);
                        // println!("dom_ids: {:?}", dom_ids);
                        // add the element name and a __node__ value to the tag
                        let curr_name = dom.join(".");

                        // set the parent tag id: to the tag id itself if this is the 1st element
                        // in the dom tree, else to the 2nd last id in the dom_id tree
                        if dom_ids.len() == 1 {
                            parent_tag_id = self.curr_tag_id;
                        } else {
                            parent_tag_id = dom_ids[dom_ids.len() - 2];
                        }
                        curr_tag.update_tag_and_value(
                            curr_name,
                            "__node__".to_string(),
                            self.curr_tag_id,
                            parent_tag_id,
                            dom.len(),
                            TagType::Node,
                        );

                        // println!("Start of element {}", elname);
                        for att_result in e.attributes() {
                            let att_value = att_result?; //expect("There was an error getting the attributes!");
                            let att_inner_value = att_value.unescape_and_decode_value(&reader)?;
                            // .expect("Could not get the Attribute::value!");
                            curr_attr.update_values(
                                String::from_utf8_lossy(att_value.key).to_string(),
                                att_inner_value,
                            );
                            curr_tag.update_attributes(curr_attr.clone());
                        }
                    }
                }
                Ok(Event::Text(ref e)) | Ok(Event::CData(ref e)) => {
                    // process only if not to be skipped
                    if !skip_tag {
                        let curr_name = dom.join(".");
                        let curr_value = e.unescape_and_decode(&reader)?;
                        // .expect("Error while getting element text!");

                        // set the parent tag id: to the tag id itself if this is the 1st element
                        // in the dom tree, else to the 2nd last id in the dom_id tree
                        if dom_ids.len() == 1 {
                            parent_tag_id = self.curr_tag_id;
                        } else {
                            parent_tag_id = dom_ids[dom_ids.len() - 2];
                        }

                        curr_tag.update_tag_and_value(
                            curr_name,
                            curr_value,
                            self.curr_tag_id,
                            parent_tag_id,
                            dom.len(),
                            TagType::DataTag,
                        );
                        // add the tag to the document tags
                        tags_n_vals.push(curr_tag.clone());
                        // process the tag into the parsed xml index map
                        self.process_tag(&mut curr_tag);
                    }
                }
                Ok(Event::Empty(_e)) => {} //no need to process empty elements
                Ok(Event::Comment(_e)) => {} //no need to process empty elements
                // Ok(Event::CData(_e)) => {}
                Ok(Event::Decl(_e)) => {}
                Ok(Event::PI(_e)) => {} //no need to process processing instructions
                Ok(Event::DocType(_e)) => {}
                Ok(Event::End(_e)) => {
                    // process only if not to be skipped
                    if !skip_tag {
                        // do clean-up work at tag closure
                        // re-initiate the attr fields with empty strings
                        curr_attr.clear_values();
                        // re-initiage the current tag
                        curr_tag.clear_tag_and_value();
                        curr_tag.clear_attributes();

                        // go one item back in tag_level, the dom tree and the dom_ids tree
                        tag_level -= 1;
                        let _last = dom.pop();
                        let _last_id = dom_ids.pop();
                    }
                }
                Ok(Event::Eof) => break, // exits the loop when reaching end of file
                Err(e) => {
                    // return an error
                    let msg = format!("Error at position {}: {:?}", reader.buffer_position(), e);
                    return Err(msg)?;
                } // _ => (), // All `Event`s are handled above
            }

            // if we don't keep a borrow elsewhere, we can clear the buffer to keep memory usage low
            buf.clear();
        }

        self.tags_n_values = Some(tags_n_vals);
        Ok(())
    }

    fn process_tag(&mut self, tag: &mut Tag) {
        // process attributes first - if any
        if let Some(attrs) = tag.attributes.as_ref() {
            let mut attribs: String = String::new();
            let key: String;
            let value: String;

            match self.attribute_usage {
                AttributeUsage::AddToTagName => {
                    // add the attributes to the tag name
                    for att in attrs {
                        attribs = format!("{}-{}", attribs, att.value);
                    }
                    key = format!("{}{}", tag.name, attribs);
                    tag.update_tag_name(key);
                }
                AttributeUsage::AddToTagValue => {
                    // add the attributes to the tag value
                    for att in attrs {
                        attribs = format!("{}{}-", attribs, att.value);
                    }
                    value = format!("{}{}", attribs, tag.value);
                    tag.update_tag_value(value);
                }
                AttributeUsage::AddSeparateTag => {
                    // process the tag itself - this is needed to ensure
                    // the forward star is not missing an element
                    // add the Tag Name to the long name
                    let (_, tag_name_short) = tag
                        .name
                        .rsplit_once(".")
                        .unwrap_or_else(|| ("n/a", tag.name.as_str()));
                    let tag_name_long = format!("{}.{}", tag.name, tag_name_short);
                    let mut copy_tag = tag.derive_new_without_attributes(
                        tag_name_long,
                        tag.value.to_owned(),
                        tag.tag_id,
                    );
                    self.process_tag(&mut copy_tag);

                    // add the attributes as new tags and
                    // process the new tags
                    for att in attrs {
                        // increment of self.curr_tag_id
                        self.curr_tag_id += 1;
                        let tag_name_long = format!("{}.{}", tag.name, att.key);
                        let mut tmp_tag = tag.derive_new_without_attributes(
                            tag_name_long,
                            att.value.to_owned(),
                            self.curr_tag_id,
                        );
                        self.process_tag(&mut tmp_tag);
                    }

                    // exit here to prevent duplicate insertion of the tag itself
                    return;
                }
                AttributeUsage::Ignore => {
                    // ignore the attributes, i.e. do nothing
                }
            }
        }

        // process the tag part 1 - add to forward star
        if self.fstar.has_root() {
            // println!(
            //     "calling add_child for tag '{}' with parent_tag_id {}, tag_id {}",
            //     tag.name, tag.parent_tag_id, tag.tag_id
            // );

            self.fstar.add_child(
                tag.parent_tag_id.to_string().as_str(),
                tag.tag_id.to_string().as_str(),
            );
        } else {
            // println!(
            //     "calling add_root for tag '{}' with tag_id {}",
            //     tag.name, tag.tag_id
            // );

            self.fstar.add_root(tag.tag_id.to_string().as_str());
        }

        // process the tag part 2 - add to indexmap
        if self.xml_parsed.contains_key(&tag.name) {
            let values = self.xml_parsed.get_mut(&tag.name).unwrap();
            values.push((
                tag.tag_id,
                tag.level,
                tag.value.to_owned(),
                tag.tag_type as usize,
            ));
            // self.xml_parsed.insert(tag.name.to_owned(), values);
        } else {
            let values = vec![(
                tag.tag_id,
                tag.level,
                tag.value.to_owned(),
                tag.tag_type as usize,
            )];
            self.xml_parsed.insert(tag.name.to_owned(), values);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use std::time::Instant;

    #[test]
    pub fn process_doc() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.008.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><CstmrDrctDbtInitn><GrpHdr><MsgId>yd5oBwTm19W2rZG3</MsgId><CreDtTm>2013-10-08T12:57:52</CreDtTm><NbOfTxs>2</NbOfTxs><CtrlSum>56465384.0</CtrlSum><InitgPty><Nm>PILOTFORETAG B</Nm><Id><OrgId><Othr><Id>7158637412</Id><SchmeNm><Cd>BANK</Cd></SchmeNm></Othr></OrgId></Id></InitgPty></GrpHdr><PmtInf><PmtInfId>SEND PAYMENT VER 009</PmtInfId><PmtMtd>DD</PmtMtd><BtchBookg>true</BtchBookg><NbOfTxs>2</NbOfTxs><CtrlSum>56465384.0</CtrlSum><PmtTpInf><SvcLvl><Cd>SEPA</Cd></SvcLvl><LclInstrm><Cd>B2B</Cd></LclInstrm><SeqTp>RCUR</SeqTp></PmtTpInf><ReqdColltnDt>2013-11-08</ReqdColltnDt><Cdtr><Nm>PILOTFORETAG B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr></Cdtr><CdtrAcct><Id><IBAN>CH23885378935554937471</IBAN></Id></CdtrAcct><CdtrAgt><FinInstnId><BIC>HANDNL2A</BIC></FinInstnId></CdtrAgt><CdtrSchmeId><Id><PrvtId><Othr><Id>CH13546501204560291467</Id><SchmeNm><Prtry>SEPA</Prtry></SchmeNm></Othr></PrvtId></Id></CdtrSchmeId><DrctDbtTxInf><PmtId><EndToEndId>BMO1 SEND PROD VER 10 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">49975405.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER8</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>Pilot B</Nm><PstlAdr><Ctry>NL</Ctry></PstlAdr><Id><OrgId><Othr><Id>5497683033</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH89549400409945581319</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 1</Ustrd></RmtInf></DrctDbtTxInf><DrctDbtTxInf><PmtId><EndToEndId>BMO2 SEND PROD VER 11 1106</EndToEndId></PmtId><InstdAmt Ccy="EUR">6489979.0</InstdAmt><ChrgBr>SLEV</ChrgBr><DrctDbtTx><MndtRltdInf><MndtId>PRODVER9</MndtId><DtOfSgntr>2011-10-01</DtOfSgntr></MndtRltdInf></DrctDbtTx><DbtrAgt><FinInstnId><BIC>HANDDEFF</BIC></FinInstnId></DbtrAgt><Dbtr><Nm>PILOT B</Nm><PstlAdr><Ctry>DE</Ctry></PstlAdr><Id><OrgId><Othr><Id>7159672956</Id><SchmeNm><Cd>CUST</Cd></SchmeNm></Othr></OrgId></Id></Dbtr><DbtrAcct><Id><IBAN>CH89549400409945581319</IBAN></Id></DbtrAcct><RmtInf><Ustrd>Invoice 2</Ustrd></RmtInf></DrctDbtTxInf></PmtInf></CstmrDrctDbtInitn></Document>"#;

        // print the input
        println!("-----------------------------------");
        // println!("Processing Document {}\n", xml);
        // // Start the timer
        // let timer = Instant::now();

        // process the doc
        // let doc_tags: Vec<Tag> = XmlDoc::parse_xml(xml);

        // let parsed_xml =
        //     XmlDoc::new(1, xml, AttributeUsage::AddSeparateTag, Some(1), Some(0)).unwrap();
        let parsed_xml = XmlDoc::new(1, xml, AttributeUsage::AddSeparateTag, None, None).unwrap();

        // let doc_tags: &Vec<Tag> = &parsed_xml.tags_n_values.unwrap();

        // debug the parsed_xml
        // println!("{}", parsed_xml);
        println!("{:?}", parsed_xml.fstar);

        // Stop the timer
        // let duration = timer.elapsed();

        // Print the timing
        // println!("Document Tags vector created in {:?}\n", duration,);

        /*
        // print the tags and values of the doc
        for curr_tag in doc_tags {
            match curr_tag.attributes {
                Some(atts) => {
                    println!(
                        "Tag: {} | Value: {} | Attributes: {:?}",
                        curr_tag.name, curr_tag.value, atts
                    );
                    // println!("     {:?}", atts);
                }
                None => {
                    println!(
                        "Tag: {} | Value: {} | Attributes: none",
                        curr_tag.name, curr_tag.value
                    );
                }
            }
        }
        */

        // print the end of the processing
        println!("-----------------------------------");
    }
}
