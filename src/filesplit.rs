use regex::Regex;
use std::fs::File;
use std::io::Read;
use std::time::Instant;

pub fn split_file(filename: &str, reg_ex: &str) -> Vec<String> {
    let mut content = String::new();

    // Open the file in read-only mode.
    match File::open(filename) {
        // The file is open (no error).
        Ok(mut file) => {
            // Start the timer
            //let start = Instant::now();

            // Read all the file content into a variable (ignoring the result of the operation).
            file.read_to_string(&mut content).unwrap();

            // Stop the timer
            //let duration = start.elapsed();

            // println!(
            //     "file read into memory in {:?} , now occupying {} bytes.",
            //     duration,
            //     content.len()
            // );

            // The file is automatically closed when is goes out of scope.
        }
        // Error handling.
        Err(error) => {
            panic!("Error opening file {}: {}", filename, error);
        }
    }

    // run the regex split
    do_regex_split(&content, reg_ex)
}

fn do_regex_split(text_to_split: &str, reg_ex: &str) -> Vec<String> {
    // Start the timer
    let start_2 = Instant::now();
    // Split by regex
    let seperator = Regex::new(reg_ex).expect("Invalid regex"); // r"(<\?xml .*?>)" <Document .*?>
    let keep_tog: bool = true;
    let splits = split_keep_regex(&seperator, &text_to_split, keep_tog);
    // Stop the timer
    let duration_2 = start_2.elapsed();
    // Print the result
    println!(
        "vector created with regex in {:?}, it has {} items",
        duration_2,
        splits.len()
    );

    // println!("{:?}", splits)
    // for (i, split) in splits[..3].iter().enumerate() {
    //     //
    //     println!("Item {} in vec is: {}\n", i, split);
    // }

    // return the vector
    splits
}

fn split_keep_regex(r: &Regex, text: &str, keep_together: bool) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();
    let mut last = 0;

    // debug only
    // let v: Vec<_> = text.match_indices(r).collect();
    // for (idx, itm) in v {
    //     println!("{} | {} | {}", idx, itm, itm.len());
    // }
    // end debug only
    if keep_together {
        for (index, _) in text.match_indices(r) {
            if last != index {
                result.push(text[last..index - 1].to_owned());
                last = index;
            }
        }
        if last < text.len() {
            result.push(text[last..].to_owned());
        }
    } else {
        for (index, matched) in text.match_indices(r) {
            if last != index {
                result.push(text[last..index].to_owned());
            }
            result.push(matched.to_owned());
            last = index + matched.len();
        }
        if last < text.len() {
            result.push(text[last..].to_owned());
        }
    }
    result
}
