const NILVALUE: usize = usize::MAX;

#[derive(Debug, Clone)]
pub struct ForwardStar {
    node_caption: Vec<String>,
    first_link: Vec<usize>,
    to_node: Vec<usize>,
    num_links: usize,
    num_nodes: usize,
    selected_node: usize,
}

impl ForwardStar {
    pub fn new() -> ForwardStar {
        ForwardStar {
            node_caption: Vec::new(),
            first_link: Vec::new(),
            to_node: Vec::new(),
            num_links: 0,
            num_nodes: 0,
            selected_node: NILVALUE,
        }
    }
    // --------------------------
    // Internal functions
    // --------------------------

    fn select_node_by_caption(&mut self, node_caption: &str) -> Result<usize, String> {
        let sel_node: Vec<usize> = self
            .node_caption
            .iter()
            .enumerate()
            .filter(|&(_, nc)| nc == &node_caption)
            .map(|(i, _)| i)
            .collect();

        // println!("sel_node: {:?}", sel_node);
        if sel_node.len() == 0 {
            // return an error
            let msg = format!("parent node caption '{}' was not found", node_caption);
            Err(msg)
        } else if sel_node.len() > 1 {
            // return an error
            let msg = format!(
                "parent node caption '{}' is not unique, unique node caption required",
                node_caption
            );
            Err(msg)
        } else {
            Ok(sel_node[0])
        }
    }

    fn add_link(&mut self, from_node: usize, to_node: usize) {
        // Create room for the new link
        self.num_links += 1;
        self.to_node.push(NILVALUE);

        // Move the other links over to make room for the new one
        let new_var_from = self.num_links - 1;
        let new_var_to = self.first_link[from_node + 1] + 1 - 1; // the last - 1 is needed as range works <from> inclusive to <to> exclusive
        for i in (new_var_to..new_var_from).step_by(1).rev() {
            self.to_node[i] = self.to_node[i - 1];
        }

        // Insert the new link
        self.to_node[self.first_link[from_node + 1]] = to_node;

        // Update the FirstLink entries
        let var_from = from_node + 1;
        let var_to = self.num_nodes + 1; // + 1 is needed as range works <from> inclusive to <to> exclusive
        for i in var_from..var_to {
            self.first_link[i] = self.first_link[i] + 1;
        }
    }

    fn new_node(&mut self, caption: &str) -> usize {
        // new entry
        self.first_link.push(self.first_link[self.num_nodes]);

        self.node_caption.push(caption.to_owned());
        let out = self.num_nodes;
        self.num_nodes += 1;
        out
    }

    // --------------------------
    // Public functions
    // --------------------------
    pub fn add_root(&mut self, root_caption: &str) {
        // set root
        self.first_link.push(0);
        self.node_caption.push(root_caption.to_owned());

        // set sentinel
        self.first_link.push(0);

        // set number of nodes
        self.num_nodes = 1;
    }

    pub fn add_child(&mut self, parent_node_caption: &str, child_caption: &str) {
        // select the parent node
        match self.select_node_by_caption(parent_node_caption) {
            Ok(x) => self.selected_node = x,
            Err(e) => panic!("{}", e),
        };
        // create the new node
        let node = self.new_node(child_caption);
        // add the link from the parent to the new node
        self.add_link(self.selected_node, node);
    }

    pub fn find_parent_by_caption(&mut self, node_caption: &str) -> (usize, usize) {
        // get the node index
        let node;
        match self.select_node_by_caption(node_caption) {
            Ok(x) => {
                node = x;
            }
            Err(e) => panic!("{}", e),
        };
        // let node = self.select_node_by_caption(node_caption)?;
        self.find_parent(node)
    }

    pub fn find_parent(&self, node: usize) -> (usize, usize) {
        // initialize parent and link as "we did not find a parent"
        let parent = NILVALUE;
        let link = NILVALUE;
        let mut out = (parent, link);

        // Find the link into this node
        for parent in 0..self.num_nodes {
            // the - 1 is non needed as range works <from> inclusive to <to> exclusive
            for link in self.first_link[parent]..self.first_link[parent + 1] {
                // the - 1 is non needed as range works <from> inclusive to <to> exclusive
                if self.to_node[link] == node {
                    out = (parent, link);
                    break;
                }
            }
        }
        out
    }

    pub fn find_node_by_caption(&mut self, node_caption: &str) -> usize {
        // get the node index
        match self.select_node_by_caption(node_caption) {
            Ok(x) => {
                return x;
            }
            Err(_e) => panic!("parent node must be unique is not unique!"),
        };
    }

    pub fn display_tree(&self) {
        self.display_node(0, 0, None);
    }

    pub fn display_node(&self, node: usize, parent: usize, display_node_only: Option<bool>) {
        let flg_display_node_only = display_node_only.unwrap_or(true);

        if node != parent {
            // display the node and parent
            println!(
                "{} -> {}",
                self.node_caption[parent], self.node_caption[node]
            )
        } else {
            if flg_display_node_only {
                // display the node
                println!("{}", self.node_caption[node]);
            }
        }
        // display the children
        for link in self.first_link[node]..self.first_link[node + 1] {
            // -1 in <to> argument not needed as range works <from> inclusive to <to> exclusive
            self.display_node(self.to_node[link], node, None);
        }
    }

    pub fn has_root(&self) -> bool {
        if self.num_nodes == 0 {
            false
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_fstar() {
        let mut fstar = ForwardStar::new();
        println!("has root: {}", fstar.has_root());
        fstar.add_root("Grandfather");
        println!("has root: {}", fstar.has_root());
        fstar.add_child("Grandfather", "Father");
        fstar.add_child("Grandfather", "Daughter");
        fstar.add_child("Father", "Son of Father");
        fstar.add_child("Father", "Daughter of Father");
        fstar.add_child("Daughter", "Son of Daughter");
        fstar.add_child("Daughter", "Daughter of Daugther");
        fstar.add_child("Son of Father", "Son of Son of Father");
        fstar.add_child("Son of Father", "Daughter of Son of Father");

        println!("This is fstar:\n{:?}", fstar);
    }
}
