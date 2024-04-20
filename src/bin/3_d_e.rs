use std::io::{stdin, stdout};
use std::{collections::HashMap, io::Write};
use std::collections::HashSet;

use crate::Payload::ReadOk;
use anyhow::{self, Context};
use rust_gosssip_gloomers::*;
use serde::{Deserialize, Serialize};
use serde_json;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn unique_elements(vec: &mut Vec<usize>) {
    let mut set = HashSet::new();
    vec.retain(|e| set.insert(*e));
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Propagate {
        messages: Vec<usize>,
        start_idx: usize,
    },
    PropagateOk {
        end_idx: usize,
    },
}

#[derive(Debug)]
struct Node {
    messages: Vec<usize>,
    message_to_propagate: HashMap<String, usize>,
    message_propagated: HashMap<String, usize>,
    node_id: String,
    node_ids: Vec<String>,
    topology: Option<HashMap<String, Vec<String>>>,
    id: usize,
}

impl Node {
    fn step(&mut self, input: Message<Payload>) {
        eprintln!("{:?}", self.message_to_propagate);
        match input.body.payload {
            Payload::Broadcast { message } => {
                self.messages.push(message);
                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::BroadcastOk,
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                };
                self.propagate().unwrap();
                serde_json::to_writer(stdout().lock(), &response)
                    .context("Can not serialize")
                    .unwrap();
                stdout().lock().write_all(b"\n").unwrap();
            }
            Payload::BroadcastOk => {
                panic!("input type can not be broadcast_ok")
            }
            Payload::Read => {
                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: ReadOk {
                            messages: self.messages.clone(),
                        },
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                };
                serde_json::to_writer(stdout().lock(), &response)
                    .context("Can not serialize")
                    .unwrap();
                stdout().lock().write_all(b"\n").unwrap();
            }
            Payload::ReadOk { .. } => {
                panic!("input type can not be read_ok")
            }
            Payload::Topology { topology } => {
                self.topology = Some(topology);
                for node in &self.node_ids {
                    if *node != self.node_id {
                        self.message_to_propagate.insert(node.to_string(), 0);
                    }
                }

                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::TopologyOk,
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                };
                serde_json::to_writer(stdout().lock(), &response)
                    .context("Can not serialize")
                    .unwrap();
                stdout().lock().write_all(b"\n").unwrap();
            }

            Payload::TopologyOk => {
                panic!("input type can not be topology_ok")
            }
            Payload::Propagate {
                messages,
                start_idx,
            } => {
                let src = input.dest.clone();
                let n = messages.len();
                match self.message_propagated.get(&input.src) {
                    Some(&val) => {
                        if start_idx < val {
                            self.messages.extend(messages);
                            self.message_propagated.insert(src, n + start_idx);
                        }
                    }
                    None => {
                        self.messages.extend(messages);
                        self.message_propagated.insert(src, n + start_idx);
                    }
                }
                unique_elements(&mut self.messages);
                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::PropagateOk {
                            end_idx: n + start_idx,
                        },
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                };
                serde_json::to_writer(stdout().lock(), &response)
                    .context("Can not serialize")
                    .unwrap();
                stdout().lock().write_all(b"\n").unwrap();
            }
            Payload::PropagateOk { end_idx } => {
                self.message_to_propagate.insert(input.src, end_idx);
            }
        }
    }

    fn propagate(&mut self) -> anyhow::Result<()> {
        if self.messages.len() == 0 {
            return Ok(());
        }
        for (node, &idx) in &self.message_to_propagate {
            if idx < self.messages.len() {
                let message = Message {
                    src: self.node_id.to_string(),
                    dest: node.to_string(),
                    body: Body {
                        payload: Payload::Propagate {
                            messages: self.messages[idx..].to_owned(),
                            start_idx: idx,
                        },
                        msg_id: Some(self.id),
                        in_reply_to: None,
                    },
                };
                serde_json::to_writer(stdout().lock(), &message)
                    .context("Can not serialize")
                    .unwrap();
                stdout().lock().write_all(b"\n")?;
                self.id += 1;
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    /////
    // Initialize the node

    let mut buffer = String::new();
    stdin()
        .read_line(&mut buffer)
        .expect("Failed to read buffer");
    let init_request: Message<Init> =
        serde_json::from_str(&buffer).expect("Failed to parse init message");
    let mut node = Node {
        messages: vec![],
        message_to_propagate: HashMap::new(),
        message_propagated: HashMap::new(),
        node_id: init_request.body.payload.node_id,
        node_ids: init_request.body.payload.node_ids,
        topology: None,
        id: 0,
    };
    let reply = Message {
        src: init_request.dest,
        dest: init_request.src,
        body: Body {
            in_reply_to: init_request.body.msg_id,
            payload: init_ok {},
            msg_id: None,
        },
    };
    serde_json::to_writer(stdout().lock(), &reply).context("Can not serialize")?;
    stdout().lock().write_all(b"\n")?;
    let inputs = serde_json::Deserializer::from_reader(stdin().lock()).into_iter::<Message<Payload>>();
    ////////
    for input in inputs {
        let input = input.unwrap();
        node.step(input);
    }

    Ok(())
}
