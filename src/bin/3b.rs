use std::io::{stdin, stdout};
use std::{collections::HashMap, io::Write};

use crate::Payload::ReadOk;
use anyhow::{self, Context};
use rust_gosssip_gloomers::*;
use serde::{Deserialize, Serialize};
use serde_json;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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
    },
    PropagateOk {
        message_size: usize,
    },
}

#[derive(Debug)]
struct Node {
    messages: Vec<usize>,
    node_to_idx: HashMap<String, usize>,
    node_id: String,
    node_ids: Vec<String>,
    topology: Option<HashMap<String, Vec<String>>>,
    id: usize,
}

impl Node {
    fn step(&mut self, input: Message<Payload>) {
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
                serde_json::to_writer(stdout().lock(), &response)
                    .context("Can not serialize")
                    .unwrap();
                stdout().write_all(b"\n").unwrap();
                self.propagate().unwrap();
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
                stdout().write_all(b"\n").unwrap();
            }
            Payload::ReadOk { .. } => {
                panic!("input type can not be read_ok")
            }
            Payload::Topology { topology } => {
                self.topology = Some(topology);
                for node in &self.node_ids {
                    if *node != self.node_id {
                        self.node_to_idx.insert(node.to_string(), 0);
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
                stdout().write_all(b"\n").unwrap();
            }

            Payload::TopologyOk => {
                panic!("input type can not be topology_ok")
            }
            Payload::Propagate { messages } => {
                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::PropagateOk {
                            message_size: messages.len(),
                        },
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                };
                self.messages.extend(messages);
                serde_json::to_writer(stdout().lock(), &response)
                    .context("Can not serialize")
                    .unwrap();
                stdout().write_all(b"\n").unwrap();
            }
            Payload::PropagateOk { message_size } => {
                let prev = self.node_to_idx.get(&input.src).unwrap();
                self.node_to_idx.insert(input.src, prev + message_size);
            }
        }
    }

    fn propagate(&mut self) -> anyhow::Result<()> {
        if self.messages.len() == 0 {
            return Ok(());
        }
        for (node, &idx) in &self.node_to_idx {
            if idx < self.messages.len() {
                let message = Message {
                    src: self.node_id.to_string(),
                    dest: node.to_string(),
                    body: Body {
                        payload: Payload::Propagate {
                            messages: self.messages[idx..].to_owned(),
                        },
                        msg_id: Some(self.id),
                        in_reply_to: None,
                    },
                };
                serde_json::to_writer(stdout().lock(), &message)
                    .context("Can not serialize")
                    .unwrap();
                stdout().write_all(b"\n").unwrap();
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
    let node = Arc::new(Mutex::new(Node {
        messages: vec![],
        node_to_idx: HashMap::new(),
        node_id: init_request.body.payload.node_id,
        node_ids: init_request.body.payload.node_ids,
        topology: None,
        id: 0,
    }));
    let reply = Message {
        src: init_request.dest,
        dest: init_request.src,
        body: Body {
            in_reply_to: init_request.body.msg_id,
            payload: init_ok {},
            msg_id: None,
        },
    };
    serde_json::to_writer(stdout(), &reply).context("Can not serialize")?;
    stdout().write_all(b"\n")?;

    ////////

    let th1 = thread::spawn(move || loop {
        let mut x = serde_json::Deserializer::from_reader(stdin().lock());
        let input: Message<Payload> = Message::deserialize(&mut x).unwrap();
        node.lock().unwrap().step(input);
    });

    /*
    let th2 = thread::spawn(move || {
        loop{
            thread::sleep(Duration::from_secs(1));
            stdout().write_all(b"this is cron").unwrap();
            stdout().write_all(b"\n").unwrap();
        }

    });
    th2.join().unwrap();
     */
    th1.join().unwrap();

    Ok(())
}
