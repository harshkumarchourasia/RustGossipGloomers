use std::io::stdin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{collections::HashMap, thread};

use crate::Payload::ReadOk;
use anyhow::{self};
use rust_gosssip_gloomers::*;
use serde::{Deserialize, Serialize};
use serde_json;

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
        end_idx: usize,
    },
    PropagateOk {
        end_idx: usize,
    },
}

#[derive(Debug)]
struct Node {
    messages: Vec<usize>,
    to_propagate: Vec<usize>,
    counter: HashMap<String, usize>,
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
                self.to_propagate.push(message);
                respond(Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::BroadcastOk,
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                });
            }
            Payload::BroadcastOk => {
                panic!("input type can not be broadcast_ok")
            }
            Payload::Read => {
                respond(Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: ReadOk {
                            messages: self.messages.clone(),
                        },
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                });
            }
            Payload::ReadOk { .. } => {
                panic!("input type can not be read_ok")
            }
            Payload::Topology { topology } => {
                self.topology = Some(topology);
                for node in &self.node_ids {
                    if *node != self.node_id {
                        self.counter.insert(node.to_string(), 0);
                    }
                }

                respond(Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::TopologyOk,
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                });
            }

            Payload::TopologyOk => {
                panic!("input type can not be topology_ok")
            }
            Payload::Propagate { messages, end_idx } => {
                self.messages.extend(messages);
                respond(Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::PropagateOk { end_idx },
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                });
            }
            Payload::PropagateOk { end_idx } => {
                self.counter.insert(input.src, end_idx);
            }
        }
    }

    fn propagate(&mut self) -> anyhow::Result<()> {
        if self.messages.len() == 0 {
            return Ok(());
        }
        for (node, &idx) in &self.counter {
            if idx < self.to_propagate.len() {
                respond(Message {
                    src: self.node_id.to_string(),
                    dest: node.to_string(),
                    body: Body {
                        payload: Payload::Propagate {
                            messages: self.to_propagate[idx..].to_owned(),
                            end_idx: self.to_propagate.len(),
                        },
                        msg_id: Some(self.id),
                        in_reply_to: None,
                    },
                });
                self.id += 1;
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut buffer = String::new();
    stdin()
        .read_line(&mut buffer)
        .expect("Failed to read buffer for init message");
    let init_request: Message<Init> =
        serde_json::from_str(&buffer).expect("Failed to serialize init message");

    let node = Arc::new(Mutex::new(Node {
        messages: vec![],
        to_propagate: vec![],
        counter: HashMap::new(),
        node_id: init_request.body.payload.node_id,
        node_ids: init_request.body.payload.node_ids,
        topology: None,
        id: 0,
    }));
    respond(Message {
        src: init_request.dest,
        dest: init_request.src,
        body: Body {
            in_reply_to: init_request.body.msg_id,
            payload: init_ok {},
            msg_id: None,
        },
    });

    let node_copy = node.clone();

    let th1 = thread::spawn(move || loop {
        let mut x = serde_json::Deserializer::from_reader(stdin().lock());
        let input: Message<Payload> = Message::deserialize(&mut x).unwrap();
        node.lock().unwrap().step(input);
        thread::sleep(Duration::from_millis(1));
    });

    let th2 = thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(300));
        node_copy.lock().unwrap().propagate().unwrap();
    });
    th2.join().unwrap();
    th1.join().unwrap();

    Ok(())
}
