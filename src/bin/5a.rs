use anyhow::Context;
use rust_gosssip_gloomers::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{stdin, stdout, Write};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<Vec<usize>>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

impl Node {
    fn step(&mut self, input: Message<Payload>) -> Message<Payload>{
        match input.body.payload {
            Payload::Send { key, msg } => {
                let offset = self.log.get(&key).or(Some(&vec![])).unwrap().len();
                self.log.entry(key.clone()).or_insert_with(||vec![]).push(Msg {value: msg, offset });
                self.committed_offset.entry(key).or_insert(0);

                Message{
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::SendOk { offset },
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                }
            }
            Payload::SendOk { .. } => {
                unreachable!();
            }
            Payload::Poll { offsets } => {
                let mut msgs: HashMap<String, Vec<Vec<usize>>> = HashMap::new();
                for (key, offset) in offsets {
                    let key_msgs = self.log.get(&key).map_or_else(Vec::new, |log| {
                        log.iter()
                            .skip(offset)
                            .map(|msg| vec![msg.offset, msg.value])
                            .collect()
                    });
                    msgs.insert(key, key_msgs);
                }
                Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::PollOk { msgs },
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                }
            }
            Payload::PollOk { .. } => {
                unreachable!();
            }
            Payload::CommitOffsets { offsets } => {
                offsets.into_iter().for_each(|(k,v)| {
                    self.committed_offset.insert(k, v);
                });
                Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::CommitOffsetsOk,
                        msg_id: None,
                        in_reply_to: input.body.msg_id
                    }
                }
            }
            Payload::CommitOffsetsOk => {
                unreachable!();
            }
            Payload::ListCommittedOffsets { keys } => {
                let offsets = self.committed_offset
                    .iter()
                    .filter(|(key, _)| {
                        keys.contains(&key)
                    })
                    .map(|(key, value)| (key.clone(), *value))
                    .collect();
                Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::ListCommittedOffsetsOk {offsets},
                        in_reply_to: input.body.msg_id,
                        msg_id: None
                    }
                }

            }
            Payload::ListCommittedOffsetsOk { .. } => {
                unreachable!();
            }
        }
    }
}

struct Msg{
    value: usize,
    offset: usize
}

struct Node {
    msg_id: usize,
    log: HashMap<String, Vec<Msg>>,
    committed_offset: HashMap<String, usize>,
}

fn main() {
    let mut buffer = String::new();

    stdin()
        .read_line(&mut buffer)
        .expect("Failed to read string");
    let init: Message<Init> = serde_json::from_str(&buffer).expect("Failed to parse INIT message");
    let mut node = Node {
        msg_id: 0,
        log: HashMap::new(),
        committed_offset: HashMap::new(),
    };
    let reply = Message {
        src: init.dest,
        dest: init.src,
        body: Body {
            in_reply_to: init.body.msg_id,
            payload: init_ok {},
            msg_id: None,
        },
    };
    serde_json::to_writer(stdout(), &reply).context("Can not serialize").unwrap();
    stdout().write_all(b"\n").unwrap();

    let inputs =
        serde_json::Deserializer::from_reader(stdin().lock()).into_iter::<Message<Payload>>();
    for input in inputs {
        let input = input.unwrap();
        respond(node.step(input));
    }
}
