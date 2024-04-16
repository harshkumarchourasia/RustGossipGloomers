use std::{
    collections::HashMap,
    io::{StdoutLock, Write},
};

use anyhow::{self, Context};
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Serialize, Deserialize)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Serialize, Deserialize)]
struct Body {
    #[serde(flatten)]
    payload: Payload,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<usize>,
}

#[derive(Serialize, Deserialize)]
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

    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

struct SingleBroadCaseNode {
    messages: Vec<usize>,
    node_id: Option<String>,
    node_ids: Option<Vec<String>>,
    topology: Option<HashMap<String, Vec<String>>>,
}

impl SingleBroadCaseNode {
    pub fn broadcast(&mut self, input: Message, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Broadcast { message } => {
                self.messages.push(message);

                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::BroadcastOk,
                        msg_id: None,
                        in_reply_to: input.body.msg_id,
                    },
                };

                serde_json::to_writer(&mut *output, &response)?;
                output.write_all(b"\n")?;
                Ok(())
            }
            Payload::BroadcastOk => Ok(()),
            Payload::Read => {
                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::ReadOk {
                            messages: self.messages.clone(),
                        },
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                };

                serde_json::to_writer(&mut *output, &response)?;
                output.write_all(b"\n")?;
                Ok(())
            }
            Payload::ReadOk { .. } => Ok(()),
            Payload::Init { .. } => {
                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::InitOk,
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                };

                serde_json::to_writer(&mut *output, &response).expect("Can not serialize");
                output
                    .write_all(b"\n")
                    .context("writing trailing new line")?;

                Ok(())
            }
            Payload::InitOk => Ok(()),
            Payload::Topology { .. } => {
                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::TopologyOk,
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                };
                serde_json::to_writer(&mut *output, &response).expect("Can not serialize");
                output
                    .write_all(b"\n")
                    .context("writing trailing new line")?;

                Ok(())
            }
            Payload::TopologyOk => Ok(()),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();
    let mut node = SingleBroadCaseNode {
        messages: vec![],
        node_id: None,
        node_ids: None,
        topology: None,
    };

    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    for input in inputs {
        let input = input.context("can not deserialize the input message")?;
        node.broadcast(input, &mut stdout)?;
    }

    Ok(())
}
