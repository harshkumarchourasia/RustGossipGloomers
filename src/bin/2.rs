use anyhow;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json;
use std::io::{StdoutLock, Write};

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
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        id: usize,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}

struct UniqueIdNode {
    counter: usize,
    delta: Option<usize>,
    node_id: Option<String>,
    node_ids: Option<Vec<String>>,
}

impl UniqueIdNode {
    pub fn new(node_id: Option<String>, node_ids: Option<Vec<String>>) -> UniqueIdNode {
        UniqueIdNode {
            counter: 0,
            node_id,
            node_ids,
            delta: None,
        }
    }
}

impl UniqueIdNode {
    pub fn generate(&mut self, input: Message, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Generate => {
                let id = self.counter * self.node_ids.as_ref().unwrap().iter().len()
                    + self.delta.unwrap();
                self.counter += 1;
                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::GenerateOk { id },
                        msg_id: input.body.msg_id,
                        in_reply_to: input.body.msg_id,
                    },
                };

                serde_json::to_writer(&mut *output, &response)?;
                output.write_all(b"\n")?;

                Ok(())
            }
            Payload::GenerateOk { .. } => Ok(()),
            Payload::Init {
                node_id,
                mut node_ids,
            } => {
                node_ids.sort();
                if let Some(delta) = node_ids.iter().position(|x| *x == node_id) {
                    self.delta = Some(delta);
                } else {
                    panic!("node_id not present in node_ids");
                }

                self.node_ids = Some(node_ids);

                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::InitOk,
                        msg_id: input.body.msg_id,
                        in_reply_to: input.body.msg_id,
                    },
                };

                serde_json::to_writer(&mut *output, &response).expect("Can not serialize");
                output
                    .write_all(b"\n")
                    .context("writing trailing new line")?;

                Ok(())
            }
            Payload::InitOk => Ok(()),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = UniqueIdNode::new(None, None);
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
    for input in inputs {
        let input: Message =
            input.context("Maelstrom input from STDIN can not be deserialized ")?;
        node.generate(input, &mut stdout)?;
    }

    Ok(())
}
