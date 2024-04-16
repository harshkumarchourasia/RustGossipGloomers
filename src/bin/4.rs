use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::io::{StdoutLock, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
struct Message<P> {
    src: String,
    dest: String,
    body: Body<P>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Body<P> {
    #[serde(flatten)]
    payload: P,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<usize>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
    BroadCast { payload: HashMap<String, usize> },
}

#[derive(Deserialize)]
struct Init {
    node_id: String,
    node_ids: Vec<String>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
struct init_ok {}

#[derive(Debug)]
struct Node {
    sum: usize,
    log: HashMap<String, usize>,
    node_id: String,
    node_ids: Vec<String>,
}

impl Node {
    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Add { delta } => {
                self.log.insert(Uuid::new_v4().to_string(), delta);
                self.sum += delta;
                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::AddOk,
                        msg_id: None,
                        in_reply_to: input.body.msg_id,
                    },
                };
                serde_json::to_writer(&mut *output, &response)?;
                output.write_all(b"\n")?;
            }
            Payload::AddOk => {
                panic!("This code should be unreachable")
            }
            Payload::Read => {
                let response = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        payload: Payload::ReadOk { value: self.sum },
                        in_reply_to: input.body.msg_id,
                        msg_id: None,
                    },
                };
                serde_json::to_writer(&mut *output, &response)?;
                output.write_all(b"\n")?;
            }
            Payload::ReadOk { .. } => {
                panic!("This code should be unreachable")
            }
            Payload::BroadCast { payload } => {

            }
        }
        Ok(())
    }

    fn broadcast(&self, output: &mut StdoutLock) {
        for node in &self.node_ids {
            if node == &self.node_id {
                continue;
            } else {
                let broadcast_message = Message {
                    src: self.node_id.clone(),
                    dest: node.to_string(),
                    body: Body {
                        payload: &self.log,
                        in_reply_to: None,
                        msg_id: None,
                    },
                };
                serde_json::to_writer(&mut *output, &broadcast_message)?;
                output.write_all(b"\n")?;
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    let mut buffer = String::new();

    io::stdin()
        .read_line(&mut buffer)
        .expect("Failed to read string");
    let init: Message<Init> = serde_json::from_str(&buffer).expect("Failed to parse INIT message");
    let node = Arc::new(Mutex::new(Node {
        sum: 0,
        log: HashMap::new(),
        node_id: init.body.payload.node_id,
        node_ids: init.body.payload.node_ids,
    }));
    let reply = Message {
        src: init.dest,
        dest: init.src,
        body: Body {
            in_reply_to: init.body.msg_id,
            payload: init_ok {},
            msg_id: None,
        },
    };
    serde_json::to_writer(io::stdout(), &reply).context("Can not serialize")?;

    let node_1 = Arc::clone(&node);
    let handle_client = thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Payload>>();
        let mut stdout = std::io::stdout().lock();
        for input in inputs {
            let input = input
                .context("can not deserialize the input message")
                .unwrap();
            node_1.lock().unwrap().step(input, &mut stdout).unwrap();
        }
    });

    let node_2 = Arc::clone(&node);
    let do_broadcast = thread::spawn(move || loop {
        let mut stdout = std::io::stdout().lock();
        thread::sleep(Duration::from_millis(200));
        node_2.lock().unwrap().broadcast(&mut stdout);
    });

    handle_client.join().unwrap();
    do_broadcast.join().unwrap();

    Ok(())
}
