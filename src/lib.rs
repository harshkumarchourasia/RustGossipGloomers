use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::io::{stdout, Write};

#[derive(Serialize, Deserialize, Debug)]
pub struct Message<P> {
    pub src: String,
    pub dest: String,
    pub body: Body<P>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Body<P> {
    #[serde(flatten)]
    pub payload: P,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<usize>,
}

#[derive(Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub struct init_ok {}

pub fn respond<P>(message: Message<P>)
where
    P: Serialize,
{
    serde_json::to_writer(stdout().lock(), &message)
        .context("Can not serialize")
        .unwrap();
    stdout().lock().write_all(b"\n").unwrap();
}
