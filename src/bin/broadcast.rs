/// ```bash
/// $ cargo build
/// $ maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
/// ````
use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use core::borrow::Borrow;
use core::hash::Hash;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(BroadcastHandler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct BroadcastHandler {
    s: Arc<Mutex<State>>,
}

#[derive(Clone, Default, Debug)]
struct State {
    messages: HashSet<u64>,
    messages_list: Vec<u64>,
    already_send: HashMap<String, usize>,
    neighbours: Vec<String>,
}

impl State {
    fn insert(&mut self, value: u64) {
        if self.messages.contains(&value) {
            return;
        }
        self.messages.insert(value);
        self.messages_list.push(value);
    }

    fn take_all(&self) -> Vec<u64> {
        self.messages_list.clone()
    }

    fn take_node<Q>(&self, node_id: &Q) -> (usize, Vec<u64>)
    where
        Q: ?Sized,
        String: Borrow<Q>,
        Q: Hash + Eq,
    {
        let drop_first = self.already_send.get(node_id);
        let drop_first = drop_first.unwrap_or(&0);
        let slice = self.messages_list.as_slice();
        let slice = &slice[*drop_first..];
        (*drop_first, slice.into())
    }

    fn update_node(&mut self, node_id: String, prev_len: usize, len: usize) {
        let entry = self.already_send.get_mut(&node_id);
        match entry {
            Some(v) => {
                if *v == prev_len {
                    *v += len;
                }
            }
            None => {
                if prev_len == 0 {
                    self.already_send.insert(node_id, len);
                }
            }
        };
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Init {
        _node_id: String,
        _node_ids: Vec<String>,
    },
    Broadcast {
        message: u64,
    },
    Update {
        messages: Vec<u64>,
    },
    Read {},
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    BroadcastOk {},
    ReadOk { messages: Vec<u64> },
    UpdateOk {},
    TopologyOk {},
}

#[async_trait]
impl Node for BroadcastHandler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Broadcast { message }) => {
                self.s.lock().await.insert(message);
                // let neighbours = self.s.lock().await.neighbours.clone();
                let mut rpcs = vec![];
                for n in runtime.neighbours() {
                    let (prev_len, messages) = self.s.lock().await.take_node(n);
                    let len = messages.len();
                    let msg = Request::Update { messages };
                    let rpc = runtime.rpc(n.clone(), msg).await?;
                    rpcs.push((n.clone(), prev_len, len, rpc));
                }
                for (n, prev_len, len, rpc) in rpcs {
                    if rpc.await.is_ok() {
                        self.s.lock().await.update_node(n, prev_len, len);
                    }
                }
                runtime.reply_ok(req).await?;
                Ok(())
            }
            Ok(Request::Update { messages }) => {
                let mut state = self.s.lock().await;
                for m in messages {
                    state.insert(m);
                }
                runtime.reply_ok(req).await
            }
            Ok(Request::Read {}) => {
                let result = self.s.lock().await.take_all();
                runtime
                    .reply(req, Response::ReadOk { messages: result })
                    .await
            }
            Ok(Request::Topology { mut topology }) => {
                self.s.lock().await.neighbours = topology
                    .insert(runtime.node_id().to_string(), vec![])
                    .unwrap();
                runtime.reply_ok(req).await
            }
            _ => done(runtime, req),
        }
    }
}
