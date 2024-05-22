/// ```bash
/// $ cargo build
/// $ ./maelstrom test -w g-counter --bin ./target/debug/g_counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
/// ````
use async_trait::async_trait;
use maelstrom::kv::{seq_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_context::context::Context;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();

    let handler = Arc::new(GCounterHandler::new(runtime.clone()));

    runtime.with_handler(handler).run().await
}

const KEY: &str = "key";

struct GCounterHandler {
    kv: Storage,
}

impl GCounterHandler {
    fn new(runtime: Runtime) -> Self {
        GCounterHandler {
            kv: seq_kv(runtime),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Init {
        _node_id: String,
        _node_ids: Vec<String>,
    },
    Add {
        delta: u64,
    },
    Read {},
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    AddOk {},
    ReadOk { value: u64 },
}

#[async_trait]
impl Node for GCounterHandler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        let (ctx, mut _handle) = Context::new();
        match msg {
            Ok(Request::Init {
                _node_id,
                _node_ids,
            }) => self.kv.put(ctx, KEY.into(), 0).await,
            Ok(Request::Read {}) => {
                let mut value = self
                    .kv
                    .get::<u64>(ctx, KEY.into())
                    .await.unwrap_or(0);
                while self
                    .kv
                    .cas(_handle.spawn_ctx(), KEY.into(), value, value, true)
                    .await
                    .is_err()
                {
                    value = self.kv.get(_handle.spawn_ctx(), KEY.into()).await?;
                }
                runtime.reply(req, Response::ReadOk { value }).await
            }
            Ok(Request::Add { delta }) => {
                let mut value = self
                    .kv
                    .get::<u64>(_handle.spawn_ctx(), KEY.into())
                    .await
                    .unwrap_or(0);
                while self
                    .kv
                    .cas(_handle.spawn_ctx(), KEY.into(), value, value + delta, true)
                    .await
                    .is_err()
                {
                    value = self.kv.get(_handle.spawn_ctx(), KEY.into()).await?;
                }
                runtime.reply_ok(req).await
            }
            _ => done(runtime, req),
        }
    }
}
