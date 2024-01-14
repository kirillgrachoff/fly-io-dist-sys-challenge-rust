/// ```bash
/// $ cargo build
/// $ maelstrom test -w echo --bin ./target/debug/echo --node-count 1 --time-limit 10 --log-stderr
/// ````
use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(EchoServer::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Copy, Default)]
struct EchoServer {}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Init {},
    Echo { echo: String },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    EchoOk { echo: String },
}

#[async_trait]
impl Node for EchoServer {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Init {}) => Ok(()),
            Ok(Request::Echo { echo }) => runtime.reply(req, Response::EchoOk { echo }).await,
            _ => done(runtime, req),
        }
    }
}
