/// ```bash
/// $ cargo build
/// $ maelstrom test -w unique-ids --bin ./target/debug/unique_ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
/// ````
use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Serialize, Deserialize};
use std::sync::{Arc, Mutex};

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(UniqueIdHandler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct UniqueIdHandler {
    s: Arc<Mutex<SeedData>>,
}

#[derive(Clone, Default, Debug)]
struct SeedData {
    node_count: usize,
    current_id: usize,
}

impl SeedData {
    fn new(node_id: usize, node_count: usize) -> Self {
        Self {
            node_count,
            current_id: node_id,
        }
    }

    fn take_one(&mut self) -> usize {
        let result = self.current_id;
        self.current_id += self.node_count;
        result
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Generate {},
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Response {
    GenerateOk { id: usize, },
}

#[async_trait]
impl Node for UniqueIdHandler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();
        match msg {
            Ok(Request::Init { node_id, node_ids }) => {
                let mut s = self.s.as_ref().lock().unwrap();
                let id: usize = node_id.strip_prefix("n").unwrap().parse().unwrap();
                *s = SeedData::new(id , node_ids.len());
                Ok(())
            },
            Ok(Request::Generate { }) => {
                let id = self.s.lock().unwrap().take_one();
                runtime.reply(req, Response::GenerateOk { id }).await
            },
            _ => done(runtime, req),
        }
    }
}
