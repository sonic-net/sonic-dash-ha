mod ping;
mod show;
use clap::Parser;
use std::sync::Arc;
use swbus_edge::edge_runtime::SwbusEdgeRuntime;
use swbus_proto::message_id_generator::MessageIdGenerator;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::time::{self, Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "swbuscli")]
struct Command {
    /// swbusd address
    #[arg()]
    address: String,

    /// service path of the client (todo: remove this)
    #[arg(value_parser = ServicePath::from_string)]
    service_path: ServicePath,
    /// Enable debug output
    #[arg(short = 'd', long)]
    debug: bool,

    #[command(subcommand)]
    subcommand: CliSubCmd,
}

#[derive(Parser, Debug)]
enum CliSubCmd {
    Ping(ping::PingCmd),
    Show(show::ShowCmd),
}

trait CmdHandler {
    async fn handle(&self, ctx: &CommandContext);
}

struct CommandContext {
    debug: bool,
    sp: ServicePath,
    runtime: Arc<Mutex<SwbusEdgeRuntime>>,
    id_generator: MessageIdGenerator,
}

pub struct ResponseResult {
    pub error_code: SwbusErrorCode,
    pub error_message: String,
    pub msg: Option<SwbusMessage>,
}

impl ResponseResult {
    pub fn from_code(error_code: i32, error_message: String, msg: Option<SwbusMessage>) -> Self {
        ResponseResult {
            error_code: SwbusErrorCode::try_from(error_code).unwrap_or(SwbusErrorCode::Fail),
            error_message,
            msg,
        }
    }
}

pub(crate) async fn wait_for_response(
    recv_queue_rx: &mut mpsc::Receiver<SwbusMessage>,
    request_id: u64,
    timeout: u32,
) -> ResponseResult {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(timeout as u64) {
        match time::timeout(Duration::from_secs(timeout as u64), recv_queue_rx.recv()).await {
            Ok(Some(msg)) => match msg.body {
                Some(swbus_message::Body::Response(ref response)) => {
                    if response.request_id != request_id {
                        // Not my response
                        continue;
                    }
                    return ResponseResult::from_code(response.error_code, response.error_message.clone(), Some(msg));
                }
                _ => continue,
            },
            Ok(None) => {
                return ResponseResult::from_code(SwbusErrorCode::Fail as i32, "channel broken".to_string(), None);
            }
            Err(_) => {
                return ResponseResult::from_code(SwbusErrorCode::Timeout as i32, "request timeout".to_string(), None);
            }
        }
    }
    ResponseResult::from_code(SwbusErrorCode::Timeout as i32, "request timeout".to_string(), None)
}

#[tokio::main]
async fn main() {
    let args = Command::parse();

    let runtime = Arc::new(Mutex::new(SwbusEdgeRuntime::new(
        format!("http://{}", args.address),
        args.service_path.clone(),
    )));
    let runtime_clone = runtime.clone();
    tokio::spawn(async move {
        runtime_clone.lock().await.start().await.unwrap();
    });

    let ctx = CommandContext {
        debug: args.debug,
        sp: args.service_path,
        runtime: runtime.clone(),
        id_generator: MessageIdGenerator::new(),
    };

    if ctx.debug {
        println!("Swbus-edge client started.");
    }

    match args.subcommand {
        CliSubCmd::Ping(ping_args) => ping_args.handle(&ctx).await,
        CliSubCmd::Show(show_args) => show_args.handle(&ctx).await,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use swbus_proto::swbus::{swbus_message, SwbusMessage};

    #[tokio::test]
    async fn test_response_result_from_code() {
        let msg = SwbusMessage {
            body: Some(swbus_message::Body::Response(swbus_proto::swbus::RequestResponse::ok(
                1,
            ))),
            ..Default::default()
        };
        let result = ResponseResult::from_code(SwbusErrorCode::Ok as i32, "Success".to_string(), Some(msg.clone()));
        assert_eq!(result.error_code, SwbusErrorCode::Ok);
        assert_eq!(result.msg, Some(msg));
    }

    #[tokio::test]
    async fn test_wait_for_response_success() {
        let (tx, mut rx) = mpsc::channel(1);
        let msg = SwbusMessage {
            body: Some(swbus_message::Body::Response(swbus_proto::swbus::RequestResponse::ok(
                1,
            ))),
            ..Default::default()
        };
        tx.send(msg).await.unwrap();

        let result = wait_for_response(&mut rx, 1, 5).await;
        assert_eq!(result.error_code, SwbusErrorCode::Ok);
    }

    #[tokio::test]
    async fn test_wait_for_response_timeout() {
        let (tx, mut rx) = mpsc::channel(1);
        let msg = SwbusMessage {
            body: Some(swbus_message::Body::Response(swbus_proto::swbus::RequestResponse::ok(
                1,
            ))),
            ..Default::default()
        };
        tx.send(msg).await.unwrap();

        // mismatched response id causing timeout
        let result = wait_for_response(&mut rx, 1000, 1).await;
        assert_eq!(result.error_code, SwbusErrorCode::Timeout);
        assert_eq!(result.error_message, "request timeout");
    }

    #[tokio::test]
    async fn test_wait_for_response_channel_broken() {
        let (tx, mut rx) = mpsc::channel(1);
        drop(tx);

        let result = wait_for_response(&mut rx, 1, 5).await;
        assert_eq!(result.error_code, SwbusErrorCode::Fail);
        assert_eq!(result.error_message, "channel broken");
    }
}
