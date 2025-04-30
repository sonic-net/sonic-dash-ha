mod ping;
mod show;
mod trace_route;
use anyhow::{Context, Result};
use clap::Parser;
use std::sync::Arc;
use swbus_config::{swbus_config_from_db, swbus_config_from_yaml, SwbusConfig};
use swbus_edge::edge_runtime::SwbusEdgeRuntime;
use swbus_proto::message_id_generator::MessageIdGenerator;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::time::{self, Duration, Instant};
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, Layer};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "swbuscli")]
struct Command {
    /// Enable debug output
    #[arg(short = 'd', long)]
    debug: bool,
    /// Path to swbusd config file. Only used for local testing.
    #[arg(short, long)]
    config_file: Option<String>,
    #[command(subcommand)]
    subcommand: CliSubCmd,
}

#[derive(Parser, Debug)]
enum CliSubCmd {
    Ping(ping::PingCmd),
    TraceRoute(trace_route::TraceRouteCmd),
    Show(show::ShowCmd),
}

trait CmdHandler {
    async fn handle(&self, ctx: &CommandContext);
}

struct CommandContext {
    debug: bool,
    // The source servicepath of swbus-cli
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

fn init_logger(debug: bool) {
    let stdout_level = if debug {
        tracing::level_filters::LevelFilter::DEBUG
    } else {
        tracing::level_filters::LevelFilter::INFO
    };

    // Create a stdout logger for `info!` and lower severity levels
    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .without_time()
        .with_target(false)
        .with_level(false)
        .with_filter(stdout_level);

    // Create a stderr logger for `error!` and higher severity levels
    let stderr_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .without_time()
        .with_target(false)
        .with_level(false)
        .with_filter(tracing::level_filters::LevelFilter::ERROR);

    // Combine the layers and set them as the global subscriber
    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(stderr_layer)
        .init();
}

/// get swbusd config from config_db or file if config_file is provided.
/// If config_file is provided, it will be used, otherwise, the slot id will be read from
/// environment variable DEV and used to get the config from config_db.
fn get_swbus_config(config_file: Option<&str>) -> Result<SwbusConfig> {
    match config_file {
        Some(config_file) => {
            let config = swbus_config_from_yaml(config_file)
                .context(format!("Failed to read swbusd config from file {}", config_file))?;
            Ok(config)
        }
        None => {
            let dev = std::env::var("DEV").context("Environment DEV is not found")?;
            // Remove the prefix "dpu" from the slot id
            let dev = &dev[3..];
            let slot: u32 = dev.parse().context("Invalid slot id")?;
            let config = swbus_config_from_db(slot).context("Failed to get swbusd config from db")?;
            Ok(config)
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Command::parse();

    init_logger(args.debug);

    let swbus_config = get_swbus_config(args.config_file.as_deref()).unwrap();

    let mut sp = swbus_config.get_swbusd_service_path().unwrap_or_else(|| {
        error!("No cluster route found in swbusd config");
        std::process::exit(1);
    });

    sp.service_type = "swbus-cli".to_string();
    sp.service_id = Uuid::new_v4().to_string();
    let runtime = Arc::new(Mutex::new(SwbusEdgeRuntime::new(
        format!("http://{}", swbus_config.endpoint),
        sp.clone(),
    )));
    let runtime_clone = runtime.clone();
    tokio::spawn(async move {
        runtime_clone.lock().await.start().await.unwrap();
    });

    let ctx = CommandContext {
        debug: args.debug,
        sp,
        runtime: runtime.clone(),
        id_generator: MessageIdGenerator::new(),
    };

    if ctx.debug {
        info!("Swbus-edge client started.");
    }

    match args.subcommand {
        CliSubCmd::Ping(ping_args) => ping_args.handle(&ctx).await,
        CliSubCmd::Show(show_args) => show_args.handle(&ctx).await,
        CliSubCmd::TraceRoute(trace_route_args) => trace_route_args.handle(&ctx).await,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use swbus_config::test_utils::*;
    use swbus_proto::swbus::{swbus_message, SwbusMessage};
    use swss_common_testing::*;

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

    #[test]
    fn test_get_swbus_config() {
        let slot = 1;
        let npu_ipv4 = "10.0.1.0";
        let _ = Redis::start_config_db();

        // Mock the config database with a sample configuration
        populate_configdb_for_test();

        std::env::set_var("DEV", format!("dpu{}", slot));
        let config = get_swbus_config(None).unwrap();
        assert_eq!(config.endpoint.to_string(), format!("{}:{}", "0.0.0.0", 23606 + slot));
        let expected_sp = ServicePath::with_node(
            "region-a",
            "cluster-a",
            &format!("{}-dpu{}", npu_ipv4, slot),
            "",
            "",
            "",
            "",
        );
        assert!(config
            .routes
            .iter()
            .any(|r| r.scope == RouteScope::Cluster && r.key == expected_sp));

        cleanup_configdb_for_test();
    }
}
