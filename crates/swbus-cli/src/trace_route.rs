use super::CmdHandler;
use crate::wait_for_response;
use clap::Parser;
use std::time::Instant;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tracing::info;

const MAX_HOP: u8 = 10;
#[derive(Parser, Debug)]
pub struct TraceRouteCmd {
    /// Timeout in seconds for each request
    #[arg(short = 't', long, default_value_t = 1)]
    timeout: u32,

    /// The destination service path of the request
    #[arg(value_parser = ServicePath::from_string)]
    dest: ServicePath,
}

impl CmdHandler for TraceRouteCmd {
    async fn handle(&self, ctx: &super::CommandContext) {
        // Create a channel to receive response
        let (recv_queue_tx, mut recv_queue_rx) = mpsc::channel::<SwbusMessage>(1);
        let mut src_sp = ctx.sp.clone();
        src_sp.resource_type = "traceroute".to_string();
        src_sp.resource_id = "0".to_string();
        // Register the channel to the runtime to receive response
        ctx.runtime.lock().await.add_handler(src_sp.clone(), recv_queue_tx);

        // Send ping messages
        info!("traceroute to {}, {} hops max", self.dest.to_longest_path(), MAX_HOP);

        let header = SwbusMessageHeader::new(src_sp.clone(), self.dest.clone(), ctx.id_generator.generate());
        let header_id = header.id;
        let trace_route_msg = SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::TraceRouteRequest(TraceRouteRequest::new())),
        };
        let start = Instant::now();
        ctx.runtime.lock().await.send(trace_route_msg).await.unwrap();

        for i in 0..MAX_HOP {
            // wait on the channel to receive response or timeout
            let result = wait_for_response(&mut recv_queue_rx, header_id, self.timeout).await;
            match result.error_code {
                SwbusErrorCode::Ok => {
                    let elapsed = start.elapsed();
                    let source_sp = result
                        .msg
                        .as_ref()
                        .unwrap()
                        .header
                        .as_ref()
                        .unwrap()
                        .source
                        .as_ref()
                        .unwrap();
                    info!(
                        "{}  {}  {:.3}ms  {} hops",
                        i + 1,
                        source_sp.to_longest_path(),
                        elapsed.as_secs_f64() * 1000.0,
                        (64 - result.msg.as_ref().unwrap().header.as_ref().unwrap().ttl) as u8
                    );
                    if source_sp == &self.dest {
                        break;
                    }
                }
                SwbusErrorCode::Timeout => {
                    info!("{}  *  *  *", i + 1);
                }
                _ => {
                    let src_sp = match result.msg {
                        Some(msg) => msg.header.unwrap().source.unwrap().to_longest_path().to_string(),
                        None => "".to_string(),
                    };
                    info!(
                        "{}  {}  {}({})",
                        i + 1,
                        src_sp,
                        result
                            .error_code
                            .as_str_name()
                            .strip_prefix("SWBUS_ERROR_CODE_")
                            .unwrap_or(result.error_code.as_str_name()),
                        result.error_message
                    );
                }
            }
        }
    }
}
