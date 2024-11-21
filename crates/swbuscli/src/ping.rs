use clap::Parser;
use std::time::Instant;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

#[derive(Parser, Debug)]
pub struct PingCmd {
    /// The number of pings to send. Default is unlimited.
    #[arg(short = 'c', long, default_value_t = u32::MAX)]
    count: u32,

    /// Timeout in seconds for each ping
    #[arg(short = 't', long, default_value_t = 1)]
    timeout: u32,

    /// Interval in seconds between pings
    #[arg(short = 'i', long, default_value_t = 1)]
    interval: u32,

    /// The destination service path to ping
    #[arg(value_parser = ServicePath::from_string)]
    dest: ServicePath,
}

impl super::CmdHandler for PingCmd {
    async fn handle(&self, ctx: &super::CommandContext) {
        //Create a channel to receive response
        let (recv_queue_tx, mut recv_queue_rx) = mpsc::channel::<SwbusMessage>(1);
        let mut src_sp = ctx.sp.clone();
        src_sp.resource_type = "ping".to_string();
        src_sp.resource_id = "0".to_string();
        //Register the channel to the runtime to receive response
        ctx.runtime
            .lock()
            .await
            .add_handler(src_sp.clone(), recv_queue_tx)
            .await
            .unwrap();

        //Send ping messages
        println!("PING {}", self.dest.to_longest_path());
        for i in 0..self.count {
            let ping_msg = SwbusMessage {
                header: Some(SwbusMessageHeader::new(src_sp.clone(), self.dest.clone())),
                body: Some(swbus_message::Body::PingRequest(PingRequest::new())),
            };
            let start = Instant::now();
            ctx.runtime.lock().await.send(ping_msg).await.unwrap();
            //wait on the channel to receive response
            match time::timeout(Duration::from_secs(self.timeout as u64), recv_queue_rx.recv()).await {
                Ok(Some(msg)) => {
                    let elapsed = start.elapsed();
                    println!(
                        "Response received: ping_seq={}, ttl={}, time={:.3}ms",
                        i,
                        msg.header.unwrap().ttl,
                        elapsed.as_secs_f64() * 1000.0
                    );
                }
                Ok(None) => println!("Channel closed"),
                Err(_) => println!("Request timeout for ping_seq {}", i),
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(self.interval as u64)).await;
        }
    }
}