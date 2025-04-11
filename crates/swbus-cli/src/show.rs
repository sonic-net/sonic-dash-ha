use crate::wait_for_response;
use clap::Parser;
use swbus_cli_data::hamgr::actor_state::ActorState;
use swbus_cli_data::hamgr::actor_state::IncomingStateEntry;
use swbus_cli_data::hamgr::actor_state::InternalStateEntry;
use swbus_proto::swbus::*;
use tabled::settings::{object::Rows, style::Style, Alignment, Modify, Panel};
use tabled::{Table, Tabled};
use tokio::sync::mpsc;
use tracing::info;

const CMD_TIMEOUT: u32 = 10;

#[derive(Parser, Debug)]
pub struct ShowCmd {
    #[command(subcommand)]
    subcommand: ShowSubCmd,
}

#[derive(Parser, Debug)]
enum ShowSubCmd {
    Route(ShowRouteCmd),
    Actor(ShowActorCmd),
}

#[derive(Parser, Debug)]
pub struct ShowRouteCmd {}

#[derive(Parser, Debug)]
pub struct ShowActorCmd {
    /// The service path of the actor relative to the swbusd
    /// e.g. "/hamgrd/0/actor/actor1"
    #[arg(value_parser = ServicePath::from_string)]
    actor_path: ServicePath,
}

trait ShowCmdHandler {
    fn create_request(&self, ctx: &super::CommandContext, src_sp: &ServicePath) -> SwbusMessage;
    fn process_response(&self, response: &RequestResponse);
}

#[derive(Tabled)]
struct RouteDisplay {
    service_path: String,
    hop_count: u32,
    nh_id: String,
    nh_scope: String,
    nh_service_path: String,
}

impl super::CmdHandler for ShowCmd {
    async fn handle(&self, ctx: &super::CommandContext) {
        // Create a channel to receive response
        let (recv_queue_tx, mut recv_queue_rx) = mpsc::channel::<SwbusMessage>(1);
        let mut src_sp = ctx.sp.clone();
        src_sp.resource_type = "show".to_string();
        src_sp.resource_id = "0".to_string();

        // Register the channel to the runtime to receive response
        ctx.runtime.lock().await.add_handler(src_sp.clone(), recv_queue_tx);

        let sub_cmd: &dyn ShowCmdHandler = match &self.subcommand {
            ShowSubCmd::Route(show_route_args) => show_route_args,
            ShowSubCmd::Actor(show_actor_args) => show_actor_args,
        };

        let request_msg = sub_cmd.create_request(ctx, &src_sp);

        let request_id = request_msg.header.as_ref().unwrap().id;

        // Send request
        ctx.runtime.lock().await.send(request_msg).await.unwrap();

        // wait on the channel to receive response
        let result = wait_for_response(&mut recv_queue_rx, request_id, CMD_TIMEOUT).await;
        match result.error_code {
            SwbusErrorCode::Ok => {
                let body = result.msg.unwrap().body.unwrap();
                match body {
                    swbus_message::Body::Response(response) => {
                        sub_cmd.process_response(&response);
                    }
                    _ => {
                        info!("Invalid response");
                    }
                }
            }
            SwbusErrorCode::Timeout => {
                info!("Request timeout");
            }
            _ => {
                info!("{}:{}", result.error_code.as_str_name(), result.error_message);
            }
        }
    }
}

impl ShowCmdHandler for ShowRouteCmd {
    fn create_request(&self, ctx: &super::CommandContext, src_sp: &ServicePath) -> SwbusMessage {
        let mgmt_req = ManagementRequest::new(ManagementRequestType::SwbusdGetRoutes);
        let swbusd_sp = ctx.sp.to_swbusd_service_path();
        let header = SwbusMessageHeader::new(src_sp.clone(), swbusd_sp, ctx.id_generator.generate());

        SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::ManagementRequest(mgmt_req)),
        }
    }

    fn process_response(&self, response: &RequestResponse) {
        let routes = match &response.response_body {
            Some(request_response::ResponseBody::RouteQueryResult(route_result)) => route_result,
            _ => {
                info!("Expecting RouteQueryResult but got something else: {:?}", response);
                return;
            }
        };

        let routes: Vec<RouteDisplay> = routes
            .entries
            .iter()
            .map(|entry| RouteDisplay {
                service_path: entry
                    .service_path
                    .as_ref()
                    .expect("service_path in RouteQueryResult cannot be None")
                    .to_longest_path(),
                hop_count: entry.hop_count,
                nh_id: entry.nh_id.clone(),
                nh_scope: RouteScope::try_from(entry.nh_scope).unwrap().as_str_name().to_string(),
                nh_service_path: entry
                    .nh_service_path
                    .as_ref()
                    .expect("nh_service_path in RouteQueryResult cannot be None")
                    .to_longest_path(),
            })
            .collect();
        let table = Table::new(routes);
        info!("{}", table)
    }
}

#[derive(Tabled)]
struct KeyValue {
    attribute: String,
    value: String,
}
#[derive(Tabled)]
struct IncomingStateDisplay {
    key: String,
    details: String,
}

impl IncomingStateDisplay {
    fn from_incoming_state(state: &IncomingStateEntry) -> Self {
        let details = vec![
            KeyValue {
                attribute: "source".to_string(),
                value: state.source.clone(),
            },
            KeyValue {
                attribute: "request-id".to_string(),
                value: state.request_id.to_string(),
            },
            KeyValue {
                attribute: "version".to_string(),
                value: state.version.to_string(),
            },
            KeyValue {
                attribute: "message/key".to_string(),
                value: state.message.key.clone(),
            },
            KeyValue {
                attribute: "message/value".to_string(),
                value: state.message.data.clone(),
            },
        ];
        let table = Table::new(details).with(Style::ascii().remove_frame()).to_string();
        IncomingStateDisplay {
            key: state.key.clone(),
            details: table,
        }
    }
}

#[derive(Tabled)]
struct InternalStateDisplay {
    key: String,
    table_meta: String,
    fvs: String,
    backup_fvs: String,
}

impl InternalStateDisplay {
    fn from_internal_state(state: &InternalStateEntry) -> Self {
        let table_meta = vec![
            KeyValue {
                attribute: "table".to_string(),
                value: state.swss_table.clone(),
            },
            KeyValue {
                attribute: "key".to_string(),
                value: state.swss_key.to_string(),
            },
            KeyValue {
                attribute: "mutated".to_string(),
                value: state.mutated.to_string(),
            },
        ];
        let table_meta = Table::new(table_meta).with(Style::ascii().remove_frame()).to_string();

        let fvs = state
            .fvs
            .iter()
            .map(|kv| KeyValue {
                attribute: kv.key.clone(),
                value: kv.value.clone(),
            })
            .collect::<Vec<KeyValue>>();
        let fvs = Table::new(fvs).with(Style::ascii().remove_frame()).to_string();

        let backup_fvs = state
            .backup_fvs
            .iter()
            .map(|kv| KeyValue {
                attribute: kv.key.clone(),
                value: kv.value.clone(),
            })
            .collect::<Vec<KeyValue>>();

        let backup_fvs = Table::new(backup_fvs).with(Style::ascii().remove_frame()).to_string();
        InternalStateDisplay {
            key: state.key.clone(),
            table_meta,
            fvs,
            backup_fvs,
        }
    }
}

impl ShowCmdHandler for ShowActorCmd {
    fn create_request(&self, ctx: &super::CommandContext, src_sp: &ServicePath) -> SwbusMessage {
        let mgmt_req = ManagementRequest::new(ManagementRequestType::HamgrdGetActorState);
        let mut dest_sp = ctx.sp.to_swbusd_service_path();
        dest_sp.join(&self.actor_path);
        let header = SwbusMessageHeader::new(src_sp.clone(), dest_sp.clone(), ctx.id_generator.generate());

        SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::ManagementRequest(mgmt_req)),
        }
    }

    fn process_response(&self, response: &RequestResponse) {
        let result = match &response.response_body {
            Some(request_response::ResponseBody::ManagementQueryResult(ref result)) => &result.value,
            _ => {
                info!("Expecting RouteQueryResult but got something else: {:?}", response);
                return;
            }
        };

        let state: ActorState = serde_json::from_str(result).unwrap();

        // convert to table for display
        let incoming_state_display = state
            .incoming_state
            .iter()
            .map(IncomingStateDisplay::from_incoming_state)
            .collect::<Vec<IncomingStateDisplay>>();
        let incoming_state_table = Table::new(incoming_state_display)
            .with(Panel::header("Incoming State"))
            .with(Modify::list(Rows::first(), Alignment::center()))
            .with(Style::modern())
            .to_string();

        info!("{}", incoming_state_table);

        // convert to table for display
        let internal_state_display = state
            .internal_state
            .iter()
            .map(InternalStateDisplay::from_internal_state)
            .collect::<Vec<InternalStateDisplay>>();
        let internal_state_table = Table::new(internal_state_display)
            .with(Panel::header("Internal State"))
            .with(Modify::list(Rows::first(), Alignment::center()))
            .with(Style::modern())
            .to_string();

        info!("{}", internal_state_table);
    }
}
