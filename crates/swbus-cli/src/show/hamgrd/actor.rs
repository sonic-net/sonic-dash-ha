use crate::show::ShowCmdHandler;
use crate::CommandContext;
use chrono::{DateTime, Local};
use clap::Parser;
use serde_json::to_string_pretty;
use swbus_actor::state::{incoming::IncomingTableEntry, internal::InternalTableData, ActorStateDump};
use swbus_proto::swbus::*;
use tabled::settings::{object::Rows, style::Style, Alignment, Modify, Panel};
use tabled::{Table, Tabled};
use tracing::info;

#[derive(Parser, Debug)]
pub struct ShowActorCmd {
    /// The service path of the actor relative to the swbusd
    /// e.g. "/hamgrd/0/actor/actor1"
    #[arg(value_parser = ServicePath::from_string)]
    actor_path: ServicePath,
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

fn unix_secs_to_string(unix_secs: u64) -> String {
    let naive = DateTime::from_timestamp(unix_secs as i64, 0);
    match naive {
        Some(naive) => {
            let datetime: DateTime<Local> = naive.with_timezone(&Local);
            datetime.format("%Y:%m:%d %H:%M:%S").to_string()
        }
        None => "INV".to_string(),
    }
}

impl IncomingStateDisplay {
    fn from_incoming_state((key, state): (&String, &IncomingTableEntry)) -> Self {
        let details = vec![
            KeyValue {
                attribute: "source".to_string(),
                value: state.source.to_longest_path(),
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
                value: state.msg.key.clone(),
            },
            KeyValue {
                attribute: "message/value".to_string(),
                value: to_string_pretty(&state.msg.data).unwrap_or("INV".to_string()),
            },
            KeyValue {
                attribute: "created-time".to_string(),
                value: unix_secs_to_string(state.created_time),
            },
            KeyValue {
                attribute: "last-updated-time".to_string(),
                value: unix_secs_to_string(state.last_updated_time),
            },
            KeyValue {
                attribute: "response".to_string(),
                value: state.response.clone(),
            },
            KeyValue {
                attribute: "acked".to_string(),
                value: state.acked.to_string(),
            },
        ];
        let table = Table::new(details).with(Style::ascii().remove_frame()).to_string();
        IncomingStateDisplay {
            key: key.clone(),
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
    fn from_internal_state((key, state): (&String, &InternalTableData)) -> Self {
        let table_meta = vec![
            KeyValue {
                attribute: "table".to_string(),
                value: state.swss_table_name.clone(),
            },
            KeyValue {
                attribute: "key".to_string(),
                value: state.swss_key.to_string(),
            },
            KeyValue {
                attribute: "mutated".to_string(),
                value: state.mutated.to_string(),
            },
            KeyValue {
                attribute: "last-updated-time".to_string(),
                value: {
                    match state.last_updated_time {
                        Some(last_updated_time) => unix_secs_to_string(last_updated_time),
                        None => "".to_string(),
                    }
                },
            },
        ];
        let table_meta = Table::new(table_meta).with(Style::ascii().remove_frame()).to_string();

        let fvs = state
            .fvs
            .iter()
            .map(|(key, value)| KeyValue {
                attribute: key.clone(),
                value: value.to_string_lossy().into_owned(),
            })
            .collect::<Vec<KeyValue>>();
        let fvs = Table::new(fvs).with(Style::ascii().remove_frame()).to_string();

        let backup_fvs = state
            .backup_fvs
            .iter()
            .map(|(key, value)| KeyValue {
                attribute: key.clone(),
                value: value.to_string_lossy().into_owned(),
            })
            .collect::<Vec<KeyValue>>();

        let backup_fvs = Table::new(backup_fvs).with(Style::ascii().remove_frame()).to_string();
        InternalStateDisplay {
            key: key.clone(),
            table_meta,
            fvs,
            backup_fvs,
        }
    }
}

impl ShowCmdHandler for ShowActorCmd {
    fn create_request(&self, ctx: &CommandContext, src_sp: &ServicePath) -> SwbusMessage {
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

        let state: ActorStateDump = serde_json::from_str(result).unwrap();

        // convert to table for display
        let incoming_state_display = state
            .incoming
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
            .internal
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
