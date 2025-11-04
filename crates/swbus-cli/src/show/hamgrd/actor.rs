use crate::show::ShowCmdHandler;
use crate::CommandContext;
use chrono::{DateTime, Local};
use clap::Parser;
use serde_json::to_string_pretty;
use swbus_actor::state::{
    incoming::IncomingTableEntry, internal::InternalTableData, outgoing::get_elapsed_time, outgoing::SentMessageEntry,
    outgoing::UnackedMessage, ActorStateDump,
};
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
        let formatted_value = Self::format_message_value(&state.msg.data);

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
                value: formatted_value,
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

    fn format_message_value(data: &serde_json::Value) -> String {
        let formatted = Self::try_format_protobuf_json(data);
        formatted.unwrap_or_else(|| to_string_pretty(data).unwrap_or_else(|_| "INV".to_string()))
    }

    fn try_format_protobuf_json(data: &serde_json::Value) -> Option<String> {
        let obj = data.as_object()?;
        let field_values = obj.get("field_values")?.as_object()?;
        let json_str = field_values.get("json")?.as_str()?;

        let parsed = serde_json::from_str::<serde_json::Value>(json_str).ok()?;

        let mut new_obj = obj.clone();
        let new_field_values = new_obj.get_mut("field_values")?.as_object_mut()?;
        new_field_values.insert("json".to_string(), parsed);

        to_string_pretty(&new_obj).ok()
    }
}

#[derive(Tabled)]
struct OutgoingUnackedMessageDisplay {
    message: String,
}

impl OutgoingUnackedMessageDisplay {
    fn from_outgoing_state(state: &UnackedMessage) -> Self {
        let details = vec![
            KeyValue {
                attribute: "actor-message/key".to_string(),
                value: state.actor_message.key.clone(),
            },
            KeyValue {
                attribute: "actor-message/value".to_string(),
                value: to_string_pretty(&state.actor_message.data).unwrap_or("INV".to_string()),
            },
            KeyValue {
                attribute: "swbus-message/header".to_string(),
                value: {
                    match &state.swbus_message.header {
                        Some(header) => to_string_pretty(header).unwrap_or("INV".to_string()),
                        None => "".to_string(),
                    }
                },
            },
            KeyValue {
                attribute: "time-elapsed".to_string(),
                value: format!("{}", get_elapsed_time(&state.time_sent)),
            },
        ];
        let table = Table::new(details).with(Style::ascii().remove_frame()).to_string();
        OutgoingUnackedMessageDisplay { message: table }
    }
}

#[derive(Tabled)]
struct OutgoingSentStateDisplay {
    key: String,
    details: String,
}

impl OutgoingSentStateDisplay {
    fn from_outgoing_state((key, state): (&String, &SentMessageEntry)) -> Self {
        let details = vec![
            KeyValue {
                attribute: "response_source".to_string(),
                value: {
                    match &state.response_source {
                        Some(response_source) => response_source.to_longest_path(),
                        None => "".to_string(),
                    }
                },
            },
            KeyValue {
                attribute: "id".to_string(),
                value: state.id.to_string(),
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
                attribute: "last-sent-time".to_string(),
                value: unix_secs_to_string(state.last_sent_time),
            },
            KeyValue {
                attribute: "response".to_string(),
                value: {
                    match &state.response {
                        Some(response) => response.to_string(),
                        None => "".to_string(),
                    }
                },
            },
            KeyValue {
                attribute: "acked".to_string(),
                value: state.acked.to_string(),
            },
        ];
        let table = Table::new(details).with(Style::ascii().remove_frame()).to_string();
        OutgoingSentStateDisplay {
            key: key.clone(),
            details: table,
        }
    }
}

#[derive(Tabled)]
struct InternalStateDisplay {
    key: String,
    table_meta: String,
    fields: String,
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

        let mut committed_fvs = state
            .backup_fvs
            .iter()
            .map(|(key, value)| KeyValue {
                attribute: key.clone(),
                value: value.to_string_lossy().into_owned(),
            })
            .collect::<Vec<KeyValue>>();

        committed_fvs.sort_by(|a, b| a.attribute.cmp(&b.attribute));

        let committed_fvs = Table::new(committed_fvs)
            .with(Style::ascii().remove_frame())
            .to_string();
        InternalStateDisplay {
            key: key.clone(),
            table_meta,
            fields: committed_fvs,
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

    fn process_response(&self, _: &CommandContext, response: &RequestResponse) {
        let result = match &response.response_body {
            Some(request_response::ResponseBody::ManagementQueryResult(ref result)) => &result.value,
            _ => {
                info!("Expecting RouteQueryResult but got something else: {:?}", response);
                return;
            }
        };

        let state: ActorStateDump = serde_json::from_str(result).unwrap();

        // convert to table for display
        let mut incoming_state_display: Vec<IncomingStateDisplay> = state
            .incoming
            .iter()
            .map(IncomingStateDisplay::from_incoming_state)
            .collect::<Vec<IncomingStateDisplay>>();

        incoming_state_display.sort_by(|a, b| a.key.cmp(&b.key));

        let incoming_state_table = Table::new(incoming_state_display)
            .with(Panel::header("Incoming State"))
            .with(Modify::list(Rows::first(), Alignment::center()))
            .with(Style::modern())
            .to_string();

        info!("{}", incoming_state_table);

        // convert to table for display
        let mut internal_state_display = state
            .internal
            .iter()
            .map(InternalStateDisplay::from_internal_state)
            .collect::<Vec<InternalStateDisplay>>();

        internal_state_display.sort_by(|a, b| a.key.cmp(&b.key));

        let internal_state_table = Table::new(internal_state_display)
            .with(Panel::header("Internal State"))
            .with(Modify::list(Rows::first(), Alignment::center()))
            .with(Style::modern())
            .to_string();

        info!("{}", internal_state_table);

        // convert to table for display
        let mut outgoing_sent_state_display = state
            .outgoing
            .outgoing_sent
            .iter()
            .map(OutgoingSentStateDisplay::from_outgoing_state)
            .collect::<Vec<OutgoingSentStateDisplay>>();

        outgoing_sent_state_display.sort_by(|a, b| a.key.cmp(&b.key));

        let outgoing_sent_state_table = Table::new(outgoing_sent_state_display)
            .with(Panel::header("Outgoing Sent Message State"))
            .with(Modify::list(Rows::first(), Alignment::center()))
            .with(Style::modern())
            .to_string();

        info!("{}", outgoing_sent_state_table);

        if !state.outgoing.outgoing_queued.is_empty() {
            // convert to table for display
            let outgoing_queued_state_display = state
                .outgoing
                .outgoing_queued
                .iter()
                .map(OutgoingUnackedMessageDisplay::from_outgoing_state)
                .collect::<Vec<OutgoingUnackedMessageDisplay>>();
            let outgoing_queued_state_table = Table::new(outgoing_queued_state_display)
                .with(Panel::header("Outgoing Queued Message State"))
                .with(Modify::list(Rows::first(), Alignment::center()))
                .with(Style::modern())
                .to_string();

            info!("{}", outgoing_queued_state_table);
        }
    }
}
