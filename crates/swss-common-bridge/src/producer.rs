use swbus_actor::prelude::*;
use swss_common::{KeyOperation, ProducerStateTable};

pub struct ProducerTableBridge {
    table: ProducerStateTable,
}

impl ProducerTableBridge {
    pub fn new(table: ProducerStateTable) -> Self {
        ProducerTableBridge { table }
    }
}

impl Actor for ProducerTableBridge {
    async fn init(&mut self, _outbox: Outbox) {}

    async fn handle_message(&mut self, message: IncomingMessage, outbox: Outbox) {
        match &message.body {
            MessageBody::Request(DataRequest { payload }) => {
                let response = match handle_kfvs(&mut self.table, payload).await {
                    Ok(()) => OutgoingMessage::ok_response(message),
                    Err((code, msg)) => OutgoingMessage::error_response(message, code, msg),
                };

                outbox.send(response).await;
            }
            MessageBody::Response(_) => { /* ignore responses, we should never get any anyway */ }
        }
    }
}

async fn handle_kfvs(table: &mut ProducerStateTable, payload: &[u8]) -> Result<(), (SwbusErrorCode, String)> {
    let kfvs = crate::decode_kfvs(payload)
        .map_err(|e| (SwbusErrorCode::InvalidPayload, format!("error decoding kfvs: {e}")))?;

    match kfvs.operation {
        KeyOperation::Set => {
            table
                .set_async(&kfvs.key, kfvs.field_values)
                .await
                .expect("ProducerStateTable::set threw an exception");
        }
        KeyOperation::Del => {
            table
                .del_async(&kfvs.key)
                .await
                .expect("ProducerStateTable::del threw an exception");
        }
    }

    Ok(())
}
