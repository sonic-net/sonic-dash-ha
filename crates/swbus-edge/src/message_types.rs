use swbus_proto::swbus::{
    swbus_message, DataRequest, RequestResponse, ServicePath, SwbusErrorCode, SwbusMessage, SwbusMessageHeader,
};
use thiserror::Error;

/// A simplified version of [`swbus_message::Body`], only representing a `DataRequest` or `Response`.
pub enum MessageBody {
    Request {
        payload: Vec<u8>,
    },
    Response {
        request_id: u64,
        error_code: SwbusErrorCode,
        error_message: String,
    },
}

impl Into<swbus_message::Body> for MessageBody {
    fn into(self) -> swbus_message::Body {
        match self {
            MessageBody::Request { payload } => swbus_message::Body::DataRequest(DataRequest { payload }),
            MessageBody::Response {
                request_id,
                error_code,
                error_message,
            } => swbus_message::Body::Response(RequestResponse {
                request_id,
                error_code: error_code as i32,
                error_message,
            }),
        }
    }
}

#[derive(Error, Debug)]
#[error("Infra message leaked to client. This is a bug in swbus-edge. {0:?}")]
pub struct InfraMessageLeaked(swbus_message::Body);

impl TryFrom<swbus_message::Body> for MessageBody {
    type Error = InfraMessageLeaked;

    fn try_from(body: swbus_message::Body) -> Result<Self, Self::Error> {
        match body {
            swbus_message::Body::DataRequest(req) => Ok(MessageBody::Request { payload: req.payload }),
            swbus_message::Body::Response(resp) => Ok(MessageBody::Response {
                request_id: resp.request_id,
                error_code: SwbusErrorCode::try_from(resp.error_code).expect("invalid SwbusErrorCode"),
                error_message: resp.error_message,
            }),
            msg => Err(InfraMessageLeaked(msg)),
        }
    }
}

pub struct IncomingMessage {
    id: u64,
    source: ServicePath,
    body: MessageBody,
}

impl TryFrom<SwbusMessage> for IncomingMessage {
    type Error = InfraMessageLeaked;

    fn try_from(msg: SwbusMessage) -> Result<Self, Self::Error> {
        let header = msg.header.unwrap();
        let body = msg.body.unwrap();
        Ok(Self {
            id: header.id,
            source: header.source.unwrap(),
            body: body.try_into()?,
        })
    }
}

pub struct OutgoingMessage {
    source: ServicePath,
    destination: ServicePath,
    body: MessageBody,
}

impl Into<SwbusMessage> for OutgoingMessage {
    fn into(self) -> SwbusMessage {
        let header = SwbusMessageHeader::new(self.source, self.destination);
        let body = self.body.into();
        SwbusMessage {
            header: Some(header),
            body: Some(body),
        }
    }
}
