use crate::contracts::swbus::SwbusErrorCode;
use contracts::requires;
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SwbusError {
    #[error("Connection:{code:?} - {detail}")]
    ConnectionError { code: SwbusErrorCode, detail: io::Error },

    #[error("Input:{code:?} - {detail}")]
    InputError { code: SwbusErrorCode, detail: String },

    #[error("Route:{code:?} - {detail}")]
    RouteError { code: SwbusErrorCode, detail: String },

    #[error("Internal:{code:?} - {detail}")]
    InternalError { code: SwbusErrorCode, detail: String },
}

impl SwbusError {
    #[requires(code > SwbusErrorCode::ConnectionErrorMin && code < SwbusErrorCode::ConnectionErrorMax)]
    pub fn connection(code: SwbusErrorCode, detail: io::Error) -> Self {
        SwbusError::ConnectionError { code, detail }
    }

    #[requires(code > SwbusErrorCode::InputErrorMin && code < SwbusErrorCode::InputErrorMax)]
    pub fn input(code: SwbusErrorCode, detail: String) -> Self {
        SwbusError::InputError { code, detail }
    }

    #[requires(code > SwbusErrorCode::RouteErrorMin && code < SwbusErrorCode::RouteErrorMax)]
    pub fn route(code: SwbusErrorCode, detail: String) -> Self {
        SwbusError::RouteError { code, detail }
    }

    #[requires(code > SwbusErrorCode::InternalErrorMin && code < SwbusErrorCode::InternalErrorMax)]
    pub fn internal(code: SwbusErrorCode, detail: String) -> Self {
        SwbusError::InternalError { code, detail }
    }
}

pub type Result<T, E = SwbusError> = core::result::Result<T, E>;

mod tests {
    use super::*;

    #[test]
    fn swbus_error_can_be_displayed() {
        let error = SwbusError::connection(
            SwbusErrorCode::ConnectionError,
            io::Error::new(io::ErrorKind::NotConnected, "Connection error"),
        );
        assert_eq!(error.to_string(), "Connection:ConnectionError - Connection error");

        let error = SwbusError::input(SwbusErrorCode::InvalidArgs, "Input error".to_string());
        assert_eq!(error.to_string(), "Input:InvalidArgs - Input error");

        let error = SwbusError::route(SwbusErrorCode::NoRoute, "Route error".to_string());
        assert_eq!(error.to_string(), "Route:NoRoute - Route error");

        let error = SwbusError::internal(SwbusErrorCode::Fail, "Internal error".to_string());
        assert_eq!(error.to_string(), "Internal:Fail - Internal error");
    }
}
