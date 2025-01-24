use std::{
    fmt::Display,
    num::{ParseFloatError, ParseIntError},
    str::Utf8Error,
};

#[derive(Debug)]
pub struct Error {
    message: String,
}

impl Error {
    pub(crate) fn new(x: impl ToString) -> Self {
        Self { message: x.to_string() }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl serde::ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Self::new(msg)
    }
}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Self::new(msg)
    }
}

impl From<Utf8Error> for Error {
    fn from(value: Utf8Error) -> Self {
        Self::new(value)
    }
}

impl From<ParseIntError> for Error {
    fn from(value: ParseIntError) -> Self {
        Self::new(value)
    }
}

impl From<ParseFloatError> for Error {
    fn from(value: ParseFloatError) -> Self {
        Self::new(value)
    }
}
