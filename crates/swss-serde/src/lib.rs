// Ignore this lint in serializer_unsupported! macro
#![allow(clippy::multiple_bound_locations)]

mod error;
mod field_value;
mod field_values;

#[cfg(test)]
mod test;

use serde::{Deserialize, Serialize};

pub use error::Error;
pub use swss_common::FieldValues;

pub fn to_field_values<T: Serialize + ?Sized>(value: &T) -> Result<FieldValues, Error> {
    field_values::serialize_field_values(value)
}

pub fn from_field_values<'a, T: Deserialize<'a>>(fvs: &'a FieldValues) -> Result<T, Error> {
    field_values::deserialize_field_values(fvs)
}
