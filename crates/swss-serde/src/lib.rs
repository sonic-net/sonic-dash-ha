// Ignore this lint in serializer_unsupported! macro
#![allow(clippy::multiple_bound_locations)]

mod error;
mod field_value;
mod field_values;
mod table;

#[cfg(test)]
mod test;

use serde::{de::DeserializeOwned, Serialize};
use swss_common::{FieldValues, Table};

/// Purely informational error representing anything that went wrong in `swss-serde`.
///
/// These can be serialization/deserialization errors or IO errors in the case of `*_table` functions.
pub use error::Error;

/// Convert a rust struct into a [`FieldValues`] map.
pub fn to_field_values<T: Serialize + ?Sized>(value: &T) -> Result<FieldValues, Error> {
    field_values::serialize_field_values(value)
}

/// Convert a [`FieldValues`] map into a rust struct.
pub fn from_field_values<T: DeserializeOwned>(fvs: &FieldValues) -> Result<T, Error> {
    field_values::deserialize_field_values(fvs)
}

/// Write a rust struct into an swss::[`Table`].
///
/// If this function returns an error, the table's field are in an undefined state and should be reset.
pub fn to_table<T: Serialize + ?Sized>(value: &T, table: &Table, key: &str) -> Result<(), Error> {
    table::serialize_to_table(value, table, key)
}

/// Read a rust struct from an swss::[`Table`].
pub fn from_table<T: DeserializeOwned>(table: &Table, key: &str) -> Result<T, Error> {
    table::deserialize_from_table(table, key)
}
