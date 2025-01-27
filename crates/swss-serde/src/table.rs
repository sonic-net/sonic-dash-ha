use crate::{
    field_value::{serialize_field_value, FieldValueDeserializer},
    Error,
};
use serde::{
    de::{DeserializeOwned, DeserializeSeed, SeqAccess, Visitor},
    forward_to_deserialize_any,
    ser::SerializeStruct,
    Deserializer, Serialize, Serializer,
};
use serde_serializer_quick_unsupported::serializer_unsupported;
use swss_common::Table;

pub(crate) fn serialize_to_table<T: Serialize + ?Sized>(value: &T, table: &Table, key: &str) -> Result<(), Error> {
    value.serialize(TableSerializer { table, key })
}

pub(crate) fn deserialize_from_table<T: DeserializeOwned>(table: &Table, key: &str) -> Result<T, Error> {
    T::deserialize(TableDeserializer { table, key })
}

struct TableSerializer<'a, 'b> {
    table: &'a Table,
    key: &'b str,
}

impl<'a, 'b> Serializer for TableSerializer<'a, 'b> {
    type Ok = ();
    type Error = Error;
    type SerializeStruct = Self;

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    // Unsupported types
    serializer_unsupported! {
        err = (Error::new("FieldValues can only be serialized from a struct"));
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str bytes none some unit unit_struct
        unit_variant newtype_struct newtype_variant seq tuple tuple_struct tuple_variant map
        struct_variant i128 u128
    }
}

impl<'a, 'b> SerializeStruct for TableSerializer<'a, 'b> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, field: &'static str, value: &T) -> Result<(), Self::Error> {
        let fv = serialize_field_value(value)?;
        self.table.hset(self.key, field, &fv).map_err(|e| Error::new(e))?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

struct TableDeserializer<'a, 'b> {
    table: &'a Table,
    key: &'b str,
}

impl<'a, 'b, 'de> Deserializer<'de> for TableDeserializer<'a, 'b> {
    type Error = Error;

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_seq(TableSeqDeserializer {
            table: self.table,
            key: self.key,
            fields,
        })
    }

    // Unsupported types
    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf option unit
        unit_struct newtype_struct seq tuple tuple_struct map enum ignored_any identifier
    }

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(Error::new("a Table can only be deserialized into a struct"))
    }
}

struct TableSeqDeserializer<'a, 'b> {
    table: &'a Table,
    key: &'b str,
    fields: &'static [&'static str],
}

impl<'a, 'b, 'de> SeqAccess<'de> for TableSeqDeserializer<'a, 'b> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error> {
        match self.fields.first() {
            Some(field) => {
                self.fields = &self.fields[1..];
                let value = self.table.hget(self.key, field).map_err(|e| Error::new(e))?;
                seed.deserialize(FieldValueDeserializer::new(value.as_ref())).map(Some)
            }
            None => Ok(None),
        }
    }
}
