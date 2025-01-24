use crate::{
    error::Error,
    field_value::{serialize_field_value, FieldValueDeserializer},
};
use serde::{
    de::{DeserializeSeed, Error as _, SeqAccess, Visitor},
    forward_to_deserialize_any,
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize, Serializer,
};
use serde_serializer_quick_unsupported::serializer_unsupported;
use swss_common::FieldValues;

pub fn serialize_field_values<T: Serialize + ?Sized>(value: &T) -> Result<FieldValues, Error> {
    value.serialize(FieldValuesSerializer)
}

pub fn deserialize_field_values<'a, T: Deserialize<'a>>(fvs: &'a FieldValues) -> Result<T, Error> {
    T::deserialize(FieldValuesDeserializer { fvs })
}

struct FieldValuesSerializer;

impl Serializer for FieldValuesSerializer {
    type Ok = FieldValues;
    type Error = Error;

    type SerializeStruct = FieldValuesStructSerializer;

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(FieldValuesStructSerializer::default())
    }

    // Unsupported types
    serializer_unsupported! {
        err = (Error::custom("FieldValues can only be serialized from a struct"));
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str bytes none some unit unit_struct
        unit_variant newtype_struct newtype_variant seq tuple tuple_struct tuple_variant map
        struct_variant i128 u128
    }
}

#[derive(Default)]
struct FieldValuesStructSerializer {
    fvs: FieldValues,
}

impl SerializeStruct for FieldValuesStructSerializer {
    type Ok = FieldValues;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error> {
        match serialize_field_value(value) {
            Ok(s) => {
                self.fvs.insert(key.to_string(), s);
                Ok(())
            }
            Err(e) => Err(Error::custom(format!("serializing field '{key}': {e}"))),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.fvs)
    }
}

struct FieldValuesDeserializer<'a> {
    fvs: &'a FieldValues,
}

impl<'de> Deserializer<'de> for FieldValuesDeserializer<'de> {
    type Error = Error;

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_seq(FieldValuesSeqDeserializer { fields, fvs: self.fvs })
    }

    // Unsupported types
    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf option unit
        unit_struct newtype_struct seq tuple tuple_struct map enum ignored_any identifier
    }

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(Error::custom("FieldValues can only be deserialized into a struct"))
    }
}

struct FieldValuesSeqDeserializer<'a> {
    fields: &'static [&'static str],
    fvs: &'a FieldValues,
}

impl<'de> SeqAccess<'de> for FieldValuesSeqDeserializer<'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error> {
        match self.fields.first() {
            Some(field) => {
                self.fields = &self.fields[1..];
                seed.deserialize(FieldValueDeserializer::new(self.fvs.get(*field)))
                    .map(Some)
            }
            None => Ok(None),
        }
    }
}
