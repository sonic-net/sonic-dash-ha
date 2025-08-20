use crate::{
    error::Error,
    field_value::{serialize_field_value, FieldValueDeserializer},
};
use serde::{
    de::{DeserializeOwned, DeserializeSeed, IntoDeserializer, MapAccess, Visitor}, // Added IntoDeserializer
    forward_to_deserialize_any,
    ser::SerializeStruct,
    Deserializer,
    Serialize,
    Serializer,
};
use serde_serializer_quick_unsupported::serializer_unsupported;
use swss_common::{CxxString, FieldValues};

pub(crate) fn serialize_field_values<T: Serialize + ?Sized>(value: &T) -> Result<FieldValues, Error> {
    value.serialize(FieldValuesSerializer)
}

pub(crate) fn deserialize_field_values<T: DeserializeOwned>(fvs: &FieldValues) -> Result<T, Error> {
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

    serializer_unsupported! {
        err = (Error::new("FieldValues can only be serialized from a struct"));
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
            Err(e) => Err(Error::new(format!("serializing field '{key}': {e}"))),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.fvs)
    }
}

struct FieldValuesDeserializer<'a> {
    fvs: &'a FieldValues,
}

impl<'de: 'a, 'a> Deserializer<'de> for FieldValuesDeserializer<'a> {
    type Error = Error;

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        // Switch to visit_map and use the new FieldValuesMapAccess
        visitor.visit_map(FieldValuesMapAccess::new(self.fvs))
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf option unit
        unit_struct newtype_struct seq tuple tuple_struct map enum ignored_any identifier
    }

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(Error::new("FieldValues can only be deserialized into a struct"))
    }
}

// New struct implementing MapAccess
struct FieldValuesMapAccess<'a> {
    iter: std::collections::hash_map::Iter<'a, String, CxxString>,
    // To store the value corresponding to the key yielded by next_key_seed
    current_value_for_key: Option<&'a CxxString>,
}

impl<'a> FieldValuesMapAccess<'a> {
    fn new(fvs: &'a FieldValues) -> Self {
        FieldValuesMapAccess {
            iter: fvs.iter(),
            current_value_for_key: None,
        }
    }
}

impl<'de: 'a, 'a> MapAccess<'de> for FieldValuesMapAccess<'a> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some((key_string, value_cxx_string)) => {
                // Store the value for the next call to next_value_seed
                self.current_value_for_key = Some(value_cxx_string);
                // Deserialize the key string.
                // The seed K will typically expect a string-like deserializer.
                seed.deserialize(key_string.as_str().into_deserializer()).map(Some)
            }
            None => {
                // No more entries in the map
                self.current_value_for_key = None;
                Ok(None)
            }
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        // Take the stored value. It must have been set by a preceding next_key_seed call.
        match self.current_value_for_key.take() {
            Some(value_cxx_string) => {
                // Deserialize the CxxString value using FieldValueDeserializer
                seed.deserialize(FieldValueDeserializer::new(Some(value_cxx_string)))
            }
            None => {
                // This indicates a contract violation by the caller (serde's Visitor)
                // or an internal logic error.
                Err(Error::new(
                    "MapAccess: next_value_seed called without a preceding successful next_key_seed or after iteration ended.",
                ))
            }
        }
    }

    fn size_hint(&self) -> Option<usize> {
        // Provide a hint of the number of remaining key-value pairs.
        let (lower, _upper) = self.iter.size_hint();
        Some(lower)
    }
}
