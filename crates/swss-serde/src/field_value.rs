use crate::error::Error;
use serde::{
    de::{value::StrDeserializer, Error as _, SeqAccess, Unexpected, Visitor},
    forward_to_deserialize_any,
    ser::SerializeSeq,
    Deserialize, Deserializer, Serialize, Serializer,
};
use serde_serializer_quick_unsupported::serializer_unsupported;
use std::{any::type_name, str, str::FromStr};
use swss_common::CxxString;

pub(crate) fn serialize_field_value<T: Serialize + ?Sized>(value: &T) -> Result<CxxString, Error> {
    value.serialize(FieldValueSerializer)
}

#[cfg_attr(not(test), allow(dead_code))] // used in tests
pub(crate) fn deserialize_field_value<'a, T: Deserialize<'a>>(fv: Option<&'a CxxString>) -> Result<T, Error> {
    T::deserialize(FieldValueDeserializer {
        data: fv.map(|s| s.as_bytes()),
    })
}

struct FieldValueSerializer;

impl Serializer for FieldValueSerializer {
    type Ok = CxxString;
    type Error = Error;
    type SerializeSeq = FieldValueSeqSerializer;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string().into())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(v.into())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(v.into())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(v.into())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string().into())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(v.into())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(v.into())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(v.into())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string().into())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.serialize_f64(v.into())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_string().into())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(v.into())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(v.into())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(CxxString::new("none"))
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        value.serialize(Self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(FieldValueSeqSerializer::default())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        if variant.chars().all(char::is_lowercase) {
            Ok(variant.into())
        } else {
            Ok(variant.to_lowercase().into())
        }
    }

    // Unsupported types
    serializer_unsupported! {
        err = (Error::custom("unsupported field value type"));
        char struct unit unit_struct newtype_struct newtype_variant tuple tuple_struct tuple_variant map struct_variant
    }
}

#[derive(Default)]
struct FieldValueSeqSerializer {
    elements: Vec<CxxString>,
}

impl SerializeSeq for FieldValueSeqSerializer {
    type Ok = CxxString;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.elements.push(value.serialize(FieldValueSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut buf = Vec::<u8>::new();
        for s in self.elements {
            buf.extend_from_slice(s.as_bytes());
            buf.push(b',');
        }
        if !buf.is_empty() {
            buf.pop();
        }
        Ok(buf.into())
    }
}

pub(crate) struct FieldValueDeserializer<'a> {
    data: Option<&'a [u8]>,
}

impl<'a> FieldValueDeserializer<'a> {
    pub(crate) fn new(fv: Option<&'a CxxString>) -> Self {
        Self {
            data: fv.map(|s| s.as_bytes()),
        }
    }
}

impl<'a> FieldValueDeserializer<'a> {
    fn data(&self) -> Result<&'a [u8], Error> {
        match self.data {
            Some(s) => Ok(s),
            None => Err(Error::custom("field is missing")),
        }
    }

    fn to_str(&self) -> Result<&'a str, Error> {
        let data = self.data()?;
        str::from_utf8(data)
            .map_err(|_| Error::invalid_value(Unexpected::Str(&String::from_utf8_lossy(data)), &"valid UTF-8 data"))
    }

    fn parse<T: FromStr>(&self) -> Result<T, Error> {
        let str = self.to_str()?;
        let t = str
            .parse()
            .map_err(|_| Error::invalid_value(Unexpected::Str(str), &&*format!("a valid {}", type_name::<T>())))?;
        Ok(t)
    }
}

impl<'de> Deserializer<'de> for FieldValueDeserializer<'de> {
    type Error = Error;

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_bool(self.parse()?)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i8(self.parse()?)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i16(self.parse()?)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i32(self.parse()?)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i64(self.parse()?)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u8(self.parse()?)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u16(self.parse()?)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u32(self.parse()?)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u64(self.parse()?)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_f32(self.parse()?)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_f64(self.parse()?)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_borrowed_str(self.to_str()?)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_string(self.parse()?)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_borrowed_bytes(self.data()?)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_byte_buf(self.data()?.to_owned())
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.data {
            Some(b"none") => visitor.visit_none(),
            Some(_) => visitor.visit_some(self),
            None => visitor.visit_none(),
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        fn streq_case_insensitive(a: &str, b: &str) -> bool {
            a.chars()
                .zip(b.chars())
                .all(|(ca, cb)| ca.to_ascii_lowercase() == cb.to_ascii_lowercase())
        }

        // Find the variant which matches (SomeLongName == "somelongname"), or give the data str directly as a failsafe
        let str = self.to_str()?;
        let variant = variants
            .iter()
            .copied()
            .find(|v| streq_case_insensitive(str, v))
            .unwrap_or(str);
        visitor.visit_enum(StrDeserializer::new(variant))
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let data = self.data()?;
        if data.is_empty() {
            // Empty string -> empty list
            visitor.visit_seq(FieldValueSeqDeserializer {
                iter: std::iter::empty(),
            })
        } else {
            visitor.visit_seq(FieldValueSeqDeserializer {
                iter: data.split(|&c| c == b','),
            })
        }
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    // Unsupported types
    forward_to_deserialize_any! {
        char unit unit_struct ignored_any newtype_struct tuple tuple_struct map struct
    }

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(Error::custom("deserializing an unsupported type"))
    }
}

struct FieldValueSeqDeserializer<T> {
    iter: T,
}

impl<'de, T: Iterator<Item = &'de [u8]>> SeqAccess<'de> for FieldValueSeqDeserializer<T> {
    type Error = Error;

    fn next_element_seed<S>(&mut self, seed: S) -> Result<Option<S::Value>, Self::Error>
    where
        S: serde::de::DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some(bytes) => seed.deserialize(FieldValueDeserializer { data: Some(bytes) }).map(Some),
            None => Ok(None),
        }
    }
}
