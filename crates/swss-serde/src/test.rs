use crate::{
    field_value::{deserialize_field_value, serialize_field_value},
    from_field_values, to_field_values, FieldValues,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
enum E {
    A,
    SomeLongName,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct S {
    a: i32,
    b: String,
    c: Vec<E>,
    d: Option<i32>,
    e: Option<i32>,
}

#[test]
fn field_values() {
    macro_rules! round_trip {
        ($val:expr, $ty:ty) => {{
            let val1: $ty = $val;
            let fvs = to_field_values(&val1).unwrap();
            let val2: $ty = from_field_values(&fvs).unwrap();
            assert_eq!(val1, val2);
        }};
    }

    round_trip!(
        S {
            a: 5,
            b: "Hello!".into(),
            c: vec![E::A, E::SomeLongName, E::A],
            d: Some(32131),
            e: None
        },
        S
    );
    round_trip!(
        S {
            a: i32::MAX,
            b: "".into(),
            c: vec![],
            d: None,
            e: Some(i32::MIN),
        },
        S
    );
}

#[test]
fn field_value() {
    macro_rules! round_trip {
        ($val:expr, $ty:ty) => {{
            let val1: $ty = $val;
            let fv = serialize_field_value(&val1).unwrap();
            let val2: $ty = deserialize_field_value(Some(&fv)).unwrap();
            assert_eq!(val1, val2);
        }};
    }

    round_trip!(123456789, i32);
    round_trip!(i64::MAX, i64);
    round_trip!(i64::MIN, i64);
    round_trip!(String::from("hello!"), String);
    round_trip!("X", &str);
    round_trip!(E::A, E);
    round_trip!(E::SomeLongName, E);
    round_trip!(vec![], Vec<E>);
    round_trip!(vec![E::A, E::A, E::SomeLongName], Vec<E>);
    round_trip!(Some(123), Option<i32>);
    round_trip!(Some(i32::MIN), Option<i32>);
    round_trip!(Some(i32::MAX), Option<i32>);
    round_trip!(None, Option<i32>);
}

#[test]
fn missing_field_is_none() {
    // Test on a single FieldValue
    assert_eq!(deserialize_field_value::<Option<i32>>(None).unwrap(), None);

    // Test on multiple FieldValues in a struct
    #[derive(Deserialize, PartialEq, Eq, Debug)]
    struct S {
        x: Option<i32>,
        y: Option<i32>,
        z: Option<i32>,
    }
    assert_eq!(
        // Empty FieldValues is like the DB had no fields in it
        from_field_values::<S>(&FieldValues::default()).unwrap(),
        S {
            x: None,
            y: None,
            z: None
        }
    );
}

#[test]
fn option_values() {
    assert_eq!(serialize_field_value(&Some(123)).unwrap(), "123");
    assert_eq!(serialize_field_value(&None::<i32>).unwrap(), "none");
    assert_eq!(
        deserialize_field_value::<Option<i32>>(Some(&"123".into())).unwrap(),
        Some(123)
    );
    assert_eq!(
        deserialize_field_value::<Option<i32>>(Some(&"none".into())).unwrap(),
        None
    );
    assert_eq!(deserialize_field_value::<Option<i32>>(None).unwrap(), None);
}

#[test]
fn enums_are_case_insensitive() {
    assert_eq!(serialize_field_value(&E::A).unwrap(), "a");
    assert_eq!(serialize_field_value(&E::SomeLongName).unwrap(), "somelongname");
    assert_eq!(deserialize_field_value::<E>(Some(&"a".into())).unwrap(), E::A);
    assert_eq!(
        deserialize_field_value::<E>(Some(&"somelongname".into())).unwrap(),
        E::SomeLongName
    );
    assert_eq!(
        deserialize_field_value::<E>(Some(&"SoMeLoNgNaMe".into())).unwrap(),
        E::SomeLongName
    );
}
