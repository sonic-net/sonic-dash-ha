use crate::{
    field_value::{deserialize_field_value, serialize_field_value},
    from_field_values, from_table, to_field_values, to_table,
};
use serde::{Deserialize, Serialize};
use swss_common::{FieldValues, Table};
use swss_common_testing::{random_string, Redis};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
enum E {
    VariantA,
    VariantB,
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
            b: random_string(),
            c: vec![E::VariantA, E::VariantB, E::VariantA],
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
fn table() {
    let redis = Redis::start();
    let table = Table::new(redis.db_connector(), "mytable").unwrap();

    macro_rules! round_trip {
        ($val:expr, $ty:ty) => {{
            let val1: $ty = $val;
            to_table(&val1, &table, "mykey").unwrap();
            let val2: $ty = from_table(&table, "mykey").unwrap();
            assert_eq!(val1, val2);
        }};
    }

    round_trip!(
        S {
            a: i32::MAX,
            b: random_string(),
            c: vec![E::VariantA, E::VariantA, E::VariantB, E::VariantA],
            d: Some(i32::MIN),
            e: None
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
    round_trip!(E::VariantA, E);
    round_trip!(E::VariantB, E);
    round_trip!(vec![], Vec<E>);
    round_trip!(vec![E::VariantA, E::VariantA, E::VariantB], Vec<E>);
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
    #[derive(Deserialize, PartialEq, Eq, Debug, Default)]
    struct S {
        x: Option<i32>,
        y: Option<i32>,
        z: Option<i32>,
    }
    assert_eq!(
        // Empty FieldValues is like the DB had no fields in it
        from_field_values::<S>(&FieldValues::default()).unwrap(),
        S::default()
    );

    // Test on an empty redis instance
    let redis = Redis::start();
    let table = Table::new(redis.db_connector(), "mytable").unwrap();
    assert_eq!(from_table::<S>(&table, "emptykey").unwrap(), S::default())
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
    assert_eq!(serialize_field_value(&E::VariantA).unwrap(), "varianta");
    assert_eq!(serialize_field_value(&E::VariantB).unwrap(), "variantb");
    assert_eq!(
        deserialize_field_value::<E>(Some(&"VARIANTA".into())).unwrap(),
        E::VariantA
    );
    assert_eq!(
        deserialize_field_value::<E>(Some(&"VARIANTB".into())).unwrap(),
        E::VariantB
    );
    assert_eq!(
        deserialize_field_value::<E>(Some(&"VaRiAnTb".into())).unwrap(),
        E::VariantB
    );
}
