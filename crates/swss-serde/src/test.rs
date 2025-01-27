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
fn fvs_round_trip() {
    macro_rules! round_trip {
        ($val:expr, $ty:ty) => {{
            let val1: $ty = $val;
            let fvs = to_field_values(&val1).unwrap();
            println!("{val1:?} => {fvs:?}");
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
}

#[test]
fn fvs_missing_field_is_none() {
    #[derive(Deserialize, PartialEq, Eq, Debug)]
    struct S {
        x: Option<i32>,
        y: Option<i32>,
        z: Option<i32>,
    }

    assert_eq!(
        from_field_values::<S>(&FieldValues::default()).unwrap(),
        S {
            x: None,
            y: None,
            z: None
        }
    );
}

#[test]
fn single_fv_round_trip() {
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    #[serde(rename_all = "lowercase")]
    enum E {
        A,
        SomeLongNameHere,
    }

    macro_rules! round_trip {
        ($val:expr, $ty:ty) => {{
            let val1: $ty = $val;
            let fv = serialize_field_value(&val1).unwrap();
            println!("{} => {fv:?}", stringify!($val));
            let val2: $ty = deserialize_field_value(Some(&fv)).unwrap();
            assert_eq!(val1, val2);
        }};
    }

    round_trip!(123456789, i32);
    round_trip!(String::from("hello!"), String);
    round_trip!("X", &str);
    round_trip!(E::A, E);
    round_trip!(E::SomeLongNameHere, E);
    round_trip!(vec![], Vec<E>);
    round_trip!(vec![E::A, E::A, E::SomeLongNameHere], Vec<E>);
    round_trip!(Some(123), Option<i32>);
    round_trip!(None, Option<i32>);
}

#[test]
fn single_fv_missing_field_is_none() {
    assert_eq!(deserialize_field_value::<Option<i32>>(None).unwrap(), None);
}
