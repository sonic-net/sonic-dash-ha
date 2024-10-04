use std::{
    any::Any,
    borrow::{Borrow, Cow},
    collections::HashMap,
    error::Error,
    fmt::{Debug, Display},
    hash::Hash,
    mem,
    ops::Deref,
    ptr::{self, NonNull},
    slice,
    str::{self, FromStr, Utf8Error},
};

use crate::{bindings::*, cstr, str};

/// A C++ `std::string` that can be moved around and accessed from Rust.
#[repr(transparent)]
#[derive(PartialOrd, Eq)]
pub struct CxxString {
    ptr: NonNull<SWSSStringOpaque>,
}

impl CxxString {
    /// Take the object and replace the argument with null.
    /// This is to avoid copying the pointer and later double-freeing it.
    /// This takes advantage of the fact that SWSSString_free specifically permits freeing a null SWSSStr.
    pub(crate) fn take_raw(s: &mut SWSSString) -> Option<CxxString> {
        let s = mem::replace(s, ptr::null_mut());
        NonNull::new(s).map(|ptr| CxxString { ptr })
    }

    pub(crate) fn as_raw(&self) -> SWSSString {
        self.ptr.as_ptr()
    }

    /// Shortcut for self.deref().as_raw()
    pub(crate) fn as_raw_ref(&self) -> SWSSStrRef {
        (**self).as_raw()
    }

    /// Copies the given data into a new C++ string.
    pub fn new(data: impl AsRef<[u8]>) -> CxxString {
        unsafe {
            let ptr = data.as_ref().as_ptr() as *const i8;
            let len = data.as_ref().len().try_into().unwrap();
            let mut obj = SWSSString_new(ptr, len);
            CxxString::take_raw(&mut obj).unwrap()
        }
    }
}

impl<T: AsRef<[u8]>> From<T> for CxxString {
    fn from(bytes: T) -> Self {
        CxxString::new(bytes.as_ref())
    }
}

impl Drop for CxxString {
    fn drop(&mut self) {
        unsafe { SWSSString_free(self.ptr.as_ptr()) }
    }
}

/// This calls [CxxStr::to_string_lossy] which may clone the string. Use sparingly to avoid potential copies.
impl Debug for CxxString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl Clone for CxxString {
    fn clone(&self) -> Self {
        CxxString::new(self.as_bytes())
    }
}

unsafe impl Send for CxxString {}

impl Deref for CxxString {
    type Target = CxxStr;

    fn deref(&self) -> &Self::Target {
        // SAFETY: CxxString and CxxStr are both repr(transparent) and identical in alignment &
        // size, and the C API guarantees that SWSSString can always be cast into SWSSStrRef
        unsafe { std::mem::transmute(self) }
    }
}

impl Ord for CxxString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deref().cmp(other)
    }
}

impl PartialEq for CxxString {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other)
    }
}

impl PartialEq<CxxStr> for CxxString {
    fn eq(&self, other: &CxxStr) -> bool {
        self.deref().eq(other)
    }
}

impl PartialEq<str> for CxxString {
    fn eq(&self, other: &str) -> bool {
        self.deref().eq(other)
    }
}

impl PartialEq<&str> for CxxString {
    fn eq(&self, other: &&str) -> bool {
        self.deref().eq(other)
    }
}

impl PartialEq<String> for CxxString {
    fn eq(&self, other: &String) -> bool {
        self.eq(other.as_str())
    }
}

impl Hash for CxxString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.deref().hash(state);
    }
}

impl Borrow<CxxStr> for CxxString {
    fn borrow(&self) -> &CxxStr {
        self.deref()
    }
}

/// Like Rust's String and str, this is equivalent to a C++ `std::string&` and can be derived from a [CxxString].
#[repr(transparent)]
#[derive(PartialOrd, Eq)]
pub struct CxxStr {
    ptr: NonNull<SWSSStrRefOpaque>,
}

impl CxxStr {
    pub(crate) fn as_raw(&self) -> SWSSStrRef {
        self.ptr.as_ptr()
    }

    /// Length of the string, not including a null pointer
    pub fn len(&self) -> usize {
        unsafe { SWSSStrRef_length(self.as_raw()).try_into().unwrap() }
    }

    /// Underlying buffer, not including a null pointer
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            let data = SWSSStrRef_c_str(self.as_raw());
            slice::from_raw_parts(data as *const u8, self.len())
        }
    }

    /// Try to convert the C++ string to a rust `&str` without copying. This can only be done if the
    /// string contains valid UTF-8. See [std::str::from_utf8].
    pub fn to_str(&self) -> Result<&str, Utf8Error> {
        str::from_utf8(self.as_bytes())
    }

    /// Convert the C++ string to a [Cow::Borrowed] if the string contains valid UTF-8, the same as
    /// [Self::to_str]. Otherwise, make a [Cow::Owned] copy of the string and replace invalid UTF-8
    /// with replacement bytes. See [String::from_utf8_lossy]. Mainly intended for debugging.
    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.as_bytes())
    }
}

impl ToOwned for CxxStr {
    type Owned = CxxString;

    fn to_owned(&self) -> Self::Owned {
        CxxString::new(self.as_bytes())
    }
}

impl Debug for CxxStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_string_lossy())
    }
}

impl Ord for CxxStr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl Hash for CxxStr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl PartialEq for CxxStr {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes().eq(other.as_bytes())
    }
}

impl PartialEq<CxxString> for CxxStr {
    fn eq(&self, other: &CxxString) -> bool {
        self.as_bytes().eq(other.as_bytes())
    }
}

impl PartialEq<str> for CxxStr {
    fn eq(&self, other: &str) -> bool {
        self.as_bytes().eq(other.as_bytes())
    }
}

impl PartialEq<&str> for CxxStr {
    fn eq(&self, other: &&str) -> bool {
        self.eq(*other)
    }
}

impl PartialEq<String> for CxxStr {
    fn eq(&self, other: &String) -> bool {
        self.eq(other.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SelectResult {
    /// Data is now available.
    Data,
    /// Waiting was interrupted by a signal.
    Signal,
    /// Timed out.
    Timeout,
}

impl SelectResult {
    pub(crate) fn from_raw(raw: SWSSSelectResult) -> Self {
        if raw == SWSSSelectResult_SWSSSelectResult_DATA {
            SelectResult::Data
        } else if raw == SWSSSelectResult_SWSSSelectResult_SIGNAL {
            SelectResult::Signal
        } else if raw == SWSSSelectResult_SWSSSelectResult_TIMEOUT {
            SelectResult::Timeout
        } else {
            panic!("unhandled SWSSSelectResult: {raw}")
        }
    }
}

/// Type of the `operation` field in [KeyOpFieldValues].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyOperation {
    Set,
    Del,
}

impl FromStr for KeyOperation {
    type Err = InvalidKeyOperationString;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "SET" => Ok(Self::Set),
            "DEL" => Ok(Self::Del),
            _ => Err(InvalidKeyOperationString(s.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct InvalidKeyOperationString(String);

impl Display for InvalidKeyOperationString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, r#"A KeyOperation String must be "SET" or "DEL", but was {}"#, self.0)
    }
}

impl Error for InvalidKeyOperationString {}

/// Rust version of `swss::KeyOpFieldsValuesTuple`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyOpFieldValues {
    pub key: String,
    pub operation: KeyOperation,
    pub field_values: HashMap<String, CxxString>,
}

/// Intended for testing, ordered by key.
impl PartialOrd for KeyOpFieldValues {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Intended for testing, ordered by key.
impl Ord for KeyOpFieldValues {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

pub(crate) unsafe fn take_field_value_array(arr: SWSSFieldValueArray) -> HashMap<String, CxxString> {
    let mut out = HashMap::with_capacity(arr.len as usize);
    if !arr.data.is_null() {
        let entries = slice::from_raw_parts_mut(arr.data, arr.len as usize);
        for fv in entries {
            let field = str(fv.field);
            let value = CxxString::take_raw(&mut fv.value).unwrap();
            out.insert(field, value);
        }
        SWSSFieldValueArray_free(arr);
    }
    out
}

pub(crate) fn take_key_operation(op: SWSSKeyOperation) -> KeyOperation {
    if op == SWSSKeyOperation_SWSSKeyOperation_SET {
        KeyOperation::Set
    } else if op == SWSSKeyOperation_SWSSKeyOperation_DEL {
        KeyOperation::Del
    } else {
        unreachable!("Invalid SWSSKeyOperation");
    }
}

pub(crate) unsafe fn take_key_op_field_values_array(kfvs: SWSSKeyOpFieldValuesArray) -> Vec<KeyOpFieldValues> {
    let mut out = Vec::with_capacity(kfvs.len as usize);
    if !kfvs.data.is_null() {
        unsafe {
            let entries = slice::from_raw_parts_mut(kfvs.data, kfvs.len as usize);
            for kfv in entries {
                let key = str(kfv.key);
                let operation = take_key_operation(kfv.operation);
                let field_values = take_field_value_array(kfv.fieldValues);
                out.push(KeyOpFieldValues {
                    key,
                    operation,
                    field_values,
                });
            }
            SWSSKeyOpFieldValuesArray_free(kfvs);
        };
    }
    out
}

pub(crate) fn make_field_value_array<I, F, V>(fvs: I) -> (SWSSFieldValueArray, KeepAlive)
where
    I: IntoIterator<Item = (F, V)>,
    F: AsRef<[u8]>,
    V: Into<CxxString>,
{
    let mut k = KeepAlive::default();
    let mut data = Vec::new();

    for (field, value) in fvs {
        let field = cstr(field);
        let value = value.into();
        data.push(SWSSFieldValueTuple {
            field: field.as_ptr(),
            value: value.as_raw(),
        });
        k.keep((field, value));
    }

    let arr = SWSSFieldValueArray {
        data: data.as_mut_ptr(),
        len: data.len().try_into().unwrap(),
    };
    k.keep(data);

    (arr, k)
}

pub(crate) fn make_key_operation(op: KeyOperation) -> SWSSKeyOperation {
    match op {
        KeyOperation::Set => SWSSKeyOperation_SWSSKeyOperation_SET,
        KeyOperation::Del => SWSSKeyOperation_SWSSKeyOperation_DEL,
    }
}

pub(crate) fn make_key_op_field_values_array<'a, I>(kfvs: I) -> (SWSSKeyOpFieldValuesArray, KeepAlive)
where
    I: IntoIterator<Item = KeyOpFieldValues>,
{
    let mut k = KeepAlive::default();
    let mut data = Vec::new();

    for kfv in kfvs {
        let key = cstr(kfv.key);
        let operation = make_key_operation(kfv.operation);
        let (field_values, arr_k) = make_field_value_array(kfv.field_values);
        data.push(SWSSKeyOpFieldValues {
            key: key.as_ptr(),
            operation,
            fieldValues: field_values,
        });
        k.keep(Box::new((key, arr_k)))
    }

    let arr = SWSSKeyOpFieldValuesArray {
        data: data.as_mut_ptr(),
        len: data.len().try_into().unwrap(),
    };
    k.keep(Box::new(data));

    (arr, k)
}

/// Helper struct to keep rust-owned data alive while it is in use by C++
#[derive(Default)]
pub(crate) struct KeepAlive(Vec<Box<dyn Any>>);

impl KeepAlive {
    fn keep<T: Any>(&mut self, t: T) {
        self.0.push(Box::new(t))
    }
}
