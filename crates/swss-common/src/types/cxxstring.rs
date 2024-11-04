use crate::*;
use std::{
    borrow::{Borrow, Cow},
    fmt::Debug,
    hash::Hash,
    mem,
    ops::Deref,
    ptr::{self, NonNull},
    slice,
    str::Utf8Error,
};

/// A C++ `std::string` that can be moved around and accessed from Rust.
#[repr(transparent)]
#[derive(Eq)]
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

    /// Borrows a `CxxStr` from this string.
    ///
    /// Like `String::as_str`, this method is unnecessary where deref coercion can be used.
    pub fn as_cxx_str(&self) -> &CxxStr {
        self
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

/// This calls [CxxStr::to_string_lossy] which may clone the underlying data (i.e. may be expensive).
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

impl PartialOrd for CxxString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
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

/// Equivalent of a C++ `std::string&`, which can be borrowed from a [CxxString].
///
/// `CxxStr` has the same conceptual relationship with `CxxString` as a Rust `&str` does with `String`.
#[repr(transparent)]
#[derive(Eq)]
pub struct CxxStr {
    ptr: NonNull<SWSSStrRefOpaque>,
}

impl CxxStr {
    pub(crate) fn as_raw(&self) -> SWSSStrRef {
        self.ptr.as_ptr()
    }

    /// Length of the string, not including a null terminator.
    pub fn len(&self) -> usize {
        unsafe { SWSSStrRef_length(self.as_raw()).try_into().unwrap() }
    }

    /// Returns `true` if `self` has a length of zero bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The underlying bytes of the string, not including a null terminator.
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            let data = SWSSStrRef_c_str(self.as_raw());
            slice::from_raw_parts(data as *const u8, self.len())
        }
    }

    /// Tries to convert the C++ string to a Rust `&str` without copying. This can only be done if
    /// the string contains valid UTF-8. See [std::str::from_utf8].
    pub fn to_str(&self) -> Result<&str, Utf8Error> {
        std::str::from_utf8(self.as_bytes())
    }

    /// Converts the C++ string to a Rust `&str` or `String`. If the string is valid UTF-8, the
    /// result is a `&str` pointing to the original data. Otherwise, the result is a `String` with
    /// a copy of the data, but with invalid UTF-8 replaced. See [String::from_utf8_lossy].
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

impl PartialOrd for CxxStr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
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
