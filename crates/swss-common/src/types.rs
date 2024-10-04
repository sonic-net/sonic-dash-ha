mod class;
mod pod;

use std::ffi::{CStr, CString};

pub use class::*;
pub use pod::*;

pub(crate) fn cstr(s: impl AsRef<[u8]>) -> CString {
    CString::new(s.as_ref()).unwrap()
}

pub(crate) unsafe fn str(p: *const i8) -> String {
    CStr::from_ptr(p).to_str().unwrap().to_string()
}
