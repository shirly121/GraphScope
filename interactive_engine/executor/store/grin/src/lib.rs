pub mod grin_v6d;

use std::ffi::{c_char, c_void, CStr, CString};

pub type CStrPtr = *const c_char;
pub type GrinHandle = *const c_void;
pub type MutGrinHandle = *mut c_void;

#[inline]
pub unsafe fn string_rust2c(rust_str: &str) -> CStrPtr {
    let c_str = CString::new(rust_str).unwrap();
    let ptr = c_str.as_ptr() as *const c_char;
    std::mem::forget(c_str);

    ptr
}

#[inline]
pub unsafe fn string_c2rust(c_str: *const c_char) -> String {
    // Convert the `*const c_char` to a Rust `String`
    unsafe {
        let c_str = CStr::from_ptr(c_str);
        c_str.to_str().unwrap().to_owned()
    }
}
