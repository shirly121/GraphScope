use dyn_type::Object;
use graph_proxy::{GraphProxyError, GraphProxyResult};
use std::ffi::{c_char, c_void, CStr, CString};

use crate::grin_v6d::*;

pub type CStrPtr = *const c_char;
pub type GrinHandle = *const c_void;
pub type MutGrinHandle = *mut c_void;

pub unsafe fn string_rust2c(rust_str: &str) -> CStrPtr {
    let c_str = CString::new(rust_str).unwrap();
    let ptr = c_str.as_ptr() as *const c_char;
    std::mem::forget(c_str);

    ptr
}

pub unsafe fn string_c2rust(c_str: *const c_char) -> String {
    // Convert the `*const c_char` to a Rust `String`
    unsafe {
        let c_str = CStr::from_ptr(c_str);
        c_str.to_str().unwrap().to_owned()
    }
}

pub unsafe fn grin_data_to_object(
    graph_handle: GrinGraph, grin_data: GrinHandle, grin_data_type: GrinDatatype,
) -> GraphProxyResult<Object> {
    if !grin_data.is_null() {
        let result = if grin_data_type == GRIN_DATATYPE_INT32 {
            Ok(grin_get_int32(grin_data).into())
        } else if grin_data_type == GRIN_DATATYPE_INT64 {
            Ok(grin_get_int64(grin_data).into())
        } else if grin_data_type == GRIN_DATATYPE_UINT32 {
            Ok((grin_get_uint32(grin_data) as u64).into())
        } else if grin_data_type == GRIN_DATATYPE_UINT64 {
            Ok(grin_get_uint64(grin_data).into())
        } else if grin_data_type == GRIN_DATATYPE_FLOAT {
            Ok((grin_get_float(grin_data) as f64).into())
        } else if grin_data_type == GRIN_DATATYPE_DOUBLE {
            Ok(grin_get_double(grin_data).into())
        } else if grin_data_type == GRIN_DATATYPE_STRING {
            Ok(string_c2rust(grin_get_string(grin_data)).into())
        } else if grin_data_type == GRIN_DATATYPE_UNDEFINED {
            Err(GraphProxyError::QueryStoreError("`grin_data_type is undefined`".to_string()))
        } else {
            Err(GraphProxyError::UnSupported(format!("Unsupported grin data type: {:?}", grin_data_type)))
        };

        result
    } else {
        Err(GraphProxyError::QueryStoreError("`grin_data is null`".to_string()))
    }
}
