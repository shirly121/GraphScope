//
//! Copyright 2023 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicPtr, Ordering};

use ahash::HashMap;
use dyn_type::Object;
use ir_common::NameOrId;
use pegasus_common::downcast::AsAny;
use pegasus_common::impl_as_any;

use crate::adapters::grin_store::read_graph::{GrinEdgeProxy, GrinVertexProxy};
use crate::apis::{Details, PropertyValue};

/// LazyVertexDetails is used for local property fetching optimization.
/// That is, the required properties will not be materialized until LazyVertexDetails need to be shuffled.
#[allow(dead_code)]
pub struct LazyVertexDetails {
    // prop_keys specify the properties we would save for later queries after shuffle,
    // excluding the ones used only when local property fetching.
    // Specifically, Some(vec![]) indicates we need all properties
    // and None indicates we do not need any property
    prop_keys: Option<Vec<NameOrId>>,
    inner: AtomicPtr<GrinVertexProxy>,
}

impl_as_any!(LazyVertexDetails);

impl LazyVertexDetails {
    pub fn new(v: GrinVertexProxy, prop_keys: Option<Vec<NameOrId>>) -> Self {
        let ptr = Box::into_raw(Box::new(v));
        LazyVertexDetails { prop_keys, inner: AtomicPtr::new(ptr) }
    }

    fn get_vertex_ptr(&self) -> Option<*mut GrinVertexProxy> {
        let ptr = self.inner.load(Ordering::SeqCst);
        if ptr.is_null() {
            None
        } else {
            Some(ptr)
        }
    }
}

impl fmt::Debug for LazyVertexDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyVertexDetails")
            .field("properties", &self.prop_keys)
            .field("inner", &self.inner)
            .finish()
    }
}

impl Details for LazyVertexDetails {
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        if let Some(ptr) = self.get_vertex_ptr() {
            unsafe {
                if let Ok(prop_value) = (*ptr).get_property(key) {
                    Some(PropertyValue::Owned(prop_value))
                } else {
                    None
                }
            }
        } else {
            None
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        if let Some(ptr) = self.get_vertex_ptr() {
            unsafe {
                if let Ok(prop_values) = (*ptr).get_properties() {
                    Some(prop_values)
                } else {
                    None
                }
            }
        } else {
            None
        }
    }

    fn get_property_keys(&self) -> Option<Vec<NameOrId>> {
        self.prop_keys.clone()
    }
}

impl Drop for LazyVertexDetails {
    fn drop(&mut self) {
        let ptr = self.inner.load(Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                std::ptr::drop_in_place(ptr);
            }
        }
    }
}

/// LazyVertexDetails is used for local property fetching optimization.
/// That is, the required properties will not be materialized until LazyVertexDetails need to be shuffled.
#[allow(dead_code)]
pub struct LazyEdgeDetails {
    // prop_keys specify the properties we would save for later queries after shuffle,
    // excluding the ones used only when local property fetching.
    // Specifically, Some(vec![]) indicates we need all properties
    // and None indicates we do not need any property
    prop_keys: Option<Vec<NameOrId>>,
    inner: AtomicPtr<GrinEdgeProxy>,
}

impl_as_any!(LazyEdgeDetails);

impl LazyEdgeDetails {
    pub fn new(v: GrinEdgeProxy, prop_keys: Option<Vec<NameOrId>>) -> Self {
        let ptr = Box::into_raw(Box::new(v));
        LazyEdgeDetails { prop_keys, inner: AtomicPtr::new(ptr) }
    }

    fn get_edge_ptr(&self) -> Option<*mut GrinEdgeProxy> {
        let ptr = self.inner.load(Ordering::SeqCst);
        if ptr.is_null() {
            None
        } else {
            Some(ptr)
        }
    }
}

impl fmt::Debug for LazyEdgeDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyEdgeDetails")
            .field("properties", &self.prop_keys)
            .field("inner", &self.inner)
            .finish()
    }
}

impl Details for LazyEdgeDetails {
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        if let Some(ptr) = self.get_edge_ptr() {
            unsafe {
                if let Ok(prop_value) = (*ptr).get_property(key) {
                    Some(PropertyValue::Owned(prop_value))
                } else {
                    None
                }
            }
        } else {
            None
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        if let Some(ptr) = self.get_edge_ptr() {
            unsafe {
                if let Ok(prop_values) = (*ptr).get_properties() {
                    Some(prop_values)
                } else {
                    None
                }
            }
        } else {
            None
        }
    }

    fn get_property_keys(&self) -> Option<Vec<NameOrId>> {
        self.prop_keys.clone()
    }
}

impl Drop for LazyEdgeDetails {
    fn drop(&mut self) {
        let ptr = self.inner.load(Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                std::ptr::drop_in_place(ptr);
            }
        }
    }
}
