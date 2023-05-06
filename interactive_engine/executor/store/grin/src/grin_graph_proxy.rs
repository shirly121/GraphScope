use crate::grin_v6d::*;
use crate::native_utils::*;
use dyn_type::Object;
use graph_proxy::{GraphProxyError, GraphProxyResult};
use std::collections::HashMap;
use std::ffi::{c_char, CStr, CString};

pub struct GrinVertexProxy {
    graph: GrinGraph,
    vertex: GrinVertex,
    prop_table: MutGrinHandle,
    vertex_id: i64,
    vertex_type: GrinVertexType,
    vertex_type_id: GrinVertexTypeId,
}

impl Drop for GrinVertexProxy {
    fn drop(&mut self) {
        unsafe {
            grin_destroy_vertex_type(self.graph, self.vertex_type);
            grin_destroy_vertex(self.graph, self.vertex);
            grin_destroy_vertex_property_table(self.graph, self.prop_table);
        }
    }
}

impl GrinVertexProxy {
    // type PI = FFIPropertiesIter;

    pub fn new(graph: GrinGraph, vertex: GrinVertex) -> Self {
        unsafe {
            let vertex_type = grin_get_vertex_type(graph, vertex);

            // A vertex must have a type
            assert!(!vertex_type.is_null());

            let prop_table = grin_get_vertex_property_table_by_type(graph, vertex_type);

            let vertex_type_id = grin_get_vertex_type_id(graph, vertex_type);
            let vid_handle = grin_get_vertex_original_id(graph, vertex);

            // A vertex must have an id
            assert!(!vid_handle.is_null());

            let vertex_id = grin_get_int64(vid_handle);
            grin_destroy_vertex_original_id(graph, vid_handle);

            GrinVertexProxy { graph, vertex, prop_table, vertex_id, vertex_type, vertex_type_id }
        }
    }

    pub fn get_id(&self) -> i64 {
        self.vertex_id
    }

    pub fn get_label_id(&self) -> GrinVertexTypeId {
        self.vertex_type_id
    }

    pub fn get_property(&self, prop_id: GrinVertexPropertyId) -> GraphProxyResult<Object> {
        unsafe {
            if self.prop_table.is_null() {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_vertex_property_table_by_type`: {:?} returns null",
                    self.vertex_type_id
                )));
            }
            let prop_handle = grin_get_vertex_property_from_id(self.graph, self.vertex_type, prop_id);
            if prop_handle.is_null() {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_vertex_property_from_id`: {:?} returns null",
                    prop_id
                )));
            }

            let prop_type = grin_get_vertex_property_data_type(self.graph, prop_handle);
            let prop_value = grin_get_value_from_vertex_property_table(
                self.graph,
                self.prop_table,
                self.vertex,
                prop_handle,
            );

            let result = grin_data_to_object(self.graph, prop_value, prop_type);
            grin_destroy_value(self.graph, prop_type, prop_value);
            grin_destroy_vertex_property(self.graph, prop_handle);

            result
        }
    }

    pub fn get_properties(&self) -> GraphProxyResult<HashMap<String, Object>> {
        unsafe {
            if self.prop_table.is_null() {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_vertex_property_table_by_type`: {:?} returns null",
                    self.vertex_type_id
                )));
            }
            let prop_list = grin_get_vertex_property_list_by_type(self.graph, self.vertex_type);
            if prop_list.is_null() {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_vertex_property_list_by_type`: {:?} returns null",
                    self.vertex_type_id
                )));
            }
            let prop_list_size = grin_get_vertex_property_list_size(self.graph, prop_list);
            let mut result = HashMap::with_capacity(prop_list_size);

            for i in 0..prop_list_size {
                let prop_handle = grin_get_vertex_property_from_list(self.graph, prop_list, i);
                if prop_handle.is_null() {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_vertex_property_from_list`: {:?} returns null",
                        i
                    )));
                }
                let prop_name = grin_get_vertex_property_name(self.graph, prop_handle);
                let prop_type = grin_get_vertex_property_data_type(self.graph, prop_handle);
                let prop_c_value = grin_get_value_from_vertex_property_table(
                    self.graph,
                    self.prop_table,
                    self.vertex,
                    prop_handle,
                );
                let prop_value = grin_data_to_object(self.graph, prop_c_value, prop_type)?;

                grin_destroy_value(self.graph, prop_type, prop_c_value);
                grin_destroy_vertex_property(self.graph, prop_handle);

                result.insert(string_c2rust(prop_name), prop_value);
            }

            grin_destroy_vertex_property_list(self.graph, prop_list);

            Ok(result)
        }
    }
}

pub struct GrinGraphProxy {
    graph: GrinGraph,
}

impl Drop for GrinGraphProxy {
    fn drop(&mut self) {
        unsafe {
            grin_destroy_graph(self.graph);
        }
    }
}

impl GrinGraphProxy {
    pub fn get_graph(&self) -> GrinGraph {
        self.graph
    }
}
