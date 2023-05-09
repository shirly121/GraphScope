use std::ffi::{c_char, CStr, CString};
use std::fmt;

use ahash::{HashMap, HashMapExt};
use dyn_type::Object;
use graph_proxy::apis::graph::ID;
use graph_proxy::apis::{DynDetails, Vertex as RuntimeVertex};
use graph_proxy::{GraphProxyError, GraphProxyResult};
use ir_common::{LabelId, NameOrId};

use crate::grin_details::LazyVertexDetails;
use crate::grin_v6d::*;
use crate::native_utils::*;

#[inline]
fn get_vertex_propoerty_as_object(
    graph: GrinGraph, vertex: GrinVertex, prop_table: GrinVertexPropertyTable,
    prop_handle: GrinVertexProperty,
) -> GraphProxyResult<Object> {
    unsafe {
        let prop_type = grin_get_vertex_property_data_type(graph, prop_handle);
        let result = if prop_type == GRIN_DATATYPE_INT32 {
            Ok(grin_get_int32_from_vertex_property_table(graph, prop_table, vertex, prop_handle).into())
        } else if prop_type == GRIN_DATATYPE_INT64 {
            Ok(grin_get_int64_from_vertex_property_table(graph, prop_table, vertex, prop_handle).into())
        } else if prop_type == GRIN_DATATYPE_UINT32 {
            Ok((grin_get_uint32_from_vertex_property_table(graph, prop_table, vertex, prop_handle) as u64)
                .into())
        } else if prop_type == GRIN_DATATYPE_UINT64 {
            Ok(grin_get_uint64_from_vertex_property_table(graph, prop_table, vertex, prop_handle).into())
        } else if prop_type == GRIN_DATATYPE_FLOAT {
            Ok((grin_get_float_from_vertex_property_table(graph, prop_table, vertex, prop_handle) as f64)
                .into())
        } else if prop_type == GRIN_DATATYPE_DOUBLE {
            Ok(grin_get_double_from_vertex_property_table(graph, prop_table, vertex, prop_handle).into())
        } else if prop_type == GRIN_DATATYPE_STRING {
            let c_str =
                grin_get_string_from_vertex_property_table(graph, prop_table, vertex, prop_handle).into();
            let rust_str = string_c2rust(c_str);
            grin_destroy_name(graph, c_str);
            Ok(Object::String(rust_str))
        } else if prop_type == GRIN_DATATYPE_UNDEFINED {
            Err(GraphProxyError::QueryStoreError("`grin_data_type is undefined`".to_string()))
        } else {
            Err(GraphProxyError::UnSupported(format!("Unsupported grin data type: {:?}", prop_type)))
        };

        result
    }
}

#[inline]
fn get_edge_property_as_object(
    graph: GrinGraph, edge: GrinEdge, prop_table: GrinEdgePropertyTable, prop_handle: GrinEdgeProperty,
) -> GraphProxyResult<Object> {
    unsafe {
        let prop_type = grin_get_edge_property_data_type(graph, prop_handle);
        unsafe {
            let prop_type = grin_get_edge_property_data_type(graph, prop_handle);
            let result = if prop_type == GRIN_DATATYPE_INT32 {
                Ok(grin_get_int32_from_edge_property_table(graph, prop_table, edge, prop_handle).into())
            } else if prop_type == GRIN_DATATYPE_INT64 {
                Ok(grin_get_int64_from_edge_property_table(graph, prop_table, edge, prop_handle).into())
            } else if prop_type == GRIN_DATATYPE_UINT32 {
                Ok((grin_get_uint32_from_edge_property_table(graph, prop_table, edge, prop_handle) as u64)
                    .into())
            } else if prop_type == GRIN_DATATYPE_UINT64 {
                Ok(grin_get_uint64_from_edge_property_table(graph, prop_table, edge, prop_handle).into())
            } else if prop_type == GRIN_DATATYPE_FLOAT {
                Ok((grin_get_float_from_edge_property_table(graph, prop_table, edge, prop_handle) as f64)
                    .into())
            } else if prop_type == GRIN_DATATYPE_DOUBLE {
                Ok(grin_get_double_from_edge_property_table(graph, prop_table, edge, prop_handle).into())
            } else if prop_type == GRIN_DATATYPE_STRING {
                let c_str =
                    grin_get_string_from_edge_property_table(graph, prop_table, edge, prop_handle).into();
                let rust_str = string_c2rust(c_str);
                grin_destroy_name(graph, c_str);
                Ok(Object::String(rust_str))
            } else if prop_type == GRIN_DATATYPE_UNDEFINED {
                Err(GraphProxyError::QueryStoreError("`grin_data_type is undefined`".to_string()))
            } else {
                Err(GraphProxyError::UnSupported(format!("Unsupported grin data type: {:?}", prop_type)))
            };

            result
        }
    }
}

pub struct GrinVertexProxy {
    graph: GrinGraph,
    vertex: GrinVertex,
    vertex_type: GrinVertexType,
    prop_table: GrinVertexPropertyTable,
    vertex_id: i64,
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

impl fmt::Debug for GrinVertexProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrinVertexProxy")
            // .field("graph", &self.graph)
            // .field("vertex", &self.vertex)
            // .field("vertex_type", &self.vertex_type)
            // .field("prop_table", &self.prop_table)
            .field("vertex_id", &self.vertex_id)
            .field("vertex_type_id", &self.vertex_type_id)
            .finish()
    }
}

impl GrinVertexProxy {
    // type PI = FFIPropertiesIter;

    pub fn new(graph: GrinGraph, vertex: GrinVertex) -> GraphProxyResult<Self> {
        unsafe {
            let vertex_type = grin_get_vertex_type(graph, vertex);
            // A vertex must have a type
            assert!(vertex_type != GRIN_NULL_VERTEX_TYPE);
            let vertex_type_id = grin_get_vertex_type_id(graph, vertex_type);

            let prop_table = grin_get_vertex_property_table_by_type(graph, vertex_type);
            if prop_table.is_null() {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_vertex_property_table_by_type`: {:?} returns null",
                    vertex_type_id
                )));
            }

            let vertex_ref = grin_get_vertex_ref_for_vertex(graph, vertex);
            if vertex_ref == GRIN_NULL_VERTEX_REF {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_vertex_ref_for_vertex` return null"
                )));
            }
            let vertex_id = grin_serialize_vertex_ref_as_int64(graph, vertex_ref);
            grin_destroy_vertex_ref(graph, vertex_ref);

            Ok(GrinVertexProxy { graph, vertex, vertex_type, prop_table, vertex_id, vertex_type_id })
        }
    }

    pub fn get_id(&self) -> i64 {
        self.vertex_id
    }

    pub fn get_label_id(&self) -> GrinVertexTypeId {
        self.vertex_type_id
    }

    pub fn get_property(&self, prop_key: &NameOrId) -> GraphProxyResult<Object> {
        unsafe {
            let prop_handle = match prop_key {
                NameOrId::Id(prop_id) => grin_get_vertex_property_from_id(
                    self.graph,
                    self.vertex_type,
                    *prop_id as GrinVertexPropertyId,
                ),
                NameOrId::Str(prop_str) => {
                    grin_get_vertex_property_by_name(self.graph, self.vertex_type, string_rust2c(prop_str))
                }
            };

            if prop_handle == GRIN_NULL_VERTEX_PROPERTY {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_vertex_property`: {:?} returns null",
                    prop_key
                )));
            }
            let result =
                get_vertex_propoerty_as_object(self.graph, self.vertex, self.prop_table, prop_handle);

            result
        }
    }

    pub fn get_properties(&self) -> GraphProxyResult<HashMap<NameOrId, Object>> {
        unsafe {
            let prop_list = grin_get_vertex_property_list_by_type(self.graph, self.vertex_type);
            if prop_list.is_null() {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_vertex_property_list_by_type`: {:?} returns null",
                    self.vertex_type_id
                )));
            }
            let prop_list_size = grin_get_vertex_property_list_size(self.graph, prop_list);
            let mut result = HashMap::with_capacity(prop_list_size as usize);

            for i in 0..prop_list_size {
                let prop_handle = grin_get_vertex_property_from_list(self.graph, prop_list, i);
                if prop_handle == GRIN_NULL_VERTEX_PROPERTY {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_vertex_property_from_list`: {:?} returns null",
                        i
                    )));
                }
                let prop_id = grin_get_vertex_property_id(self.graph, self.vertex_type, prop_handle);

                let prop_value =
                    get_vertex_propoerty_as_object(self.graph, self.vertex, self.prop_table, prop_handle)?;
                result.insert(NameOrId::Id(prop_id as i32), prop_value);

                grin_destroy_vertex_property(self.graph, prop_handle);
            }

            grin_destroy_vertex_property_list(self.graph, prop_list);

            Ok(result)
        }
    }

    #[inline]
    pub fn into_runtime_vertex(self, prop_keys: Option<Vec<NameOrId>>) -> RuntimeVertex {
        let id = self.vertex_id as ID;
        let label = Some(self.vertex_type_id as LabelId);
        let details = LazyVertexDetails::new(self, prop_keys);
        RuntimeVertex::new(id, label, DynDetails::lazy(details))
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
