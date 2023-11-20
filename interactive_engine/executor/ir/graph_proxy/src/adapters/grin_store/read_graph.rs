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

use std::fmt;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use dyn_type::{Object, Primitives};
use grin::grin::*;
use grin::{string_c2rust, string_rust2c};
use ir_common::{LabelId, NameOrId, OneOrMany};

use crate::adapters::grin_store::details::{LazyEdgeDetails, LazyVertexDetails};
use crate::apis::graph::{ID, PKV};
use crate::apis::partitioner::PartitionId;
use crate::apis::{
    from_fn, ClusterInfo, Direction, DynDetails, Edge as RuntimeEdge, QueryParams, ReadGraph, Statement,
    Vertex as RuntimeVertex,
};
use crate::{filter_limit, filter_sample_limit, limit_n, sample_limit};
use crate::{GraphProxyError, GraphProxyResult};

#[allow(dead_code)]
pub fn create_grin_store(
    store: Arc<GrinGraphProxy>, server_partitions: Vec<PartitionId>, cluster_info: Arc<dyn ClusterInfo>,
) -> Arc<GrinGraphRuntime> {
    let graph = GrinGraphRuntime::new(store, server_partitions, cluster_info);
    Arc::new(graph)
}

/// Get value of certain type of vertex property from the grin-enabled graph, and turn it into
/// `Object` accessible to the runtime.
#[inline]
fn get_vertex_property_as_object(
    graph: GrinGraph, vertex: GrinVertex, prop_handle: GrinVertexProperty,
) -> GraphProxyResult<Object> {
    unsafe {
        let prop_type = grin_get_vertex_property_datatype(graph, prop_handle);
        let result = if prop_type == GRIN_DATATYPE_INT32 {
            Ok(grin_get_vertex_property_value_of_int32(graph, vertex, prop_handle).into())
        } else if prop_type == GRIN_DATATYPE_INT64 {
            Ok(grin_get_vertex_property_value_of_int64(graph, vertex, prop_handle).into())
        } else if prop_type == GRIN_DATATYPE_UINT32 {
            Ok((grin_get_vertex_property_value_of_uint32(graph, vertex, prop_handle) as u64).into())
        } else if prop_type == GRIN_DATATYPE_UINT64 {
            Ok(grin_get_vertex_property_value_of_uint64(graph, vertex, prop_handle).into())
        } else if prop_type == GRIN_DATATYPE_FLOAT {
            Ok((grin_get_vertex_property_value_of_float(graph, vertex, prop_handle) as f64).into())
        } else if prop_type == GRIN_DATATYPE_DOUBLE {
            Ok(grin_get_vertex_property_value_of_double(graph, vertex, prop_handle).into())
        } else if prop_type == GRIN_DATATYPE_STRING {
            let c_str = grin_get_vertex_property_value_of_string(graph, vertex, prop_handle).into();
            let rust_str = string_c2rust(c_str);
            grin_destroy_vertex_property_value_of_string(graph, c_str);
            Ok(Object::String(rust_str))
        } else if prop_type == GRIN_DATATYPE_UNDEFINED {
            Err(GraphProxyError::QueryStoreError("`grin_data_type is undefined`".to_string()))
        } else {
            Err(GraphProxyError::UnSupported(format!("Unsupported grin data type: {:?}", prop_type)))
        };

        result
    }
}

/// Get value of certain type of edge property from the grin-enabled graph, and turn it into
/// `Object` accessible to the runtime.
#[inline]
fn get_edge_property_as_object(
    graph: GrinGraph, edge: GrinEdge, prop_handle: GrinEdgeProperty,
) -> GraphProxyResult<Object> {
    unsafe {
        let prop_type = grin_get_edge_property_datatype(graph, prop_handle);
        let result = if prop_type == GRIN_DATATYPE_INT32 {
            Ok(grin_get_edge_property_value_of_int32(graph, edge, prop_handle).into())
        } else if prop_type == GRIN_DATATYPE_INT64 {
            Ok(grin_get_edge_property_value_of_int64(graph, edge, prop_handle).into())
        } else if prop_type == GRIN_DATATYPE_UINT32 {
            Ok((grin_get_edge_property_value_of_uint32(graph, edge, prop_handle) as u64).into())
        } else if prop_type == GRIN_DATATYPE_UINT64 {
            Ok(grin_get_edge_property_value_of_uint64(graph, edge, prop_handle).into())
        } else if prop_type == GRIN_DATATYPE_FLOAT {
            Ok((grin_get_edge_property_value_of_float(graph, edge, prop_handle) as f64).into())
        } else if prop_type == GRIN_DATATYPE_DOUBLE {
            Ok(grin_get_edge_property_value_of_double(graph, edge, prop_handle).into())
        } else if prop_type == GRIN_DATATYPE_STRING {
            let c_str = grin_get_edge_property_value_of_string(graph, edge, prop_handle).into();
            let rust_str = string_c2rust(c_str);
            grin_destroy_vertex_property_value_of_string(graph, c_str);
            Ok(Object::String(rust_str))
        } else if prop_type == GRIN_DATATYPE_UNDEFINED {
            Err(GraphProxyError::QueryStoreError("`grin_data_type is undefined`".to_string()))
        } else {
            Err(GraphProxyError::UnSupported(format!("Unsupported grin data type: {:?}", prop_type)))
        };

        result
    }
}

// TODO(bingqing): should use GrinDatatype to indicate the true type in GRIN
#[inline]
fn build_grin_row_from_object(
    graph: GrinGraph,
    grin_row: GrinRow,
    value: &Object,
    //, prop_type: GrinDatatype,
) -> GraphProxyResult<()> {
    unsafe {
        match value {
            Object::Primitive(Primitives::Byte(_v)) => {
                todo!()
            }
            Object::Primitive(Primitives::Integer(v)) => {
                let succeed = grin_insert_int32_to_row(graph, grin_row, *v);
                if !succeed {
                    return Err(GraphProxyError::QueryStoreError(
                        "`grin_insert_int32_to_row failed`".to_string(),
                    ));
                }
            }
            Object::Primitive(Primitives::Long(v)) => {
                let succeed = grin_insert_int64_to_row(graph, grin_row, *v);
                if !succeed {
                    return Err(GraphProxyError::QueryStoreError(
                        "`grin_insert_int64_to_row failed`".to_string(),
                    ));
                }
            }
            Object::Primitive(Primitives::ULLong(_v)) => {
                todo!()
            }
            Object::Primitive(Primitives::Float(_v)) => {
                todo!()
            }
            Object::String(ref v) => {
                let c_string = string_rust2c(v);
                let succeed = grin_insert_string_to_row(graph, grin_row, c_string);
                if !succeed {
                    return Err(GraphProxyError::QueryStoreError(
                        "`grin_insert_string_to_row failed`".to_string(),
                    ));
                }
            }
            Object::Blob(ref _v) => {
                todo!()
            }
            _ => {
                todo!()
            }
        }
        Ok(())
    }
}

#[inline]
fn get_vertex_type_id(graph: GrinGraph, vertex: GrinVertex) -> GraphProxyResult<GrinVertexTypeId> {
    unsafe {
        let vertex_type = grin_get_vertex_type(graph, vertex);
        if vertex_type == GRIN_NULL_VERTEX_TYPE {
            return Err(GraphProxyError::QueryStoreError(format!(
                "`grin_get_vertex_type`: {:?}, returns null",
                vertex
            )));
        }
        let vertex_type_id = grin_get_vertex_type_id(graph, vertex_type);
        if vertex_type_id == GRIN_NULL_VERTEX_TYPE_ID {
            return Err(GraphProxyError::QueryStoreError(format!(
                "`grin_get_vertex_type_id`: {:?}, returns null",
                vertex
            )));
        }
        grin_destroy_vertex_type(graph, vertex_type);
        Ok(vertex_type_id)
    }
}

#[inline]
fn get_vertex_ref_id(graph: GrinGraph, vertex: GrinVertex) -> GraphProxyResult<i64> {
    unsafe {
        let vertex_ref = grin_get_vertex_ref_by_vertex(graph, vertex);
        if vertex_ref == GRIN_NULL_VERTEX_REF {
            return Err(GraphProxyError::QueryStoreError(format!(
                "`grin_get_vertex_ref_by_vertex`: {:?}, returns null",
                vertex
            )));
        }
        let vertex_id = grin_serialize_vertex_ref_as_int64(graph, vertex_ref);
        grin_destroy_vertex_ref(graph, vertex_ref);

        Ok(vertex_id)
    }
}

/// A proxy for handling a vertex in a grin-enabled store.
pub struct GrinVertexProxy {
    /// The grin graph handle, only local to the current process
    graph: GrinGraph,
    /// The vertex handle, only local to the current process
    vertex: GrinVertex,
    /// The vertex type handle, for the `vertex`
    vertex_type: GrinVertexType,
}

impl Drop for GrinVertexProxy {
    fn drop(&mut self) {
        unsafe {
            // println!("drop vertex...");
            grin_destroy_vertex_type(self.graph, self.vertex_type);
            grin_destroy_vertex(self.graph, self.vertex);
        }
    }
}

unsafe impl Send for GrinVertexProxy {}
unsafe impl Sync for GrinVertexProxy {}

impl fmt::Debug for GrinVertexProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: this is for debugging, will be removed.
        let vertex_id = get_vertex_ref_id(self.graph, self.vertex).unwrap();
        let vertex_type_id = get_vertex_type_id(self.graph, self.vertex).unwrap();
        f.debug_struct("GrinVertexProxy")
            // .field("graph", &self.graph)
            // .field("vertex", &self.vertex)
            // .field("vertex_type", &self.vertex_type)
            .field("vertex_id", &vertex_id)
            .field("vertex_type_id", &vertex_type_id)
            .finish()
    }
}

impl GrinVertexProxy {
    pub fn new(graph: GrinGraph, vertex: GrinVertex) -> Self {
        unsafe {
            let vertex_type = grin_get_vertex_type(graph, vertex);
            // A vertex must have a type
            assert_ne!(vertex_type, GRIN_NULL_VERTEX_TYPE);

            GrinVertexProxy { graph, vertex, vertex_type }
        }
    }

    /// Get a certain property of the vertex
    pub fn get_property(&self, prop_key: &NameOrId) -> GraphProxyResult<Object> {
        unsafe {
            let prop_handle = match prop_key {
                NameOrId::Id(prop_id) => grin_get_vertex_property_by_id(
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
            let result = get_vertex_property_as_object(self.graph, self.vertex, prop_handle);
            grin_destroy_vertex_property(self.graph, prop_handle);

            result
        }
    }

    /// Get all properties of the vertex
    pub fn get_properties(&self) -> GraphProxyResult<HashMap<NameOrId, Object>> {
        unsafe {
            let prop_list = grin_get_vertex_property_list_by_type(self.graph, self.vertex_type);
            if prop_list == GRIN_NULL_VERTEX_PROPERTY_LIST {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_vertex_property_list_by_type`: {:?} returns null",
                    self.vertex_type
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
                let prop_name = grin_get_vertex_property_name(self.graph, self.vertex_type, prop_handle);
                let prop_name_string = string_c2rust(prop_name);
                let prop_value = get_vertex_property_as_object(self.graph, self.vertex, prop_handle)?;
                result.insert(NameOrId::Str(prop_name_string), prop_value);

                grin_destroy_vertex_property(self.graph, prop_handle);
            }

            grin_destroy_vertex_property_list(self.graph, prop_list);

            Ok(result)
        }
    }

    /// Turn the `VertexProxy` into a `RuntimeVertex` processed by GIE's runtime, for lazily
    /// accessing the data of the vertex, including identifier, label, and properties.
    /// The `prop_keys` parameter is used to specify which properties to be loaded lazily.
    #[inline]
    pub fn into_runtime_vertex(self, prop_keys: Option<&Vec<NameOrId>>) -> GraphProxyResult<RuntimeVertex> {
        let id = get_vertex_ref_id(self.graph, self.vertex)?;
        let label = get_vertex_type_id(self.graph, self.vertex)?;
        let details = LazyVertexDetails::new(self, prop_keys.cloned());
        Ok(RuntimeVertex::new(id as ID, Some(label as LabelId), DynDetails::lazy(details)))
    }
}

/// A structure to easily handle scanning various types of vertices in a grin-enabled store.
pub struct GrinVertexIter {
    /// A grin graph handle, only local to the current process
    graph: GrinGraph,
    /// A per-type map of vertex list iterator handles, used iterate vertices of specific type
    vertex_type_iter: HashMap<GrinVertexTypeId, GrinVertexListIterator>,
    /// **All** (exclude `current_vertex_type`) types of vertices to scan
    vertex_type_ids: Vec<GrinVertexTypeId>,
    /// The current vertex type that is under scanning
    current_vertex_type_id: GrinVertexTypeId,
}

impl GrinVertexIter {
    pub fn new(graph: GrinGraph, vertex_type_ids: &Vec<GrinVertexTypeId>) -> GraphProxyResult<Self> {
        unsafe {
            let mut vertex_type_iter = HashMap::new();
            let mut vertex_type_ids = vertex_type_ids.clone();
            for vertex_type_id in &vertex_type_ids {
                let vertex_type = grin_get_vertex_type_by_id(graph, *vertex_type_id);
                if vertex_type == GRIN_NULL_VERTEX_TYPE {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_vertex_type_by_id`: {:?}, returns null",
                        vertex_type_id
                    )));
                }
                let vtype_list = grin_get_vertex_list_by_type_select_master(graph, vertex_type);
                if vtype_list == GRIN_NULL_VERTEX_LIST {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_select_type_for_vertex_list`: {:?}, returns null",
                        vertex_type_id
                    )));
                }
                let vtype_iter = grin_get_vertex_list_begin(graph, vtype_list);
                if vtype_iter == GRIN_NULL_VERTEX_LIST_ITERATOR {
                    return Err(GraphProxyError::QueryStoreError(
                        "`grin_get_vertex_list_begin` returns null".to_string(),
                    ));
                }
                vertex_type_iter.insert(*vertex_type_id, vtype_iter);
                grin_destroy_vertex_list(graph, vtype_list);
                grin_destroy_vertex_type(graph, vertex_type);
            }
            let current_vertex_type_id = vertex_type_ids.pop().unwrap();

            Ok(Self { graph, vertex_type_iter, vertex_type_ids, current_vertex_type_id })
        }
    }

    pub fn get_current_vertex_iter(&self) -> GrinVertexListIterator {
        self.vertex_type_iter[&self.current_vertex_type_id].clone()
    }
}

impl Drop for GrinVertexIter {
    fn drop(&mut self) {
        unsafe {
            println!("drop GrinVertexIter...");
            for (_, iter) in self.vertex_type_iter.drain() {
                grin_destroy_vertex_list_iter(self.graph, iter);
            }
        }
    }
}

impl Iterator for GrinVertexIter {
    type Item = GrinVertexProxy;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let mut current_vertex_iter = self.get_current_vertex_iter();
            while grin_is_vertex_list_end(self.graph, current_vertex_iter) {
                // complete scanning the current type of vertices, switch to the next one
                if let Some(next_vertex_type_id) = self.vertex_type_ids.pop() {
                    self.current_vertex_type_id = next_vertex_type_id;
                    current_vertex_iter = self.get_current_vertex_iter();
                } else {
                    // no more vertex type to scan
                    return None;
                }
            }

            let vertex_handle = grin_get_vertex_from_iter(self.graph, current_vertex_iter);
            grin_get_next_vertex_list_iter(self.graph, current_vertex_iter);
            // a null vertex handle is unacceptable at the current stage
            assert_ne!(vertex_handle, GRIN_NULL_VERTEX);

            Some(GrinVertexProxy::new(self.graph, vertex_handle))
        }
    }
}

unsafe impl Send for GrinVertexIter {}
unsafe impl Sync for GrinVertexIter {}

#[cfg(feature = "grin_enable_edge_list")]
/// A structure to easily handle scanning various types of edges in a grin-enabled store.
pub struct GrinEdgeIter {
    /// A grin graph handle, only local to the current process
    graph: GrinGraph,
    /// A per-type map of edge list iterator handles, used iterate edges of specific type
    edge_type_iter: HashMap<GrinEdgeTypeId, GrinEdgeListIterator>,
    /// **All** (exclude `current_edge_type`) types of edges to scan
    edge_type_ids: Vec<GrinEdgeTypeId>,
    /// The current edge type that is under scanning
    current_edge_type_id: GrinEdgeTypeId,
}

#[cfg(feature = "grin_enable_edge_list")]
impl GrinEdgeIter {
    pub fn new(graph: GrinGraph, edge_type_ids: &Vec<GrinEdgeTypeId>) -> GraphProxyResult<Self> {
        unsafe {
            let mut edge_type_iter = HashMap::new();
            let mut edge_type_ids = edge_type_ids.clone();
            for edge_type_id in &edge_type_ids {
                let edge_type = grin_get_edge_type_by_id(graph, *edge_type_id);
                if edge_type == GRIN_NULL_VERTEX_TYPE {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_vertex_type_by_id`: {:?}, returns null",
                        edge_type_id
                    )));
                }
                let etype_list = grin_get_edge_list_by_type_select_master(graph, edge_type);
                if etype_list == GRIN_NULL_VERTEX_LIST {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_select_type_for_vertex_list`: {:?}, returns null",
                        edge_type_id
                    )));
                }
                let etype_iter = grin_get_edge_list_begin(graph, etype_list);
                if etype_iter == GRIN_NULL_VERTEX_LIST_ITERATOR {
                    return Err(GraphProxyError::QueryStoreError(
                        "`grin_get_vertex_list_begin` returns null".to_string(),
                    ));
                }
                edge_type_iter.insert(*edge_type_id, etype_iter);
                grin_destroy_edge_list(graph, etype_list);
                grin_destroy_edge_type(graph, edge_type);
            }
            let current_edge_type_id = edge_type_ids.pop().unwrap();

            Ok(Self { graph, edge_type_iter, edge_type_ids, current_edge_type_id: current_edge_type_id })
        }
    }

    pub fn get_current_edge_iter(&self) -> GrinEdgeListIterator {
        self.edge_type_iter[&self.current_edge_type_id].clone()
    }
}

#[cfg(feature = "grin_enable_edge_list")]
impl Drop for GrinEdgeIter {
    fn drop(&mut self) {
        unsafe {
            println!("drop GrinEdgeIter...");
            for (_, iter) in self.edge_type_iter.drain() {
                grin_destroy_edge_list_iter(self.graph, iter);
            }
        }
    }
}

#[cfg(feature = "grin_enable_edge_list")]
impl Iterator for GrinEdgeIter {
    type Item = GrinEdgeProxy;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let mut current_edge_iter = self.get_current_edge_iter();
            while grin_is_edge_list_end(self.graph, current_edge_iter) {
                // complete scanning the current type of edges, switch to the next one
                if let Some(next_edge_type_id) = self.edge_type_ids.pop() {
                    self.current_edge_type_id = next_edge_type_id;
                    current_edge_iter = self.get_current_edge_iter();
                } else {
                    // no more edge type to scan
                    return None;
                }
            }

            let edge_handle = grin_get_edge_from_iter(self.graph, current_edge_iter);
            grin_get_next_edge_list_iter(self.graph, current_edge_iter);
            // a null edge handle is unacceptable at the current stage
            assert_ne!(edge_handle, GRIN_NULL_VERTEX);

            Some(GrinEdgeProxy::new(self.graph, edge_handle))
        }
    }
}

#[cfg(feature = "grin_enable_edge_list")]
unsafe impl Send for GrinEdgeIter {}
#[cfg(feature = "grin_enable_edge_list")]
unsafe impl Sync for GrinEdgeIter {}

/// A proxy for handling an id-only-vertex (the vertex with no properties) in a grin-enabled store.
/// TODO: may not necessary. can directly build RuntimeVertex instead of GrinIdVertexProxy
pub struct GrinIdVertexProxy {
    /// The decoded identifier of the `vertex`, currently obtained via `grin_get_vertex_ref_by_vertex()`
    vertex_id: i64,
    /// The vertex type identifier of the `vertex`
    vertex_type_id: GrinVertexTypeId,
}

impl GrinIdVertexProxy {
    pub fn new(graph: GrinGraph, vertex: GrinVertex) -> GraphProxyResult<Self> {
        let vertex_id = get_vertex_ref_id(graph, vertex)?;
        let vertex_type_id = get_vertex_type_id(graph, vertex)?;
        unsafe { grin_destroy_vertex(graph, vertex) };
        Ok(Self { vertex_id, vertex_type_id })
    }

    /// Turn the `IdVertexProxy` into a `RuntimeVertex` processed by GIE's runtime
    #[inline]
    pub fn into_runtime_vertex(self) -> RuntimeVertex {
        let id = self.vertex_id as ID;
        let label = Some(self.vertex_type_id as LabelId);
        RuntimeVertex::new(id, label, DynDetails::default())
    }
}
pub struct GrinAdjVertexIter {
    graph: GrinGraph,
    grin_adj_list_iter_vec: Vec<GrinAdjacentListIterator>,
    curr_iter: Option<GrinAdjacentListIterator>,
}

impl GrinAdjVertexIter {
    pub fn new(
        graph: GrinGraph, vertex_handle: GrinVertex, direction: GrinDirection,
        edge_type_ids: &Vec<GrinEdgeTypeId>,
    ) -> GraphProxyResult<Self> {
        unsafe {
            let mut grin_adj_list_iter_vec = Vec::with_capacity(edge_type_ids.len());
            for edge_type_id in edge_type_ids {
                let edge_type = grin_get_edge_type_by_id(graph, *edge_type_id);
                if edge_type == GRIN_NULL_EDGE_TYPE {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_edge_type_by_id`: {:?}, returns null",
                        edge_type_id
                    )));
                }
                let grin_typed_adj_list =
                    grin_get_adjacent_list_by_edge_type(graph, direction, vertex_handle, edge_type);

                if grin_typed_adj_list == GRIN_NULL_ADJACENT_LIST {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_select_edge_type_for_adjacent_list`: {:?}, returns null",
                        edge_type_id
                    )));
                }
                let grin_typed_adj_list_iter = grin_get_adjacent_list_begin(graph, grin_typed_adj_list);
                if grin_typed_adj_list_iter == GRIN_NULL_ADJACENT_LIST_ITERATOR {
                    return Err(GraphProxyError::QueryStoreError(
                        "`grin_get_adjacent_list_begin` returns null".to_string(),
                    ));
                }
                grin_adj_list_iter_vec.push(grin_typed_adj_list_iter);
                grin_destroy_edge_type(graph, edge_type);
                grin_destroy_adjacent_list(graph, grin_typed_adj_list);
            }

            grin_destroy_vertex(graph, vertex_handle);

            Ok(Self { graph, grin_adj_list_iter_vec, curr_iter: None })
        }
    }
}

impl Iterator for GrinAdjVertexIter {
    type Item = GrinIdVertexProxy;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            unsafe {
                if let Some(ref mut iter) = self.curr_iter {
                    if !grin_is_adjacent_list_end(self.graph, *iter) {
                        let vertex_handle = grin_get_neighbor_from_adjacent_list_iter(self.graph, *iter);
                        // a null vertex handle is unacceptable at the current stage
                        assert_ne!(vertex_handle, GRIN_NULL_VERTEX);
                        grin_get_next_adjacent_list_iter(self.graph, *iter);
                        return Some(GrinIdVertexProxy::new(self.graph, vertex_handle).unwrap());
                    } else {
                        if let Some(iter_val) = self.grin_adj_list_iter_vec.pop() {
                            grin_destroy_adjacent_list_iter(self.graph, *iter);
                            self.curr_iter = Some(iter_val);
                        } else {
                            return None;
                        }
                    }
                } else {
                    if let Some(iter_val) = self.grin_adj_list_iter_vec.pop() {
                        self.curr_iter = Some(iter_val);
                    } else {
                        return None;
                    }
                }
            }
        }
    }
}

impl Drop for GrinAdjVertexIter {
    fn drop(&mut self) {
        unsafe {
            for iter in self.grin_adj_list_iter_vec.drain(..) {
                grin_destroy_adjacent_list_iter(self.graph, iter);
            }
            if let Some(curr) = self.curr_iter {
                grin_destroy_adjacent_list_iter(self.graph, curr);
            }
        }
    }
}

unsafe impl Send for GrinAdjVertexIter {}
unsafe impl Sync for GrinAdjVertexIter {}

pub struct GrinAdjEdgeIter {
    graph: GrinGraph,
    grin_adj_list_iter_vec: Vec<GrinAdjacentListIterator>,
    curr_iter: Option<GrinAdjacentListIterator>,
    // TODO(bingqing): Preserve edge_type_id. when build GrinEdgeProxy, we can directly use it, instead of query edge_type for each edge.
}

impl GrinAdjEdgeIter {
    pub fn new(
        graph: GrinGraph, vertex_handle: GrinVertex, direction: GrinDirection,
        edge_type_ids: &Vec<GrinEdgeTypeId>,
    ) -> GraphProxyResult<Self> {
        unsafe {
            let mut grin_adj_list_iter_vec = Vec::with_capacity(edge_type_ids.len());
            for edge_type_id in edge_type_ids {
                let edge_type = grin_get_edge_type_by_id(graph, *edge_type_id);
                if edge_type == GRIN_NULL_EDGE_TYPE {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_edge_type_by_id`: {:?}, returns null",
                        edge_type_id
                    )));
                }
                let grin_typed_adj_list =
                    grin_get_adjacent_list_by_edge_type(graph, direction, vertex_handle, edge_type);
                if grin_typed_adj_list == GRIN_NULL_ADJACENT_LIST {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_select_edge_type_for_adjacent_list`: {:?}, returns null",
                        edge_type_id
                    )));
                }
                let grin_typed_adj_list_iter = grin_get_adjacent_list_begin(graph, grin_typed_adj_list);
                if grin_typed_adj_list_iter == GRIN_NULL_ADJACENT_LIST_ITERATOR {
                    return Err(GraphProxyError::QueryStoreError(
                        "`grin_get_adjacent_list_begin` returns null".to_string(),
                    ));
                }
                grin_adj_list_iter_vec.push(grin_typed_adj_list_iter);
                grin_destroy_edge_type(graph, edge_type);
                grin_destroy_adjacent_list(graph, grin_typed_adj_list);
            }
            grin_destroy_vertex(graph, vertex_handle);

            Ok(Self { graph, grin_adj_list_iter_vec, curr_iter: None })
        }
    }
}

impl Iterator for GrinAdjEdgeIter {
    type Item = GrinEdgeProxy;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            unsafe {
                if let Some(ref mut iter) = self.curr_iter {
                    if !grin_is_adjacent_list_end(self.graph, *iter) {
                        let edge_handle = grin_get_edge_from_adjacent_list_iter(self.graph, *iter);
                        // a null edge handle is unacceptable at the current stage
                        assert_ne!(edge_handle, GRIN_NULL_EDGE);
                        grin_get_next_adjacent_list_iter(self.graph, *iter);
                        return Some(GrinEdgeProxy::new(self.graph, edge_handle));
                    } else {
                        if let Some(iter_val) = self.grin_adj_list_iter_vec.pop() {
                            grin_destroy_adjacent_list_iter(self.graph, *iter);
                            self.curr_iter = Some(iter_val);
                        } else {
                            return None;
                        }
                    }
                } else {
                    if let Some(iter_val) = self.grin_adj_list_iter_vec.pop() {
                        self.curr_iter = Some(iter_val);
                    } else {
                        return None;
                    }
                }
            }
        }
    }
}

impl Drop for GrinAdjEdgeIter {
    fn drop(&mut self) {
        unsafe {
            for iter in self.grin_adj_list_iter_vec.drain(..) {
                grin_destroy_adjacent_list_iter(self.graph, iter);
            }
            if let Some(curr) = self.curr_iter {
                grin_destroy_adjacent_list_iter(self.graph, curr);
            }
        }
    }
}

unsafe impl Send for GrinAdjEdgeIter {}
unsafe impl Sync for GrinAdjEdgeIter {}

/// A proxy for better handling Grin's edge related operations.
pub struct GrinEdgeProxy {
    /// The graph handle in the current process
    graph: GrinGraph,
    /// The edge handle in the current process
    edge: GrinEdge,
    /// The edge type handle, for the `edge`
    edge_type: GrinEdgeType,
}

impl Drop for GrinEdgeProxy {
    fn drop(&mut self) {
        unsafe {
            //   println!("drop edge ...");
            grin_destroy_edge_type(self.graph, self.edge_type);
            grin_destroy_edge(self.graph, self.edge);
        }
    }
}

unsafe impl Send for GrinEdgeProxy {}
unsafe impl Sync for GrinEdgeProxy {}

impl GrinEdgeProxy {
    pub fn new(graph: GrinGraph, edge: GrinEdge) -> Self {
        unsafe {
            let edge_type = grin_get_edge_type(graph, edge);
            assert_ne!(edge_type, GRIN_NULL_EDGE_TYPE);
            Self { graph, edge, edge_type }
        }
    }

    pub fn get_property(&self, prop_key: &NameOrId) -> GraphProxyResult<Object> {
        unsafe {
            let prop_handle = match prop_key {
                NameOrId::Id(prop_id) => {
                    grin_get_edge_property_by_id(self.graph, self.edge_type, *prop_id as GrinEdgePropertyId)
                }
                NameOrId::Str(prop_str) => {
                    grin_get_edge_property_by_name(self.graph, self.edge_type, string_rust2c(prop_str))
                }
            };

            if prop_handle == GRIN_NULL_EDGE_PROPERTY {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_edge_property`: {:?}, returns null",
                    prop_key
                )));
            }

            let result = get_edge_property_as_object(self.graph, self.edge, prop_handle);
            grin_destroy_edge_property(self.graph, prop_handle);
            result
        }
    }

    pub fn get_properties(&self) -> GraphProxyResult<HashMap<NameOrId, Object>> {
        unsafe {
            let prop_list = grin_get_edge_property_list_by_type(self.graph, self.edge_type);
            if prop_list == GRIN_NULL_EDGE_PROPERTY_LIST {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_edge_property_list_by_type`: {:?} returns null",
                    self.edge_type
                )));
            }
            let prop_list_size = grin_get_edge_property_list_size(self.graph, prop_list);
            let mut result = HashMap::with_capacity(prop_list_size as usize);

            for i in 0..prop_list_size {
                let prop_handle = grin_get_edge_property_from_list(self.graph, prop_list, i);
                if prop_handle == GRIN_NULL_EDGE_PROPERTY {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_edge_property_from_list`: {:?} returns null",
                        i
                    )));
                }
                let prop_id = grin_get_edge_property_id(self.graph, self.edge_type, prop_handle);

                let prop_value = get_edge_property_as_object(self.graph, self.edge, prop_handle)?;
                result.insert(NameOrId::Id(prop_id as i32), prop_value);

                grin_destroy_edge_property(self.graph, prop_handle);
            }

            grin_destroy_edge_property_list(self.graph, prop_list);

            Ok(result)
        }
    }

    pub fn into_runtime_edge(
        self, prop_keys: Option<&Vec<NameOrId>>, from_src: bool,
    ) -> GraphProxyResult<RuntimeEdge> {
        unsafe {
            let src_vertex = grin_get_src_vertex_from_edge(self.graph, self.edge);
            let src_vertex_id = get_vertex_ref_id(self.graph, src_vertex)?;
            let src_vertex_type_id = get_vertex_type_id(self.graph, src_vertex)?;
            let dst_vertex = grin_get_dst_vertex_from_edge(self.graph, self.edge);
            let dst_vertex_id = get_vertex_ref_id(self.graph, dst_vertex)?;
            let dst_vertex_type_id = get_vertex_type_id(self.graph, dst_vertex)?;
            grin_destroy_vertex(self.graph, src_vertex);
            grin_destroy_vertex(self.graph, dst_vertex);
            // TODO(bingqing): this is a hack eid, we may use `edge_id` returned by storage if supported.
            let edge_id = src_vertex_id << 32 | dst_vertex_id;
            let edge_type_id = grin_get_edge_type_id(self.graph, self.edge_type);
            let details = LazyEdgeDetails::new(self, prop_keys.cloned());
            let mut edge = RuntimeEdge::with_from_src(
                edge_id as ID,
                Some(edge_type_id as LabelId),
                src_vertex_id as ID,
                dst_vertex_id as ID,
                from_src,
                DynDetails::lazy(details),
            );
            edge.set_src_label(src_vertex_type_id as LabelId);
            edge.set_dst_label(dst_vertex_type_id as LabelId);
            Ok(edge)
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Schema {
    vertex_types: Vec<GrinVertexTypeId>,
    edge_types: Vec<GrinEdgeTypeId>,
    vertex_out_edge_types: HashMap<GrinVertexTypeId, Vec<GrinEdgeTypeId>>,
    vertex_in_edge_types: HashMap<GrinVertexTypeId, Vec<GrinEdgeTypeId>>,
}

impl Schema {
    pub fn new(graph: GrinGraph) -> GraphProxyResult<Self> {
        unsafe {
            let vertex_type_list = grin_get_vertex_type_list(graph);
            if vertex_type_list == GRIN_NULL_VERTEX_TYPE_LIST {
                return Err(GraphProxyError::QueryStoreError(
                    "`grin_get_vertex_type_list`: returns null".to_string(),
                ));
            }
            let vertex_type_list_size = grin_get_vertex_type_list_size(graph, vertex_type_list);
            let mut vertex_type_id_list = Vec::with_capacity(vertex_type_list_size);
            for i in 0..vertex_type_list_size {
                let vertex_type = grin_get_vertex_type_from_list(graph, vertex_type_list, i);
                if vertex_type == GRIN_NULL_VERTEX_TYPE {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_vertex_type_from_list`: {:?}, returns null",
                        i
                    )));
                }
                let vertex_type_id = grin_get_vertex_type_id(graph, vertex_type);
                if vertex_type_id == GRIN_NULL_VERTEX_TYPE_ID {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_vertex_type_id`: {:?}, returns null",
                        vertex_type
                    )));
                }
                vertex_type_id_list.push(vertex_type_id);
                grin_destroy_vertex_type(graph, vertex_type);
            }
            grin_destroy_vertex_type_list(graph, vertex_type_list);

            let edge_type_list = grin_get_edge_type_list(graph);
            if edge_type_list == GRIN_NULL_EDGE_TYPE_LIST {
                return Err(GraphProxyError::QueryStoreError(
                    "`grin_get_edge_type_list`: returns null".to_string(),
                ));
            }
            let edge_type_list_size = grin_get_edge_type_list_size(graph, edge_type_list);
            let mut edge_type_id_list = Vec::with_capacity(edge_type_list_size);
            let mut edge_types = Vec::with_capacity(edge_type_list_size);
            for i in 0..edge_type_list_size {
                let edge_type = grin_get_edge_type_from_list(graph, edge_type_list, i);
                if edge_type == GRIN_NULL_EDGE_TYPE {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_edge_type_from_list`: {:?}, returns null",
                        i
                    )));
                }
                let edge_type_id = grin_get_edge_type_id(graph, edge_type);
                if edge_type_id == GRIN_NULL_EDGE_TYPE_ID {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_edge_type_id`: {:?}, returns null",
                        edge_type
                    )));
                }
                edge_type_id_list.push(edge_type_id);
                edge_types.push(edge_type);

                grin_destroy_edge_type(graph, edge_type);
            }
            grin_destroy_edge_type_list(graph, edge_type_list);

            let mut vertex_out_edge_types = HashMap::new();
            let mut vertex_in_edge_types = HashMap::new();

            for (edge_type_id, edge_type) in edge_type_id_list.iter().zip(edge_types.iter()) {
                let src_vertex_types = grin_get_src_types_by_edge_type(graph, *edge_type);
                if src_vertex_types == GRIN_NULL_VERTEX_TYPE_LIST {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_src_types_by_edge_type`: {:?}, returns null",
                        edge_type
                    )));
                }
                let src_vertex_types_size = grin_get_vertex_type_list_size(graph, src_vertex_types);
                let dst_vertex_types = grin_get_dst_types_by_edge_type(graph, *edge_type);
                if dst_vertex_types == GRIN_NULL_VERTEX_TYPE_LIST {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_dst_types_by_edge_type`: {:?}, returns null",
                        edge_type
                    )));
                }
                let dst_vertex_types_size = grin_get_vertex_type_list_size(graph, dst_vertex_types);
                if src_vertex_types_size != dst_vertex_types_size {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "src_vertex_types_size != dst_vertex_types_size: {:?} != {:?}",
                        src_vertex_types_size, dst_vertex_types_size
                    )));
                }
                for i in 0..src_vertex_types_size {
                    let src_vertex_type = grin_get_vertex_type_from_list(graph, src_vertex_types, i);
                    if src_vertex_type == GRIN_NULL_VERTEX_TYPE {
                        return Err(GraphProxyError::QueryStoreError(format!(
                            "`grin_get_vertex_type_from_list`: {:?}, returns null",
                            i
                        )));
                    }
                    let src_vertex_type_id = grin_get_vertex_type_id(graph, src_vertex_type);
                    if src_vertex_type_id == GRIN_NULL_VERTEX_TYPE_ID {
                        return Err(GraphProxyError::QueryStoreError(format!(
                            "`grin_get_vertex_type_id`: {:?}, returns null",
                            src_vertex_type
                        )));
                    }
                    let src_vertex_out_edge_types = vertex_out_edge_types
                        .entry(src_vertex_type_id)
                        .or_insert(Vec::new());
                    // be carefully that edge_type_id could be duplicated
                    if !src_vertex_out_edge_types.contains(edge_type_id) {
                        src_vertex_out_edge_types.push(*edge_type_id);
                    }
                    grin_destroy_vertex_type(graph, src_vertex_type);
                    let dst_vertex_type = grin_get_vertex_type_from_list(graph, dst_vertex_types, i);
                    if dst_vertex_type == GRIN_NULL_VERTEX_TYPE {
                        return Err(GraphProxyError::QueryStoreError(format!(
                            "`grin_get_vertex_type_from_list`: {:?}, returns null",
                            i
                        )));
                    }
                    let dst_vertex_type_id = grin_get_vertex_type_id(graph, dst_vertex_type);
                    if dst_vertex_type_id == GRIN_NULL_VERTEX_TYPE_ID {
                        return Err(GraphProxyError::QueryStoreError(format!(
                            "`grin_get_vertex_type_id`: {:?}, returns null",
                            dst_vertex_type
                        )));
                    }
                    let dst_vertex_in_edge_types = vertex_in_edge_types
                        .entry(dst_vertex_type_id)
                        .or_insert(Vec::new());
                    if !dst_vertex_in_edge_types.contains(edge_type_id) {
                        dst_vertex_in_edge_types.push(*edge_type_id);
                    }
                    grin_destroy_vertex_type(graph, dst_vertex_type);
                }
                grin_destroy_vertex_type_list(graph, src_vertex_types);
                grin_destroy_vertex_type_list(graph, dst_vertex_types);
            }

            Ok(Self {
                vertex_types: vertex_type_id_list,
                edge_types: edge_type_id_list,
                vertex_out_edge_types,
                vertex_in_edge_types,
            })
        }
    }
    fn get_all_vertex_type_ids(&self) -> Vec<GrinVertexTypeId> {
        self.vertex_types.clone()
    }
    fn get_all_edge_type_ids(&self) -> Vec<GrinEdgeTypeId> {
        self.edge_types.clone()
    }
    // Return the out edge types of a vertex type,
    // and empty vector means no out edge types.
    fn get_out_edge_type_ids(&self, vertex_type_id: GrinVertexTypeId) -> Vec<GrinEdgeTypeId> {
        self.vertex_out_edge_types
            .get(&vertex_type_id)
            .cloned()
            .unwrap_or_default()
    }
    // Return the in edge types of a vertex type,
    // and empty vector means no in edge types.
    fn get_in_edge_type_ids(&self, vertex_type_id: GrinVertexTypeId) -> Vec<GrinEdgeTypeId> {
        self.vertex_in_edge_types
            .get(&vertex_type_id)
            .cloned()
            .unwrap_or_default()
    }
    // Return the adjacent edge types of a vertex type,
    // and empty vector means no adjacent edge types.
    fn get_adjacent_type_ids(
        &self, vertex_type_id: GrinVertexTypeId, direction: GrinDirection,
    ) -> Vec<GrinEdgeTypeId> {
        if direction == GRIN_DIRECTION_OUT {
            self.get_out_edge_type_ids(vertex_type_id)
        } else if direction == GRIN_DIRECTION_IN {
            self.get_in_edge_type_ids(vertex_type_id)
        } else {
            let mut out_edge_type_ids = self.get_out_edge_type_ids(vertex_type_id);
            out_edge_type_ids.extend(self.get_in_edge_type_ids(vertex_type_id));
            out_edge_type_ids
        }
    }
}
/// A proxy for better handling Grin's graph related operations.
// TODO(bingqing): optimize if only one partition in current process.
pub struct GrinGraphProxy {
    /// The partitioned graph handle in the current process
    partitioned_graph: GrinPartitionedGraph,
    /// The graph handles managed in the current partition
    graphs: HashMap<GrinPartitionId, GrinGraph>,
    /// The schema of the graph
    // TODO: this is a temporary solution, schema related info would be given in the query plan. Then this would be removed.
    schema: Arc<Schema>,
}

impl Drop for GrinGraphProxy {
    fn drop(&mut self) {
        unsafe {
            grin_destroy_partitioned_graph(self.partitioned_graph);
            for (_, graph) in self.graphs.iter() {
                grin_destroy_graph(*graph);
            }
        }
    }
}

impl GrinGraphProxy {
    pub fn new(partitioned_graph: GrinPartitionedGraph) -> GraphProxyResult<Self> {
        let mut graphs = HashMap::new();
        unsafe {
            let partition_list = grin_get_local_partition_list(partitioned_graph);
            if partition_list == GRIN_NULL_PARTITION_LIST {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_local_partition_list` returns `null`"
                )));
            }

            let partition_size = grin_get_partition_list_size(partitioned_graph, partition_list);
            for i in 0..partition_size {
                let partition = grin_get_partition_from_list(partitioned_graph, partition_list, i);
                if partition == GRIN_NULL_PARTITION {
                    return Err(GraphProxyError::QueryStoreError(format!(
                        "`grin_get_partition_from_list`: {:?} returns `null`",
                        i
                    )));
                }
                let partition_id = grin_get_partition_id(partitioned_graph, partition);
                let graph = grin_get_local_graph_by_partition(partitioned_graph, partition);
                graphs.insert(partition_id, graph);
                grin_destroy_partition(partitioned_graph, partition);
            }
            grin_destroy_partition_list(partitioned_graph, partition_list);
        }
        let schema = Schema::new(graphs.values().next().cloned().unwrap())?;

        Ok(GrinGraphProxy { partitioned_graph, graphs, schema: Arc::new(schema) })
    }

    pub fn get_partitioned_graph(&self) -> GrinPartitionedGraph {
        self.partitioned_graph
    }

    pub fn get_local_graph(&self, partition_id: GrinPartitionId) -> Option<GrinGraph> {
        self.graphs.get(&partition_id).cloned()
    }

    pub fn get_any_local_graph(&self) -> GraphProxyResult<GrinGraph> {
        self.graphs
            .values()
            .next()
            .cloned()
            .ok_or(GraphProxyError::QueryStoreError("No local graph found in current process".to_string()))
    }

    pub fn get_local_partition_ids(&self) -> Vec<GrinPartitionId> {
        self.graphs.keys().cloned().collect()
    }

    /// Get all vertices in the given partitions and vertex types.
    /// Return an iterator for scanning the vertices.
    pub fn get_all_vertices(
        &self, partitions: &Vec<GrinPartitionId>, label_ids: &Vec<GrinVertexTypeId>,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = GrinVertexProxy> + Send>> {
        let mut results = Vec::new();
        // TODO: GRIN needs to specify type ids to scan, while GIE-Gremlin doesn't support if to scan all types.
        // For now, we query the types in GraphProxy.
        // It should be specified in the `QueryParams` given in query plan later.
        let vertex_type_ids =
            if label_ids.is_empty() { self.schema.get_all_vertex_type_ids() } else { label_ids.clone() };
        for partition in partitions {
            if let Some(graph) = self.graphs.get(partition).cloned() {
                let vertex_iter = GrinVertexIter::new(graph, &vertex_type_ids)?;
                results.push(vertex_iter);
            } else {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "Partition {:?} is not found in the current process",
                    partition
                )));
            }
        }

        Ok(Box::new(
            results
                .into_iter()
                .flat_map(move |vertex_iter| vertex_iter),
        ))
    }

    #[cfg(feature = "grin_enable_edge_list")]
    pub fn get_all_edges(
        &self, partitions: &Vec<GrinPartitionId>, label_ids: &Vec<GrinEdgeTypeId>,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = GrinEdgeProxy> + Send>> {
        let mut results = Vec::new();
        let edge_type_ids =
            if label_ids.is_empty() { self.schema.get_all_edge_type_ids() } else { label_ids.clone() };
        for partition in partitions {
            if let Some(graph) = self.graphs.get(partition).cloned() {
                let edge_iter = GrinEdgeIter::new(graph, &edge_type_ids)?;
                results.push(edge_iter);
            } else {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "Partition {:?} is not found in the current process",
                    partition
                )));
            }
        }
        Ok(Box::new(
            results
                .into_iter()
                .flat_map(move |edge_iter| edge_iter),
        ))
    }

    pub fn get_vertex_by_index(
        &self, grin_type_id: GrinVertexTypeId, primary_key: &PKV,
    ) -> GraphProxyResult<Option<GrinIdVertexProxy>> {
        #[cfg(feature = "grin_enable_vertex_primary_keys")]
        unsafe {
            let any_grin_graph = self.get_any_local_graph()?;
            let grin_vertex_type = grin_get_vertex_type_by_id(any_grin_graph, grin_type_id);
            let grin_row = grin_create_row(any_grin_graph);
            match primary_key {
                OneOrMany::One(pkv) => build_grin_row_from_object(any_grin_graph, grin_row, &pkv[0].1)?,
                OneOrMany::Many(pkvs) => {
                    for (_pk, value) in pkvs {
                        build_grin_row_from_object(any_grin_graph, grin_row, value)?
                    }
                }
            };
            let grin_vertex =
                grin_get_vertex_by_primary_keys_row(any_grin_graph, grin_vertex_type, grin_row);
            grin_destroy_row(any_grin_graph, grin_row);

            if grin_vertex == GRIN_NULL_VERTEX {
                Ok(None)
            } else {
                Ok(Some(GrinIdVertexProxy::new(any_grin_graph, grin_vertex).unwrap()))
            }
        }
        #[cfg(not(feature = "grin_enable_vertex_primary_keys"))]
        Err(GraphProxyError::unsupported_error("Vertex primary keys are not enabled"))
    }

    fn fill_in_adjacent_edge_types(
        &self, graph: GrinGraph, grin_vertex: GrinVertex, grin_direction: GrinDirection,
        edge_type_ids: &Vec<GrinEdgeTypeId>,
    ) -> GraphProxyResult<Vec<GrinEdgeTypeId>> {
        let mut adjacent_edge_type_ids = edge_type_ids.clone();
        if edge_type_ids.is_empty() {
            let vertex_type_id = self.get_vertex_type_id(graph, grin_vertex)?;
            adjacent_edge_type_ids = self
                .schema
                .get_adjacent_type_ids(vertex_type_id, grin_direction);
        }
        Ok(adjacent_edge_type_ids)
    }

    pub fn get_neighbor_vertices(
        &self, vertex_id: i64, grin_direction: GrinDirection, edge_type_ids: &Vec<GrinEdgeTypeId>,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = GrinIdVertexProxy> + Send>> {
        let partition_id = self.get_partition_id_by_vertex_id(vertex_id)?;
        if let Some(graph) = self.graphs.get(&partition_id).cloned() {
            let grin_vertex = self.get_vertex_by_id(graph, vertex_id)?;
            let edge_type_ids =
                self.fill_in_adjacent_edge_types(graph, grin_vertex, grin_direction, edge_type_ids)?;
            if edge_type_ids.is_empty() {
                // no adjacent edge types in schema, return empty iterator
                Ok(Box::new(std::iter::empty()))
            } else {
                let neighbor_vertices_iter =
                    GrinAdjVertexIter::new(graph, grin_vertex, grin_direction, &edge_type_ids)?;
                Ok(Box::new(neighbor_vertices_iter))
            }
        } else {
            Err(GraphProxyError::QueryStoreError(format!(
                "Partition {:?} is not found in the current process",
                partition_id
            )))
        }
    }

    pub fn get_neighbor_edges(
        &self, vertex_id: i64, grin_direction: GrinDirection, edge_type_ids: &Vec<GrinEdgeTypeId>,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = GrinEdgeProxy> + Send>> {
        let partition_id = self.get_partition_id_by_vertex_id(vertex_id)?;
        if let Some(graph) = self.graphs.get(&partition_id).cloned() {
            let grin_vertex = self.get_vertex_by_id(graph, vertex_id)?;
            let edge_type_ids =
                self.fill_in_adjacent_edge_types(graph, grin_vertex, grin_direction, edge_type_ids)?;
            if edge_type_ids.is_empty() {
                // no adjacent edge types in schema, return empty iterator
                Ok(Box::new(std::iter::empty()))
            } else {
                let neighbor_edges_iter =
                    GrinAdjEdgeIter::new(graph, grin_vertex, grin_direction, &edge_type_ids)?;
                Ok(Box::new(neighbor_edges_iter))
            }
        } else {
            Err(GraphProxyError::QueryStoreError(format!(
                "Partition {:?} is not found in the current process",
                partition_id
            )))
        }
    }

    /// Get `GrinVertexProxy` for the given vertex id.
    pub fn get_vertex(&self, vertex_id: i64) -> GraphProxyResult<GrinVertexProxy> {
        let partition_id = self.get_partition_id_by_vertex_id(vertex_id)?;
        if let Some(graph) = self.graphs.get(&partition_id).cloned() {
            let grin_vertex = self.get_vertex_by_id(graph, vertex_id)?;
            let grin_vertex_proxy = GrinVertexProxy::new(graph, grin_vertex);
            Ok(grin_vertex_proxy)
        } else {
            Err(GraphProxyError::QueryStoreError(format!(
                "Partition {:?} is not found in the current process",
                partition_id
            )))
        }
    }

    /// Count all vertices in the given partitions with specified vertex types.
    /// Return the count
    pub fn count_all_vertices(
        &self, partitions: &Vec<GrinPartitionId>, label_ids: &Vec<GrinVertexTypeId>,
    ) -> GraphProxyResult<u64> {
        let mut count = 0;
        // TODO: GRIN needs to specify type ids to scan, while GIE-Gremlin doesn't support if to scan all types.
        // For now, we query the types in GraphProxy.
        // It should be specified in the `QueryParams` given in query plan later.
        let vertex_type_ids =
            if label_ids.is_empty() { self.schema.get_all_vertex_type_ids() } else { label_ids.clone() };
        for partition in partitions {
            if let Some(graph) = self.graphs.get(partition).cloned() {
                for type_id in &vertex_type_ids {
                    unsafe {
                        count += grin_get_vertex_num_by_type(graph, *type_id);
                    }
                }
            } else {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "Partition {:?} is not found in the current process",
                    partition
                )));
            }
        }

        Ok(count as u64)
    }

    /// Count all edges in the given partitions with specified edge types.
    /// Return the count
    pub fn count_all_edges(
        &self, partitions: &Vec<GrinPartitionId>, label_ids: &Vec<GrinEdgeTypeId>,
    ) -> GraphProxyResult<u64> {
        let mut count = 0;
        // TODO: GRIN needs to specify type ids to scan, while GIE-Gremlin doesn't support if to scan all types.
        // For now, we query the types in GraphProxy.
        // It should be specified in the `QueryParams` given in query plan later.
        let vertex_type_ids =
            if label_ids.is_empty() { self.schema.get_all_vertex_type_ids() } else { label_ids.clone() };
        for partition in partitions {
            if let Some(graph) = self.graphs.get(partition).cloned() {
                for type_id in &vertex_type_ids {
                    unsafe {
                        count += grin_get_edge_num_by_type(graph, *type_id);
                    }
                }
            } else {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "Partition {:?} is not found in the current process",
                    partition
                )));
            }
        }

        Ok(count as u64)
    }

    pub fn get_vertex_by_id(&self, grin_graph: GrinGraph, vertex_id: i64) -> GraphProxyResult<GrinVertex> {
        unsafe {
            let vertex_ref = grin_deserialize_int64_to_vertex_ref(grin_graph, vertex_id);
            assert_ne!(vertex_ref, GRIN_NULL_VERTEX_REF);
            let vertex = grin_get_vertex_from_vertex_ref(grin_graph, vertex_ref);
            assert_ne!(vertex, GRIN_NULL_VERTEX);
            grin_destroy_vertex_ref(grin_graph, vertex_ref);
            Ok(vertex)
        }
    }

    fn get_vertex_type_id(
        &self, grin_graph: GrinGraph, grin_vertex: GrinVertex,
    ) -> GraphProxyResult<GrinVertexTypeId> {
        unsafe {
            let vertex_type = grin_get_vertex_type(grin_graph, grin_vertex);
            let vertex_type_id = grin_get_vertex_type_id(grin_graph, vertex_type);
            grin_destroy_vertex_type(grin_graph, vertex_type);
            Ok(vertex_type_id)
        }
    }

    pub fn get_partition_id_by_vertex_id(&self, vertex_id: i64) -> GraphProxyResult<GrinPartitionId> {
        unsafe {
            let any_grin_graph = self.get_any_local_graph()?;
            let vertex_ref = grin_deserialize_int64_to_vertex_ref(any_grin_graph, vertex_id);
            let partition = grin_get_master_partition_from_vertex_ref(any_grin_graph, vertex_ref);
            let partition_id = grin_get_partition_id(self.partitioned_graph, partition);
            grin_destroy_vertex_ref(any_grin_graph, vertex_ref);
            grin_destroy_partition(self.partitioned_graph, partition);
            Ok(partition_id)
        }
    }
}

unsafe impl Send for GrinGraphProxy {}
unsafe impl Sync for GrinGraphProxy {}

/// An arc wrapper of GrinGraphRuntime to free it from unexpected (double) pointer destory.
pub struct GrinGraphRuntime {
    store: Arc<GrinGraphProxy>,
    server_partitions: Vec<GrinPartitionId>,
    cluster_info: Arc<dyn ClusterInfo>,
}

impl GrinGraphRuntime {
    pub fn new(
        store: Arc<GrinGraphProxy>, server_partitions: Vec<PartitionId>, cluster_info: Arc<dyn ClusterInfo>,
    ) -> Self {
        GrinGraphRuntime { store, server_partitions, cluster_info }
    }

    fn assign_worker_partitions(&self) -> GraphProxyResult<Vec<GrinPartitionId>> {
        let workers_num = self.cluster_info.get_local_worker_num()?;
        let worker_idx = self.cluster_info.get_worker_index()?;
        let mut worker_partition_list = vec![];
        for pid in &self.server_partitions {
            if *pid % workers_num == worker_idx % workers_num {
                worker_partition_list.push(*pid as GrinPartitionId)
            }
        }
        debug!(
            "workers_num {:?}, worker_idx: {:?},  worker_partition_list {:?}",
            workers_num, worker_idx, worker_partition_list
        );
        Ok(worker_partition_list)
    }

    fn encode_storage_vlabel(&self, label: LabelId) -> GrinVertexTypeId {
        label as GrinVertexTypeId
    }

    fn encode_storage_vlabels(&self, labels: &Vec<LabelId>) -> Vec<GrinVertexTypeId> {
        labels
            .iter()
            .map(|label| self.encode_storage_vlabel(*label))
            .collect()
    }

    fn encode_storage_elabel(&self, label: LabelId) -> GrinEdgeTypeId {
        label as GrinEdgeTypeId
    }

    fn encode_storage_elabels(&self, labels: &Vec<LabelId>) -> Vec<GrinEdgeTypeId> {
        labels
            .iter()
            .map(|label| self.encode_storage_elabel(*label))
            .collect()
    }
}

unsafe impl Send for GrinGraphRuntime {}
unsafe impl Sync for GrinGraphRuntime {}

impl ReadGraph for GrinGraphRuntime {
    fn scan_vertex(
        &self, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = RuntimeVertex> + Send>> {
        let worker_partitions = self.assign_worker_partitions()?;
        if !worker_partitions.is_empty() {
            let store = self.store.clone();
            let label_ids = self.encode_storage_vlabels(&params.labels);
            let row_filter = params.filter.clone();
            let columns = params.columns.clone();
            let result_iter = store
                .get_all_vertices(worker_partitions.as_ref(), &label_ids)?
                .map(move |graph_proxy| {
                    graph_proxy
                        .into_runtime_vertex(columns.as_ref())
                        .unwrap()
                });

            Ok(filter_sample_limit!(result_iter, row_filter, params.sample_ratio, params.limit))
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn index_scan_vertex(
        &self, label: LabelId, primary_key: &PKV, _params: &QueryParams,
    ) -> GraphProxyResult<Option<RuntimeVertex>> {
        // TODO(bingqing): confirm no duplicated vertex in the result
        let store = self.store.clone();
        let vertex_type_id = self.encode_storage_vlabel(label);
        let result = store
            .get_vertex_by_index(vertex_type_id, primary_key)?
            .map(move |vertex_proxy| vertex_proxy.into_runtime_vertex());
        Ok(result)
    }

    fn scan_edge(
        &self, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = RuntimeEdge> + Send>> {
        #[cfg(feature = "grin_enable_edge_list")]
        {
            let worker_partitions = self.assign_worker_partitions()?;
            if !worker_partitions.is_empty() {
                let store = self.store.clone();
                let label_ids = self.encode_storage_elabels(&params.labels);
                let row_filter = params.filter.clone();
                let columns = params.columns.clone();
                let result_iter = store
                    .get_all_edges(worker_partitions.as_ref(), &label_ids)?
                    .map(move |graph_proxy| {
                        graph_proxy
                            .into_runtime_edge(columns.as_ref(), true)
                            .unwrap()
                    });

                Ok(filter_sample_limit!(result_iter, row_filter, params.sample_ratio, params.limit))
            } else {
                Ok(Box::new(std::iter::empty()))
            }
        }
        #[cfg(not(feature = "grin_enable_edge_list"))]
        {
            Err(GraphProxyError::QueryStoreError("Edge list is not enabled".to_string()))
        }
    }

    fn get_vertex(
        &self, ids: &[ID], params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = RuntimeVertex> + Send>> {
        let store = self.store.clone();
        let row_filter = params.filter.clone();
        let columns = params.columns.clone();
        let mut result_vec = Vec::with_capacity(ids.len());
        for id in ids {
            let vertex_id = *id as i64;
            let result = store.get_vertex(vertex_id)?;
            result_vec.push(result);
        }
        let result_iter = result_vec.into_iter().map(move |vertex_proxy| {
            vertex_proxy
                .into_runtime_vertex(columns.as_ref())
                .unwrap()
        });

        Ok(filter_limit!(result_iter, row_filter, None))
    }

    fn get_edge(
        &self, _ids: &[ID], _params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = RuntimeEdge> + Send>> {
        Err(GraphProxyError::unsupported_error("get_edge() in GrinGraphRuntime"))
    }

    fn prepare_explore_vertex(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, RuntimeVertex>>> {
        let store = self.store.clone();
        let row_filter = params.filter.clone();
        let edge_type_ids = self.encode_storage_elabels(&params.labels);
        let stmt = from_fn(move |vertex_id: ID| match direction {
            Direction::Out => {
                let neighbor_vertex_iter = store
                    .get_neighbor_vertices(vertex_id, GRIN_DIRECTION_OUT, &edge_type_ids)?
                    .map(|vertex_proxy| vertex_proxy.into_runtime_vertex());
                Ok(filter_limit!(neighbor_vertex_iter, row_filter, None))
            }
            Direction::In => {
                let neighbor_vertex_iter = store
                    .get_neighbor_vertices(vertex_id, GRIN_DIRECTION_IN, &edge_type_ids)?
                    .map(|vertex_proxy| vertex_proxy.into_runtime_vertex());
                Ok(filter_limit!(neighbor_vertex_iter, row_filter, None))
            }
            Direction::Both => {
                let out_neighbor_vertex_iter =
                    store.get_neighbor_vertices(vertex_id, GRIN_DIRECTION_OUT, &edge_type_ids)?;
                let in_neighbor_vertex_iter =
                    store.get_neighbor_vertices(vertex_id, GRIN_DIRECTION_IN, &edge_type_ids)?;
                let neighbor_vertex_iter = out_neighbor_vertex_iter
                    .chain(in_neighbor_vertex_iter)
                    .map(|vertex_proxy| vertex_proxy.into_runtime_vertex());
                Ok(filter_limit!(neighbor_vertex_iter, row_filter, None))
            }
        });
        // TODO: may not reasonable to assume GrinIdVertexProxy; But can also be GrinVertexProxy
        Ok(stmt)
    }

    fn prepare_explore_edge(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, RuntimeEdge>>> {
        let store = self.store.clone();
        let row_filter = params.filter.clone();
        let edge_type_ids = self.encode_storage_elabels(&params.labels);
        let columns = params.columns.clone();
        let stmt = from_fn(move |vertex_id: ID| {
            let columns = columns.clone();
            let columns_clone = columns.clone();
            match direction {
                Direction::Out => {
                    let neighbor_edge_iter = store
                        .get_neighbor_edges(vertex_id, GRIN_DIRECTION_OUT, &edge_type_ids)?
                        .map(move |edge_proxy| {
                            edge_proxy
                                .into_runtime_edge(columns.as_ref(), true)
                                .unwrap()
                        });
                    Ok(filter_limit!(neighbor_edge_iter, row_filter, None))
                }
                Direction::In => {
                    let neighbor_edge_iter = store
                        .get_neighbor_edges(vertex_id, GRIN_DIRECTION_IN, &edge_type_ids)?
                        .map(move |edge_proxy| {
                            edge_proxy
                                .into_runtime_edge(columns.as_ref(), false)
                                .unwrap()
                        });
                    Ok(filter_limit!(neighbor_edge_iter, row_filter, None))
                }
                Direction::Both => {
                    let out_neighbor_edge_iter = store
                        .get_neighbor_edges(vertex_id, GRIN_DIRECTION_OUT, &edge_type_ids)?
                        .map(move |edge_proxy| {
                            edge_proxy
                                .into_runtime_edge(columns.as_ref(), true)
                                .unwrap()
                        });
                    let in_neighbor_edge_iter = store
                        .get_neighbor_edges(vertex_id, GRIN_DIRECTION_IN, &edge_type_ids)?
                        .map(move |edge_proxy| {
                            edge_proxy
                                .into_runtime_edge(columns_clone.as_ref(), false)
                                .unwrap()
                        });
                    Ok(filter_limit!(out_neighbor_edge_iter.chain(in_neighbor_edge_iter), row_filter, None))
                }
            }
        });

        // TODO: filter_limit!
        Ok(stmt)
    }

    fn get_primary_key(&self, _id: &ID) -> GraphProxyResult<Option<PKV>> {
        todo!()
    }

    fn count_vertex(&self, params: &QueryParams) -> GraphProxyResult<u64> {
        if params.filter.is_some() {
            // the filter can not be pushed down to store,
            // so we need to scan all vertices with filter and then count
            Ok(self.scan_vertex(params)?.count() as u64)
        } else {
            let worker_partitions = self.assign_worker_partitions()?;
            if !worker_partitions.is_empty() {
                let store = self.store.clone();
                let label_ids: Vec<GrinVertexTypeId> = self.encode_storage_vlabels(&params.labels);
                let count = store.count_all_vertices(&worker_partitions, &label_ids)?;
                Ok(count)
            } else {
                Ok(0)
            }
        }
    }

    fn count_edge(&self, params: &QueryParams) -> GraphProxyResult<u64> {
        #[cfg(feature = "grin_enable_edge_list")]
        {
            if params.filter.is_some() {
                // the filter can not be pushed down to store,
                // so we need to scan all vertices with filter and then count
                Ok(self.scan_edge(params)?.count() as u64)
            } else {
                let worker_partitions = self.assign_worker_partitions()?;
                if !worker_partitions.is_empty() {
                    let store = self.store.clone();
                    let label_ids: Vec<GrinVertexTypeId> = self.encode_storage_vlabels(&params.labels);
                    let count = store.count_all_edges(&worker_partitions, &label_ids)?;
                    Ok(count)
                } else {
                    Ok(0)
                }
            }
        }
        #[cfg(not(feature = "grin_enable_edge_list"))]
        {
            Err(GraphProxyError::QueryStoreError("Edge list is not enabled".to_string()))
        }
    }
}
