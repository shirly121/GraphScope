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

use std::sync::Arc;
use std::{fmt, vec};

use ahash::{HashMap, HashMapExt};
use dyn_type::{Object, Primitives};
use grin::grin_v6d::*;
use ir_common::{LabelId, NameOrId, OneOrMany};

use crate::adapters::grin_store::details::{LazyEdgeDetails, LazyVertexDetails};
use crate::adapters::grin_store::native_utils::*;
use crate::apis::graph::{ID, PKV};
use crate::apis::{
    from_fn, register_graph, Direction, DynDetails, Edge as RuntimeEdge, QueryParams, ReadGraph, Statement,
    Vertex as RuntimeVertex,
};
use crate::utils::expr::eval_pred::PEvaluator;
use crate::{filter_limit, filter_sample_limit, limit_n, sample_limit};
use crate::{GraphProxyError, GraphProxyResult};

const FAKE_EDGE_ID: i64 = 0;

#[allow(dead_code)]
pub fn create_grin_store(store: GrinPartitionedGraph) {
    let grin_graph_proxy = GrinGraphProxy::new(store).unwrap();
    let graph = GrinGraphRuntime::from(grin_graph_proxy);
    register_graph(Arc::new(graph));
}

/// Get value of certain type of vertex property from the grin-enabled graph, and turn it into
/// `Object` accessible to the runtime.
#[inline]
fn get_vertex_propoerty_as_object(
    graph: GrinGraph, vertex: GrinVertex, prop_table: GrinVertexPropertyTable,
    prop_handle: GrinVertexProperty,
) -> GraphProxyResult<Object> {
    unsafe {
        let prop_type = grin_get_vertex_property_datatype(graph, prop_handle);
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
            grin_destroy_string_value(graph, c_str);
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
    graph: GrinGraph, edge: GrinEdge, prop_table: GrinEdgePropertyTable, prop_handle: GrinEdgeProperty,
) -> GraphProxyResult<Object> {
    unsafe {
        let prop_type = grin_get_edge_property_datatype(graph, prop_handle);
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
            grin_destroy_string_value(graph, c_str);
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
        if vertex_type_id == GRIN_NULL_NATURAL_ID {
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
    /// The vertex property table handle, for accessing properties of the `vertex`
    prop_table: GrinVertexPropertyTable,
}

impl Drop for GrinVertexProxy {
    fn drop(&mut self) {
        unsafe {
            println!("drop vertex...");
            grin_destroy_vertex_type(self.graph, self.vertex_type);
            grin_destroy_vertex(self.graph, self.vertex);
            grin_destroy_vertex_property_table(self.graph, self.prop_table);
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
            // .field("prop_table", &self.prop_table)
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
            let prop_table = grin_get_vertex_property_table_by_type(graph, vertex_type);
            assert!(!prop_table.is_null());
            GrinVertexProxy { graph, vertex, vertex_type, prop_table }
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
            let result =
                get_vertex_propoerty_as_object(self.graph, self.vertex, self.prop_table, prop_handle);
            grin_destroy_vertex_property(self.graph, prop_handle);

            result
        }
    }

    /// Get all properties of the vertex
    pub fn get_properties(&self) -> GraphProxyResult<HashMap<NameOrId, Object>> {
        unsafe {
            let prop_list = grin_get_vertex_property_list_by_type(self.graph, self.vertex_type);
            // TODO(bingqing): fix all is_null() checks
            if prop_list.is_null() {
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
    /// A vertex list handle, used as an entry for accessing all vertices of the graph
    vertex_list: GrinVertexList,
    /// A vertex list iterator handle, used to iterate over the vertex list
    vertex_iter: GrinVertexListIterator,
    /// A per-type map of vertex list handles, used as an entry for accessing all vertices of specific type
    vertex_type_list: HashMap<GrinVertexTypeId, GrinVertexList>,
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
            let vertex_list = grin_get_vertex_list(graph);
            if vertex_list.is_null() {
                return Err(GraphProxyError::QueryStoreError(
                    "`grin_get_vertex_list` returns null".to_string(),
                ));
            }
            let vertex_iter = grin_get_vertex_list_begin(graph, vertex_list);
            if vertex_iter == GRIN_NULL_LIST_ITERATOR {
                return Err(GraphProxyError::QueryStoreError(
                    "`grin_get_vertex_list_begin` returns null".to_string(),
                ));
            }

            let mut vertex_type_list = HashMap::new();
            let mut vertex_type_iter = HashMap::new();
            let mut current_vertex_type_id = GRIN_NULL_VERTEX_TYPE;
            let mut vertex_type_ids = vertex_type_ids.clone();

            if !vertex_type_ids.is_empty() {
                for vertex_type_id in &vertex_type_ids {
                    let vertex_type = grin_get_vertex_type_by_id(graph, *vertex_type_id);
                    if vertex_type == GRIN_NULL_VERTEX_TYPE {
                        return Err(GraphProxyError::QueryStoreError(format!(
                            "`grin_get_vertex_type_by_id`: {:?}, returns null",
                            vertex_type_id
                        )));
                    }
                    let vtype_list = grin_select_type_for_vertex_list(graph, vertex_type, vertex_list);
                    if vtype_list.is_null() {
                        return Err(GraphProxyError::QueryStoreError(format!(
                            "`grin_select_type_for_vertex_list`: {:?}, returns null",
                            vertex_type_id
                        )));
                    }
                    let vtype_iter = grin_get_vertex_list_begin(graph, vtype_list);
                    if vtype_iter == GRIN_NULL_LIST_ITERATOR {
                        return Err(GraphProxyError::QueryStoreError(
                            "`grin_get_vertex_list_begin` returns null".to_string(),
                        ));
                    }
                    vertex_type_list.insert(*vertex_type_id, vtype_list);
                    vertex_type_iter.insert(*vertex_type_id, vtype_iter);

                    grin_destroy_vertex_type(graph, vertex_type);
                }
                current_vertex_type_id = vertex_type_ids.pop().unwrap();
            }
            Ok(Self {
                graph,
                vertex_list,
                vertex_iter,
                vertex_type_list,
                vertex_type_iter,
                vertex_type_ids,
                current_vertex_type_id,
            })
        }
    }

    pub fn get_current_vertex_iter(&self) -> GrinVertexListIterator {
        if !self.vertex_type_iter.is_empty() {
            self.vertex_type_iter[&self.current_vertex_type_id].clone()
        } else {
            // empty vertex_type_iter means scan all vertex types
            self.vertex_iter.clone()
        }
    }
}

impl Drop for GrinVertexIter {
    fn drop(&mut self) {
        unsafe {
            grin_destroy_vertex_list(self.graph, self.vertex_list);
            grin_destroy_vertex_list_iter(self.graph, self.vertex_iter);

            for (_, list) in self.vertex_type_list.iter() {
                grin_destroy_vertex_list(self.graph, *list);
            }
            for (_, iter) in self.vertex_type_iter.iter() {
                grin_destroy_vertex_list_iter(self.graph, *iter);
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

/// A proxy for handling an id-only-vertex (the vertex with no properties) in a grin-enabled store.
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
            let grin_adj_list = grin_get_adjacent_list(graph, direction, vertex_handle);
            if grin_adj_list.eq(&GRIN_NULL_LIST) {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_adjacent_list`: {:?}, returns null",
                    vertex_handle
                )));
            }
            if !edge_type_ids.is_empty() {
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
                        grin_select_edge_type_for_adjacent_list(graph, edge_type, grin_adj_list);
                    if grin_typed_adj_list == GRIN_NULL_LIST {
                        return Err(GraphProxyError::QueryStoreError(format!(
                            "`grin_select_edge_type_for_adjacent_list`: {:?}, returns null",
                            edge_type_id
                        )));
                    }
                    let grin_typed_adj_list_iter = grin_get_adjacent_list_begin(graph, grin_typed_adj_list);
                    if grin_typed_adj_list_iter == GRIN_NULL_LIST_ITERATOR {
                        return Err(GraphProxyError::QueryStoreError(
                            "`grin_get_adjacent_list_begin` returns null".to_string(),
                        ));
                    }
                    grin_adj_list_iter_vec.push(grin_typed_adj_list_iter);

                    grin_destroy_adjacent_list(graph, grin_typed_adj_list);
                }
                grin_destroy_adjacent_list(graph, grin_adj_list);
                Ok(Self { graph, grin_adj_list_iter_vec, curr_iter: None })
            } else {
                // empty edge_type_ids means scan all edge types
                let grin_adj_list_iter = grin_get_adjacent_list_begin(graph, grin_adj_list);
                grin_destroy_adjacent_list(graph, grin_adj_list);
                if grin_adj_list_iter == GRIN_NULL_LIST_ITERATOR {
                    return Err(GraphProxyError::QueryStoreError(
                        "`grin_get_adjacent_list_begin` returns null".to_string(),
                    ));
                }
                Ok(Self { graph, grin_adj_list_iter_vec: vec![grin_adj_list_iter], curr_iter: None })
            }
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
            for iter in self.grin_adj_list_iter_vec.iter() {
                grin_destroy_adjacent_list_iter(self.graph, *iter);
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
            let grin_adj_list = grin_get_adjacent_list(graph, direction, vertex_handle);
            if grin_adj_list.eq(&GRIN_NULL_LIST) {
                return Err(GraphProxyError::QueryStoreError(format!(
                    "`grin_get_adjacent_list`: {:?}, returns null",
                    vertex_handle
                )));
            }
            if !edge_type_ids.is_empty() {
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
                        grin_select_edge_type_for_adjacent_list(graph, edge_type, grin_adj_list);
                    if grin_typed_adj_list == GRIN_NULL_LIST {
                        return Err(GraphProxyError::QueryStoreError(format!(
                            "`grin_select_edge_type_for_adjacent_list`: {:?}, returns null",
                            edge_type_id
                        )));
                    }
                    let grin_typed_adj_list_iter = grin_get_adjacent_list_begin(graph, grin_typed_adj_list);
                    if grin_typed_adj_list_iter == GRIN_NULL_LIST_ITERATOR {
                        return Err(GraphProxyError::QueryStoreError(
                            "`grin_get_adjacent_list_begin` returns null".to_string(),
                        ));
                    }
                    grin_adj_list_iter_vec.push(grin_typed_adj_list_iter);

                    grin_destroy_adjacent_list(graph, grin_typed_adj_list);
                }

                grin_destroy_adjacent_list(graph, grin_adj_list);
                Ok(Self { graph, grin_adj_list_iter_vec, curr_iter: None })
            } else {
                // empty edge_type_ids means scan all edge types
                let grin_adj_list_iter = grin_get_adjacent_list_begin(graph, grin_adj_list);
                grin_destroy_adjacent_list(graph, grin_adj_list);
                if grin_adj_list_iter == GRIN_NULL_LIST_ITERATOR {
                    return Err(GraphProxyError::QueryStoreError(
                        "`grin_get_adjacent_list_begin` returns null".to_string(),
                    ));
                }
                Ok(Self { graph, grin_adj_list_iter_vec: vec![grin_adj_list_iter], curr_iter: None })
            }
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
                        // a null vertex handle is unacceptable at the current stage
                        assert_ne!(edge_handle, GRIN_NULL_VERTEX);
                        grin_get_next_adjacent_list_iter(self.graph, *iter);
                        return Some(GrinEdgeProxy::new(self.graph, edge_handle));
                    } else {
                        if let Some(iter_val) = self.grin_adj_list_iter_vec.pop() {
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
            for iter in self.grin_adj_list_iter_vec.iter() {
                grin_destroy_adjacent_list_iter(self.graph, *iter);
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
    /// The edge property table handle, for accessing properties of the `edge`
    prop_table: GrinEdgePropertyTable,
}

impl Drop for GrinEdgeProxy {
    fn drop(&mut self) {
        unsafe {
            println!("drop edge ...");
            grin_destroy_edge_type(self.graph, self.edge_type);
            grin_destroy_edge(self.graph, self.edge);
            grin_destroy_edge_property_table(self.graph, self.prop_table);
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
            let prop_table = grin_get_edge_property_table_by_type(graph, edge_type);
            assert!(!prop_table.is_null());
            Self { graph, edge, edge_type, prop_table }
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

            let result = get_edge_property_as_object(self.graph, self.edge, self.prop_table, prop_handle);
            grin_destroy_edge_property(self.graph, prop_handle);
            result
        }
    }

    pub fn get_properties(&self) -> GraphProxyResult<HashMap<NameOrId, Object>> {
        unsafe {
            let prop_list = grin_get_edge_property_list_by_type(self.graph, self.edge_type);
            if prop_list == GRIN_NULL_LIST {
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

                let prop_value =
                    get_edge_property_as_object(self.graph, self.edge, self.prop_table, prop_handle)?;
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
            let edge_id = FAKE_EDGE_ID;
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

/// A proxy for better handling Grin's graph related operations.
pub struct GrinGraphProxy {
    /// The partitioned graph handle in the current process
    partitioned_graph: GrinPartitionedGraph,
    /// The graph handles managed in the current partition
    graphs: HashMap<GrinPartitionId, GrinGraph>,
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
            if partition_list == GRIN_NULL_LIST {
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

        Ok(GrinGraphProxy { partitioned_graph, graphs })
    }

    pub fn get_partitioned_graph(&self) -> GrinPartitionedGraph {
        self.partitioned_graph
    }

    pub fn get_local_graph(&self, partition_id: GrinPartitionId) -> Option<GrinGraph> {
        self.graphs.get(&partition_id).cloned()
    }

    pub fn get_any_local_graph(&self) -> Option<GrinGraph> {
        self.graphs.values().next().cloned()
    }

    /// Get all vertices in the given partitions and vertex types, with optional some
    /// filtering conditions that may be pushed down to the store (TODO(longbin)).
    /// Return an iterator for scanning the vertices.
    pub fn get_all_vertices(
        &self, partitions: &Vec<GrinPartitionId>, label_ids: &Vec<GrinVertexTypeId>,
        _row_filter: Option<Arc<PEvaluator>>,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = GrinVertexProxy> + Send>> {
        let mut results = Vec::new();
        for partition in partitions {
            if let Some(graph) = self.graphs.get(partition).cloned() {
                let vertex_iter = GrinVertexIter::new(graph, label_ids)?;
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

    pub fn get_vertex_by_index(
        &self, grin_type_id: GrinVertexTypeId, primary_key: &PKV,
    ) -> GraphProxyResult<Option<GrinIdVertexProxy>> {
        unsafe {
            let any_grin_graph = self
                .get_any_local_graph()
                .ok_or(GraphProxyError::QueryStoreError(
                    "No graph is found in the current process".to_string(),
                ))?;

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

            let grin_vertex = grin_get_vertex_by_primary_keys(any_grin_graph, grin_vertex_type, grin_row);
            grin_destroy_row(any_grin_graph, grin_row);

            if grin_vertex == GRIN_NULL_VERTEX {
                Ok(None)
            } else {
                Ok(Some(GrinIdVertexProxy::new(any_grin_graph, grin_vertex).unwrap()))
            }
        }
    }

    pub fn get_neighbor_vertices(
        &self, vertex_id: i64, grin_direction: GrinDirection, edge_type_ids: &Vec<GrinEdgeTypeId>,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = GrinIdVertexProxy> + Send>> {
        let grin_vertex = self.get_vertex_by_id(vertex_id)?;
        let partition_id = self.get_partition_id_by_vertex_id(vertex_id)?;
        if let Some(graph) = self.graphs.get(&partition_id).cloned() {
            let neighbor_vertices_iter =
                GrinAdjVertexIter::new(graph, grin_vertex, grin_direction, edge_type_ids)?;
            Ok(Box::new(neighbor_vertices_iter))
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
        let grin_vertex = self.get_vertex_by_id(vertex_id)?;
        let partition_id = self.get_partition_id_by_vertex_id(vertex_id)?;
        if let Some(graph) = self.graphs.get(&partition_id).cloned() {
            let neighbor_edges_iter =
                GrinAdjEdgeIter::new(graph, grin_vertex, grin_direction, edge_type_ids)?;
            Ok(Box::new(neighbor_edges_iter))
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
            let grin_vertex = self.get_vertex_by_id(vertex_id)?;
            let grin_vertex_proxy = GrinVertexProxy::new(graph, grin_vertex);
            Ok(grin_vertex_proxy)
        } else {
            Err(GraphProxyError::QueryStoreError(format!(
                "Partition {:?} is not found in the current process",
                partition_id
            )))
        }
    }

    fn get_vertex_by_id(&self, vertex_id: i64) -> GraphProxyResult<GrinVertex> {
        unsafe {
            let any_grin_graph = self
                .get_any_local_graph()
                .ok_or(GraphProxyError::QueryStoreError(
                    "No graph is found in the current process".to_string(),
                ))?;

            let vertex_ref = grin_deserialize_int64_to_vertex_ref(any_grin_graph, vertex_id);
            assert_ne!(vertex_ref, GRIN_NULL_VERTEX_REF);
            let vertex = grin_get_vertex_from_vertex_ref(any_grin_graph, vertex_ref);
            assert_ne!(vertex, GRIN_NULL_VERTEX);
            grin_destroy_vertex_ref(any_grin_graph, vertex_ref);
            Ok(vertex)
        }
    }

    fn get_partition_id_by_vertex_id(&self, vertex_id: i64) -> GraphProxyResult<GrinPartitionId> {
        unsafe {
            let any_grin_graph = self
                .get_any_local_graph()
                .ok_or(GraphProxyError::QueryStoreError(
                    "No graph is found in the current process".to_string(),
                ))?;
            let partition = grin_get_master_partition_from_vertex_ref(any_grin_graph, vertex_id);
            let partition_id = grin_get_partition_id(self.partitioned_graph, partition);
            grin_destroy_partition(any_grin_graph, partition);
            Ok(partition_id)
        }
    }
}

unsafe impl Send for GrinGraphProxy {}
unsafe impl Sync for GrinGraphProxy {}

/// An arc wrapper of GrinGraphRuntime to free it from unexpected (double) pointer destory.
pub struct GrinGraphRuntime {
    store: Arc<GrinGraphProxy>,
}

impl From<GrinGraphProxy> for GrinGraphRuntime {
    fn from(graph: GrinGraphProxy) -> Self {
        GrinGraphRuntime { store: Arc::new(graph) }
    }
}

unsafe impl Send for GrinGraphRuntime {}
unsafe impl Sync for GrinGraphRuntime {}

impl ReadGraph for GrinGraphRuntime {
    fn scan_vertex(
        &self, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = RuntimeVertex> + Send>> {
        if let Some(partitions) = params.partitions.as_ref() {
            let store = self.store.clone();
            let label_ids: Vec<GrinVertexTypeId> = params
                .labels
                .iter()
                .map(|label| *label as GrinVertexTypeId)
                .collect();
            let row_filter = params.filter.clone();

            let partitions: Vec<GrinPartitionId> = partitions
                .iter()
                .map(|pid| *pid as GrinPartitionId)
                .collect();

            let columns = params.columns.clone();

            let result_iter = store
                .get_all_vertices(partitions.as_ref(), &label_ids, row_filter)?
                .map(move |graph_proxy| {
                    graph_proxy
                        .into_runtime_vertex(columns.as_ref())
                        .unwrap()
                });

            Ok(sample_limit!(result_iter, params.sample_ratio, params.limit))
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn index_scan_vertex(
        &self, label: LabelId, primary_key: &PKV, _params: &QueryParams,
    ) -> GraphProxyResult<Option<RuntimeVertex>> {
        // TODO(bingqing): confirm partition
        let store = self.store.clone();
        let vertex_type_id = label as GrinVertexTypeId;
        let result = store
            .get_vertex_by_index(vertex_type_id, primary_key)?
            .map(move |vertex_proxy| vertex_proxy.into_runtime_vertex());
        Ok(result)
    }

    fn scan_edge(
        &self, _params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = RuntimeEdge> + Send>> {
        Err(GraphProxyError::unsupported_error("scan_edge() in GrinGraphRuntime"))
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
        // TODO: filter_limit!
        // Ok(filter_limit!(result_iter, row_filter, None))
        Ok(sample_limit!(result_iter, params.sample_ratio, params.limit))
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
        let edge_type_ids: Vec<GrinEdgeTypeId> = params
            .labels
            .iter()
            .map(|label| *label as GrinVertexTypeId)
            .collect();
        let stmt = from_fn(move |vertex_id: ID| match direction {
            Direction::Out => {
                let neighbor_vertex_iter =
                    store.get_neighbor_vertices(vertex_id, GRIN_DIRECTION_OUT, &edge_type_ids)?;
                Ok(Box::new(
                    neighbor_vertex_iter
                        .into_iter()
                        .map(|vertex_proxy| vertex_proxy.into_runtime_vertex()),
                ))
            }
            Direction::In => {
                let neighbor_vertex_iter =
                    store.get_neighbor_vertices(vertex_id, GRIN_DIRECTION_IN, &edge_type_ids)?;
                Ok(Box::new(
                    neighbor_vertex_iter
                        .into_iter()
                        .map(|vertex_proxy| vertex_proxy.into_runtime_vertex()),
                ))
            }
            Direction::Both => {
                let out_neighbor_vertex_iter =
                    store.get_neighbor_vertices(vertex_id, GRIN_DIRECTION_OUT, &edge_type_ids)?;
                let in_neighbor_vertex_iter =
                    store.get_neighbor_vertices(vertex_id, GRIN_DIRECTION_IN, &edge_type_ids)?;
                Ok(Box::new(
                    out_neighbor_vertex_iter
                        .chain(in_neighbor_vertex_iter)
                        .into_iter()
                        .map(|vertex_proxy| vertex_proxy.into_runtime_vertex()),
                ))
            }
        });

        // TODO: filter_limit!
        // TODO: may not reasonable to assume GrinIdVertexProxy; But can also be GrinVertexProxy
        Ok(stmt)
    }

    fn prepare_explore_edge(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, RuntimeEdge>>> {
        let store = self.store.clone();
        let edge_type_ids: Vec<GrinEdgeTypeId> = params
            .labels
            .iter()
            .map(|label| *label as GrinVertexTypeId)
            .collect();
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
                    Ok(Box::new(neighbor_edge_iter))
                }
                Direction::In => {
                    let neighbor_edge_iter = store
                        .get_neighbor_edges(vertex_id, GRIN_DIRECTION_IN, &edge_type_ids)?
                        .map(move |edge_proxy| {
                            edge_proxy
                                .into_runtime_edge(columns.as_ref(), false)
                                .unwrap()
                        });
                    Ok(Box::new(neighbor_edge_iter))
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
                    Ok(Box::new(out_neighbor_edge_iter.chain(in_neighbor_edge_iter)))
                }
            }
        });

        // TODO: filter_limit!
        Ok(stmt)
    }

    fn get_primary_key(&self, _id: &ID) -> GraphProxyResult<Option<PKV>> {
        todo!()
    }
}
