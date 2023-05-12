use std::ffi::{c_char, CStr, CString};
use std::fmt;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use dyn_type::Object;
use graph_proxy::apis::graph::ID;
use graph_proxy::apis::{DynDetails, ReadGraph, Vertex as RuntimeVertex};
use graph_proxy::utils::expr::eval_pred::PEvaluator;
use graph_proxy::{GraphProxyError, GraphProxyResult};
use ir_common::{LabelId, NameOrId};

use crate::grin_details::LazyVertexDetails;
use crate::grin_v6d::*;
use crate::native_utils::*;

/// Get value of certain type of vertex property from the grin-enabled graph, and turn it into
/// `Object` accessible to the runtime.
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

/// Get value of certain type of edge property from the grin-enabled graph, and turn it into
/// `Object` accessible to the runtime.
#[inline]
fn get_edge_property_as_object(
    graph: GrinGraph, edge: GrinEdge, prop_table: GrinEdgePropertyTable, prop_handle: GrinEdgeProperty,
) -> GraphProxyResult<Object> {
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
    /// The decoded identifier of the `vertex`, currently obtained via `grin_get_vertex_ref_by_vertex()`
    vertex_id: i64,
    /// The vertex type identifier of the `vertex`
    vertex_type_id: GrinVertexTypeId,
}

impl Drop for GrinVertexProxy {
    fn drop(&mut self) {
        unsafe {
            println!("drop vertex: {:?}", self.vertex_id);
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
    pub fn new(graph: GrinGraph, vertex: GrinVertex) -> Self {
        unsafe {
            let vertex_type = grin_get_vertex_type(graph, vertex);
            // A vertex must have a type
            assert_ne!(vertex_type, GRIN_NULL_VERTEX_TYPE);
            let vertex_type_id = grin_get_vertex_type_id(graph, vertex_type);

            let prop_table = grin_get_vertex_property_table_by_type(graph, vertex_type);
            assert!(!prop_table.is_null());

            let vertex_ref = grin_get_vertex_ref_by_vertex(graph, vertex);
            assert_ne!(vertex_ref, GRIN_NULL_VERTEX_REF);

            let vertex_id = grin_serialize_vertex_ref_as_int64(graph, vertex_ref);
            grin_destroy_vertex_ref(graph, vertex_ref);

            GrinVertexProxy { graph, vertex, vertex_type, prop_table, vertex_id, vertex_type_id }
        }
    }

    pub fn get_id(&self) -> i64 {
        self.vertex_id
    }

    pub fn get_label_id(&self) -> GrinVertexTypeId {
        self.vertex_type_id
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

            result
        }
    }

    /// Get all properties of the vertex
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

    /// Turn the `VertexProxy` into a `RuntimeVertex` processed by GIE's runtime, for lazily
    /// accessing the data of the vertex, including identifier, label, and properties.
    /// The `prop_keys` parameter is used to specify which properties to be loaded lazily.
    #[inline]
    pub fn into_runtime_vertex(self, prop_keys: Option<&Vec<NameOrId>>) -> RuntimeVertex {
        let id = self.vertex_id as ID;
        let label = Some(self.vertex_type_id as LabelId);
        let details = LazyVertexDetails::new(self, prop_keys.cloned());
        RuntimeVertex::new(id, label, DynDetails::lazy(details))
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
        } else {  // empty vertex_type_iter means scan all vertex types
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
}

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
        &self, params: &graph_proxy::apis::QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = RuntimeVertex> + Send>> {
        if let Some(partitions) = params.partitions.as_ref() {
            let store = self.store.clone();
            /*
            let si = params
                .get_extra_param(SNAPSHOT_ID)
                .map(|s| {
                    s.parse::<SnapshotId>()
                        .unwrap_or(DEFAULT_SNAPSHOT_ID)
                })
                .unwrap_or(DEFAULT_SNAPSHOT_ID);
            */
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
                .map(move |graph_proxy| graph_proxy.into_runtime_vertex(columns.as_ref()));

            Ok(sample_limit!(result_iter, params.sample_ratio, params.limit))
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn index_scan_vertex(
        &self, label: LabelId, primary_key: &graph_proxy::apis::graph::PKV,
        params: &graph_proxy::apis::QueryParams,
    ) -> GraphProxyResult<Option<RuntimeVertex>> {
        todo!()
    }

    fn scan_edge(
        &self, params: &graph_proxy::apis::QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = graph_proxy::apis::Edge> + Send>> {
        todo!()
    }

    fn get_vertex(
        &self, ids: &[ID], params: &graph_proxy::apis::QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = RuntimeVertex> + Send>> {
        todo!()
    }

    fn get_edge(
        &self, ids: &[ID], params: &graph_proxy::apis::QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = graph_proxy::apis::Edge> + Send>> {
        todo!()
    }

    fn prepare_explore_vertex(
        &self, direction: graph_proxy::apis::Direction, params: &graph_proxy::apis::QueryParams,
    ) -> GraphProxyResult<Box<dyn graph_proxy::apis::Statement<ID, RuntimeVertex>>> {
        todo!()
    }

    fn prepare_explore_edge(
        &self, direction: graph_proxy::apis::Direction, params: &graph_proxy::apis::QueryParams,
    ) -> GraphProxyResult<Box<dyn graph_proxy::apis::Statement<ID, graph_proxy::apis::Edge>>> {
        todo!()
    }

    fn get_primary_key(&self, id: &ID) -> GraphProxyResult<Option<graph_proxy::apis::graph::PKV>> {
        todo!()
    }
}
