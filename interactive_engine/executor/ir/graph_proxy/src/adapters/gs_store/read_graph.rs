//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use std::convert::TryInto;
use std::fmt;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use dyn_type::{Object, Primitives};
use global_query::store_api::prelude::{Condition, Property};
use global_query::store_api::{
    Edge as StoreEdge, LabelId as StoreLabelId, PartitionId, Vertex as StoreVertex, VertexId,
};
use global_query::store_api::{PropId, SnapshotId};
use global_query::{
    GlobalGraphQuery, GraphPartitionManager, PartitionLabeledVertexIds, PartitionVertexIds,
};
use graph_store::utils::IterList;
use ir_common::{KeyId, LabelId, NameOrId, OneOrMany};
use pegasus_common::downcast::*;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

use crate::apis::graph::PKV;
use crate::apis::{
    from_fn, register_graph, Details, Direction, DynDetails, Edge, PropertyValue, QueryParams, ReadGraph,
    Statement, Vertex, ID,
};
use crate::utils::expr::eval_pred::{EvalPred, PEvaluator};
use crate::{filter_limit, filter_sample_limit, limit_n, sample_limit};
use crate::{GraphProxyError, GraphProxyResult};

// Should be identical to the param_name given by compiler
const SNAPSHOT_ID: &str = "SID";
// This will refer to the latest graph
const DEFAULT_SNAPSHOT_ID: SnapshotId = SnapshotId::max_value() - 1;
// This represents the primary key of GraphScopeStore
const GS_STORE_PK: KeyId = 0;

pub struct GraphScopeStore<V, VI, E, EI>
where
    V: StoreVertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: StoreEdge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    store: Arc<dyn GlobalGraphQuery<V = V, E = E, VI = VI, EI = EI>>,
    partition_manager: Arc<dyn GraphPartitionManager>,
    row_filter_pushdown: bool,
    column_filter_pushdown: bool,
}

#[allow(dead_code)]
pub fn create_gs_store<V, VI, E, EI>(
    store: Arc<dyn GlobalGraphQuery<V = V, E = E, VI = VI, EI = EI>>,
    partition_manager: Arc<dyn GraphPartitionManager>, row_filter_push_down: bool,
    column_filter_push_down: bool,
) where
    V: StoreVertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: StoreEdge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    let graph = GraphScopeStore {
        store,
        partition_manager,
        row_filter_pushdown: row_filter_push_down,
        column_filter_pushdown: column_filter_push_down,
    };
    register_graph(Arc::new(graph));
}

impl<V, VI, E, EI> ReadGraph for GraphScopeStore<V, VI, E, EI>
where
    V: StoreVertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: StoreEdge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    fn scan_vertex(
        &self, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        if let Some(partitions) = params.partitions.as_ref() {
            let store = self.store.clone();
            let si = params
                .get_extra_param(SNAPSHOT_ID)
                .map(|s| {
                    s.parse::<SnapshotId>()
                        .unwrap_or(DEFAULT_SNAPSHOT_ID)
                })
                .unwrap_or(DEFAULT_SNAPSHOT_ID);
            let label_ids = encode_storage_labels(params.labels.as_ref())?;
            let row_filter = params.filter.clone();

            let (condition, row_filter_exists_but_not_pushdown) =
                encode_storage_row_filter_condition(row_filter.as_ref(), self.row_filter_pushdown);

            // props that will be used in futher compute
            let cache_prop_ids = encode_storage_prop_keys(params.columns.as_ref())?;
            // props that will be returned by storage layer
            let prop_ids = if self.column_filter_pushdown {
                if row_filter_exists_but_not_pushdown {
                    // need to call filter_limit!, so get columns in row_filter and params.columns
                    extract_needed_columns(row_filter.as_ref(), cache_prop_ids.as_ref())?
                } else {
                    // row_filter pushdown success, only need params.columns
                    cache_prop_ids.clone()
                }
            } else {
                // now, ir assume that it can get all properties from a vertex/edge locally
                // just column filter is not pushdown.
                get_all_storage_props()
            };

            let partitions: Vec<PartitionId> = partitions
                .iter()
                .map(|pid| *pid as PartitionId)
                .collect();

            let result = store
                .get_all_vertices(
                    si,
                    label_ids.as_ref(),
                    // None means no filter condition pushed down to storage as not supported yet. Same as follows.
                    condition.as_ref(),
                    // None means no need to dedup by properties. Same as follows.
                    None,
                    prop_ids.as_ref(),
                    // Zero limit means no limit. Same as follows.
                    0,
                    // Each worker will scan the partitions pre-allocated in source operator. Same as follows.
                    partitions.as_ref(),
                )
                .map(move |v| to_runtime_vertex(v, cache_prop_ids.clone()));

            if row_filter_exists_but_not_pushdown {
                // fall back to call filter_limit! to do row filter
                Ok(filter_sample_limit!(result, row_filter, params.sample_ratio, params.limit))
            } else {
                Ok(sample_limit!(result, params.sample_ratio, params.limit))
            }
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn index_scan_vertex(
        &self, label_id: LabelId, primary_key: &PKV, _params: &QueryParams,
    ) -> GraphProxyResult<Option<Vertex>> {
        // get_vertex_id_by_primary_keys() is a global query function, that is,
        // you can query vertices (with only vertex id) by pks on any graph partitions (not matter locally or remotely).
        // To guarantee the correctness (i.e., avoid duplication results), only worker 0 is assigned for query.
        if pegasus::get_current_worker().index == 0 {
            let store_label_id = encode_storage_label(label_id)?;
            let store_indexed_values = match primary_key {
                OneOrMany::One(pkv) => {
                    vec![encode_store_prop_val(pkv[0].1.clone())]
                }
                OneOrMany::Many(pkvs) => pkvs
                    .iter()
                    .map(|(_pk, value)| encode_store_prop_val(value.clone()))
                    .collect(),
            };

            if let Some(vid) = self
                .partition_manager
                .get_vertex_id_by_primary_keys(store_label_id, store_indexed_values.as_ref())
            {
                Ok(Some(Vertex::new(vid as ID, Some(label_id.clone()), DynDetails::default())))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn scan_edge(&self, params: &QueryParams) -> GraphProxyResult<Box<dyn Iterator<Item = Edge> + Send>> {
        if let Some(partitions) = params.partitions.as_ref() {
            let store = self.store.clone();
            let si = params
                .get_extra_param(SNAPSHOT_ID)
                .map(|s| {
                    s.parse::<SnapshotId>()
                        .unwrap_or(DEFAULT_SNAPSHOT_ID)
                })
                .unwrap_or(DEFAULT_SNAPSHOT_ID);
            let label_ids = encode_storage_labels(params.labels.as_ref())?;
            let row_filter = params.filter.clone();

            // the same as above
            let (condition, row_filter_exists_but_not_pushdown) =
                encode_storage_row_filter_condition(row_filter.as_ref(), self.row_filter_pushdown);

            let cache_prop_ids = encode_storage_prop_keys(params.columns.as_ref())?;
            let prop_ids = if self.column_filter_pushdown {
                if row_filter_exists_but_not_pushdown {
                    extract_needed_columns(row_filter.as_ref(), cache_prop_ids.as_ref())?
                } else {
                    cache_prop_ids.clone()
                }
            } else {
                get_all_storage_props()
            };

            let partitions: Vec<PartitionId> = partitions
                .iter()
                .map(|pid| *pid as PartitionId)
                .collect();
            let result = store
                .get_all_edges(
                    si,
                    label_ids.as_ref(),
                    condition.as_ref(),
                    None,
                    prop_ids.as_ref(),
                    0,
                    partitions.as_ref(),
                )
                .map(move |e| to_runtime_edge(&e));

            if row_filter_exists_but_not_pushdown {
                Ok(filter_sample_limit!(result, row_filter, params.sample_ratio, params.limit))
            } else {
                Ok(sample_limit!(result, params.sample_ratio, params.limit))
            }
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn get_vertex(
        &self, ids: &[ID], params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        let store = self.store.clone();
        let si = params
            .get_extra_param(SNAPSHOT_ID)
            .map(|s| {
                s.parse::<SnapshotId>()
                    .unwrap_or(DEFAULT_SNAPSHOT_ID)
            })
            .unwrap_or(DEFAULT_SNAPSHOT_ID);

        // props that will be used in futher compute
        let cache_prop_ids = encode_storage_prop_keys(params.columns.as_ref())?;
        // also need props in filter, because `filter_limit!`
        let prop_ids = if self.column_filter_pushdown {
            extract_needed_columns(params.filter.as_ref(), cache_prop_ids.as_ref())?
        } else {
            // column filter not pushdown, ir assume that it can get all props locally
            get_all_storage_props()
        };

        let filter = params.filter.clone();
        let partition_label_vertex_ids =
            get_partition_label_vertex_ids(ids, self.partition_manager.clone());

        let result = store
            .get_vertex_properties(si, partition_label_vertex_ids.clone(), prop_ids.as_ref())
            .map(move |v| to_runtime_vertex(v, cache_prop_ids.clone()));

        Ok(filter_limit!(result, filter, None))
    }

    fn get_edge(
        &self, _ids: &[ID], _params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Edge> + Send>> {
        // TODO(bingqing): adapt get_edge when graphscope support this
        Err(GraphProxyError::query_store_error("GraphScope storage does not support get_edge for now"))?
    }

    fn prepare_explore_vertex(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, Vertex>>> {
        let row_filter = params.filter.clone();

        let (condition, row_filter_exists_but_not_push_down) =
            encode_storage_row_filter_condition(row_filter.as_ref(), self.row_filter_pushdown);

        let limit = params.limit.clone();
        let store = self.store.clone();
        let partition_manager = self.partition_manager.clone();
        let si = params
            .get_extra_param(SNAPSHOT_ID)
            .map(|s| {
                s.parse::<SnapshotId>()
                    .unwrap_or(DEFAULT_SNAPSHOT_ID)
            })
            .unwrap_or(DEFAULT_SNAPSHOT_ID);
        let edge_label_ids = encode_storage_labels(params.labels.as_ref())?;

        let stmt = from_fn(move |v: ID| {
            let src_id = get_partition_vertex_id(v, partition_manager.clone());
            let iter = match direction {
                Direction::Out => store.get_out_vertex_ids(
                    si,
                    vec![src_id],
                    edge_label_ids.as_ref(),
                    condition.as_ref(),
                    None,
                    limit.unwrap_or(0),
                ),
                Direction::In => store.get_in_vertex_ids(
                    si,
                    vec![src_id],
                    edge_label_ids.as_ref(),
                    condition.as_ref(),
                    None,
                    limit.unwrap_or(0),
                ),
                Direction::Both => {
                    let mut iters = vec![];
                    let out_iter = store.get_out_vertex_ids(
                        si,
                        vec![src_id.clone()],
                        edge_label_ids.as_ref(),
                        condition.as_ref(),
                        None,
                        limit.clone().unwrap_or(0),
                    );
                    iters.push(out_iter);
                    let in_iter = store.get_in_vertex_ids(
                        si,
                        vec![src_id],
                        edge_label_ids.as_ref(),
                        condition.as_ref(),
                        None,
                        limit.unwrap_or(0),
                    );
                    iters.push(in_iter);
                    Box::new(IterList::new(iters))
                }
            };
            let iters = iter.map(|(_src, vi)| vi).collect();
            let iter_list = IterList::new(iters).map(move |v| to_empty_vertex(&v));

            if row_filter_exists_but_not_push_down {
                Ok(filter_limit!(iter_list, row_filter, None))
            } else {
                Ok(Box::new(iter_list))
            }
        });
        Ok(stmt)
    }

    fn prepare_explore_edge(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, Edge>>> {
        let store = self.store.clone();
        let si = params
            .get_extra_param(SNAPSHOT_ID)
            .map(|s| {
                s.parse::<SnapshotId>()
                    .unwrap_or(DEFAULT_SNAPSHOT_ID)
            })
            .unwrap_or(DEFAULT_SNAPSHOT_ID);

        let partition_manager = self.partition_manager.clone();
        let row_filter = params.filter.clone();

        // the same as above
        let (condition, row_filter_exists_but_not_pushdown) =
            encode_storage_row_filter_condition(row_filter.as_ref(), self.row_filter_pushdown);

        let cache_prop_ids = encode_storage_prop_keys(params.columns.as_ref())?;
        let prop_ids = if self.column_filter_pushdown {
            if row_filter_exists_but_not_pushdown {
                extract_needed_columns(row_filter.as_ref(), cache_prop_ids.as_ref())?
            } else {
                cache_prop_ids.clone()
            }
        } else {
            get_all_storage_props()
        };

        let limit = params.limit.clone();
        let edge_label_ids = encode_storage_labels(params.labels.as_ref())?;

        let columns = params.columns.clone();

        let stmt = from_fn(move |v: ID| {
            let src_id = get_partition_vertex_id(v, partition_manager.clone());
            let iter = match direction {
                Direction::Out => store.get_out_edges(
                    si,
                    vec![src_id],
                    edge_label_ids.as_ref(),
                    condition.as_ref(),
                    None,
                    prop_ids.as_ref(),
                    limit.unwrap_or(0),
                ),
                Direction::In => store.get_in_edges(
                    si,
                    vec![src_id],
                    edge_label_ids.as_ref(),
                    condition.as_ref(),
                    None,
                    prop_ids.as_ref(),
                    limit.unwrap_or(0),
                ),
                Direction::Both => {
                    let mut iter = vec![];
                    let out_iter = store.get_out_edges(
                        si,
                        vec![src_id.clone()],
                        edge_label_ids.as_ref(),
                        condition.as_ref(),
                        None,
                        prop_ids.as_ref(),
                        limit.clone().unwrap_or(0),
                    );
                    iter.push(out_iter);
                    let in_iter = store.get_in_edges(
                        si,
                        vec![src_id],
                        edge_label_ids.as_ref(),
                        condition.as_ref(),
                        None,
                        prop_ids.as_ref(),
                        limit.unwrap_or(0),
                    );
                    iter.push(in_iter);
                    Box::new(IterList::new(iter))
                }
            };
            let iters = iter.map(|(_src, ei)| ei).collect();
            let iter_list = IterList::new(iters).map(move |e| to_runtime_edge(&e));
            if row_filter_exists_but_not_pushdown {
                let filter = row_filter.clone().unwrap();
                let columns = columns.clone();
                let filter_result = iter_list
                    .filter(move |e| filter.eval_bool(Some(e)).unwrap_or(false))
                    .map(move |e| edge_trim(e, columns.as_ref()));
                Ok(Box::new(filter_result))
            } else {
                Ok(Box::new(iter_list))
            }
        });
        Ok(stmt)
    }

    fn get_primary_key(&self, id: &ID) -> GraphProxyResult<Option<PKV>> {
        let store = self.store.clone();
        let outer_id = store.translate_vertex_id(*id as VertexId);
        let pk_val = Object::from(outer_id);
        Ok(Some((GS_STORE_PK.into(), pk_val).into()))
    }
}

#[inline]
fn to_runtime_vertex<V>(v: V, prop_keys: Option<Vec<PropId>>) -> Vertex
where
    V: 'static + StoreVertex,
{
    let id = v.get_id() as ID;
    let label = encode_runtime_v_label(&v);
    let details = LazyVertexDetails::new(v, prop_keys);
    Vertex::new(id, Some(label), DynDetails::lazy(details))
}

#[inline]
fn to_empty_vertex<V: StoreVertex>(v: &V) -> Vertex {
    let id = v.get_id() as ID;
    let label = encode_runtime_v_label(v);
    Vertex::new(id, Some(label), DynDetails::default())
}

#[inline]
fn to_runtime_edge<E: StoreEdge>(e: &E) -> Edge {
    // TODO: LazyEdgeDetails
    let id = e.get_edge_id() as ID;
    let label = encode_runtime_e_label(e);
    let properties = e
        .get_properties()
        .map(|(prop_id, prop_val)| encode_runtime_property(prop_id, prop_val))
        .collect();
    // TODO: new an edge with with_from_src()
    let mut edge =
        Edge::new(id, Some(label), e.get_src_id() as ID, e.get_dst_id() as ID, DynDetails::new(properties));
    edge.set_src_label(e.get_src_label_id() as LabelId);
    edge.set_dst_label(e.get_dst_label_id() as LabelId);
    edge
}

#[inline]
fn edge_trim(mut edge: Edge, columns: Option<&Vec<NameOrId>>) -> Edge {
    if let Some(columns) = columns {
        if columns.is_empty() {
            // vec![] means all properties are needed, and do nothing
        } else {
            let details = edge.get_details_mut();
            let mut trimmed_details = HashMap::new();
            for column in columns {
                trimmed_details.insert(
                    column.clone(),
                    details
                        .get_property(column)
                        .map(|p| p.try_to_owned())
                        .unwrap_or(None)
                        .unwrap_or(Object::None),
                );
            }
            *details = DynDetails::new(trimmed_details);
        }
    } else {
        // None means no properties are needed
        let details = edge.get_details_mut();
        *details = DynDetails::default();
    }
    edge
}

/// LazyVertexDetails is used for local property fetching optimization.
/// That is, the required properties will not be materialized until LazyVertexDetails need to be shuffled.
#[allow(dead_code)]
pub struct LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    // prop_keys specify the properties we would save for later queries after shuffle,
    // excluding the ones used only when local property fetching.
    // Specifically, in graphscope store, None means we need all properties,
    // and Some(vec![]) means we do not need any property
    prop_keys: Option<Vec<PropId>>,
    inner: AtomicPtr<V>,
}

impl<V> LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    pub fn new(v: V, prop_keys: Option<Vec<PropId>>) -> Self {
        let ptr = Box::into_raw(Box::new(v));
        LazyVertexDetails { prop_keys, inner: AtomicPtr::new(ptr) }
    }

    fn get_vertex_ptr(&self) -> Option<*mut V> {
        let ptr = self.inner.load(Ordering::SeqCst);
        if ptr.is_null() {
            None
        } else {
            Some(ptr)
        }
    }
}

impl<V> fmt::Debug for LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("gs_store LazyVertexDetails")
            .field("properties", &self.prop_keys)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<V> Details for LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    // TODO: consider the situation when push `props` down to groot
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        if let NameOrId::Id(key) = key {
            if let Some(ptr) = self.get_vertex_ptr() {
                unsafe {
                    (*ptr)
                        .get_property(*key as PropId)
                        .map(|prop| PropertyValue::Owned(encode_runtime_prop_val(prop)))
                }
            } else {
                None
            }
        } else {
            info!("Have not support getting property by prop_name in gs_store yet");
            None
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        let mut all_props = HashMap::new();
        if let Some(prop_keys) = self.prop_keys.as_ref() {
            if prop_keys.is_empty() {
                // the case of get_all_properties from vertex;
                if let Some(ptr) = self.get_vertex_ptr() {
                    unsafe {
                        all_props = (*ptr)
                            .get_properties()
                            .map(|(prop_id, prop_val)| encode_runtime_property(prop_id, prop_val))
                            .collect();
                    }
                } else {
                    return None;
                }
            } else {
                let prop_keys = self.prop_keys.as_ref().unwrap();
                // the case of get_all_properties with prop_keys pre-specified
                for key in prop_keys.iter() {
                    let key = NameOrId::Id(*key as KeyId);
                    if let Some(prop) = self.get_property(&key) {
                        all_props.insert(key.clone(), prop.try_to_owned().unwrap());
                    } else {
                        all_props.insert(key.clone(), Object::None);
                    }
                }
            }
        }
        Some(all_props)
    }

    fn insert_property(&mut self, key: NameOrId, _value: Object) {
        if let NameOrId::Id(key) = key {
            if let Some(prop_keys) = self.prop_keys.as_mut() {
                prop_keys.push(key as PropId);
            }
        } else {
            info!("Have not support insert property by prop_name in gs_store yet");
        }
    }
}

impl<V> AsAny for LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<V> Drop for LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    fn drop(&mut self) {
        let ptr = self.inner.load(Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                std::ptr::drop_in_place(ptr);
            }
        }
    }
}

/// In ir, None means we do not need any properties,
/// and Some means we need given properties (and Some(vec![]) means we need all properties)
#[inline]
fn encode_storage_prop_keys(prop_names: Option<&Vec<NameOrId>>) -> GraphProxyResult<Option<Vec<PropId>>> {
    if let Some(prop_names) = prop_names {
        let ids = prop_names
            .iter()
            .map(|prop_key| match prop_key {
                NameOrId::Str(_) => Err(GraphProxyError::FilterPushDownError(
                    "encode storage prop key error, should provide prop_id".to_owned(),
                )),
                NameOrId::Id(prop_id) => Ok(*prop_id as PropId),
            })
            .collect::<Result<Vec<PropId>, _>>()?;
        Ok(Some(ids))
    } else {
        Ok(None)
    }
}

/// convert filter in `QueryParams` to storage `Condition` filter
/// and return a flag that represents row filter exists but not push down
/// caused by `row_filter_pushdown == false` or  convert error
#[inline]
fn encode_storage_row_filter_condition(
    row_filter: Option<&Arc<PEvaluator>>, row_filter_pushdown: bool,
) -> (Option<Condition>, bool) {
    if row_filter_pushdown {
        let condition = if let Some(filter) = row_filter { filter.as_ref().try_into() } else { Ok(None) };
        match condition {
            Ok(cond) => (cond, false),
            Err(e) => {
                error!("convert ir filter to store condition error {}", e);
                (None, row_filter.is_some())
            }
        }
    } else {
        (None, row_filter.is_some())
    }
}

/// get columns used in filter and output
#[inline]
fn extract_needed_columns(
    filter: Option<&Arc<PEvaluator>>, out_columns: Option<&Vec<PropId>>,
) -> GraphProxyResult<Option<Vec<PropId>>> {
    use ahash::HashSet;

    use crate::utils::expr::eval_pred::zip_option_vecs;

    // Some(vec[]) means need all props, so can't merge it with props needed in filter
    if let Some(out_columns) = out_columns {
        if out_columns.is_empty() {
            return Ok(Some(Vec::with_capacity(0)))
        }
    }

    let filter_needed = if let Some(filter) = filter { filter.as_ref().extract_prop_ids() } else { None };
    let columns = zip_option_vecs(filter_needed, out_columns.cloned());
    // remove duplicated prop ids
    Ok(columns.map(|v| {
        v.into_iter()
            .collect::<HashSet<PropId>>()
            .into_iter()
            .collect::<Vec<PropId>>()
    }))
}

/// Some(vec![]) means need all properties
#[inline]
fn get_all_storage_props() -> Option<Vec<PropId>> {
    Some(Vec::with_capacity(0))
}

#[inline]
fn encode_storage_labels(labels: &Vec<LabelId>) -> GraphProxyResult<Vec<StoreLabelId>> {
    labels
        .iter()
        .map(|label| encode_storage_label(*label))
        .collect::<Result<Vec<StoreLabelId>, _>>()
}

#[inline]
fn encode_storage_label(label: LabelId) -> GraphProxyResult<StoreLabelId> {
    Ok(label as StoreLabelId)
}

#[inline]
fn encode_runtime_v_label<V: StoreVertex>(v: &V) -> LabelId {
    v.get_label_id() as LabelId
}

#[inline]
fn encode_runtime_e_label<E: StoreEdge>(e: &E) -> LabelId {
    e.get_label_id() as LabelId
}

#[inline]
fn encode_runtime_property(prop_id: PropId, prop_val: Property) -> (NameOrId, Object) {
    let prop_key = NameOrId::Id(prop_id as KeyId);
    let prop_val = encode_runtime_prop_val(prop_val);
    (prop_key, prop_val)
}

#[inline]
fn encode_runtime_prop_val(prop_val: Property) -> Object {
    match prop_val {
        Property::Bool(b) => b.into(),
        Property::Char(c) => {
            if c <= (i8::MAX as u8) {
                Object::Primitive(Primitives::Byte(c as i8))
            } else {
                Object::Primitive(Primitives::Integer(c as i32))
            }
        }
        Property::Short(s) => Object::Primitive(Primitives::Integer(s as i32)),
        Property::Int(i) => Object::Primitive(Primitives::Integer(i)),
        Property::Long(l) => Object::Primitive(Primitives::Long(l)),
        Property::Float(f) => Object::Primitive(Primitives::Float(f as f64)),
        Property::Double(d) => Object::Primitive(Primitives::Float(d)),
        Property::Bytes(v) => Object::Blob(v.into_boxed_slice()),
        Property::String(s) => Object::String(s),
        _ => unimplemented!(),
    }
}

#[inline]
fn encode_store_prop_val(prop_val: Object) -> Property {
    match prop_val {
        Object::Primitive(p) => match p {
            Primitives::Byte(b) => Property::Char(b as u8),
            Primitives::Integer(i) => Property::Int(i),
            Primitives::Long(i) => Property::Long(i),
            Primitives::ULLong(i) => Property::Long(i as i64),
            Primitives::Float(f) => Property::Double(f),
        },
        Object::String(s) => Property::String(s),
        Object::Vector(vec) => {
            if let Some(probe) = vec.get(0) {
                match probe {
                    Object::Primitive(p) => match p {
                        Primitives::Byte(_) | Primitives::Integer(_) => Property::ListInt(
                            vec.into_iter()
                                .map(|i| i.as_i32().unwrap())
                                .collect(),
                        ),
                        Primitives::Long(_) => Property::ListLong(
                            vec.into_iter()
                                .map(|i| i.as_i64().unwrap())
                                .collect(),
                        ),
                        Primitives::ULLong(_) => Property::ListLong(
                            vec.into_iter()
                                .map(|i| i.as_u128().unwrap() as i64)
                                .collect(),
                        ),
                        Primitives::Float(_) => Property::ListDouble(
                            vec.into_iter()
                                .map(|i| i.as_f64().unwrap())
                                .collect(),
                        ),
                    },
                    Object::String(_) => Property::ListString(
                        vec.into_iter()
                            .map(|i| i.as_str().unwrap().into_owned())
                            .collect(),
                    ),
                    Object::Blob(_) => Property::ListBytes(
                        vec.into_iter()
                            .map(|i| i.as_bytes().unwrap().to_vec())
                            .collect(),
                    ),
                    Object::None => Property::Null,
                    _ => Property::Unknown,
                }
            } else {
                Property::Null
            }
        }
        Object::Blob(b) => Property::Bytes(b.to_vec()),
        Object::None => Property::Null,
        _ => Property::Unknown,
    }
}

/// Transform type of ids to PartitionLabeledVertexIds as required by graphscope store,
/// which consists of (PartitionId, Vec<(Option<StoreLabelId>, Vec<VertexId>)>)
fn get_partition_label_vertex_ids(
    ids: &[ID], graph_partition_manager: Arc<dyn GraphPartitionManager>,
) -> Vec<PartitionLabeledVertexIds> {
    let mut partition_label_vid_map = HashMap::new();
    for vid in ids {
        let partition_id = graph_partition_manager.get_partition_id(*vid as VertexId) as PartitionId;
        let label_vid_list = partition_label_vid_map
            .entry(partition_id)
            .or_insert(HashMap::new());
        label_vid_list
            .entry(None)
            .or_insert(vec![])
            .push(*vid as VertexId);
    }

    partition_label_vid_map
        .into_iter()
        .map(|(pid, label_vid_map)| (pid, label_vid_map.into_iter().collect()))
        .collect()
}

/// Transform type of ids to PartitionVertexIds as required by graphscope store,
/// which consists of (PartitionId,Vec<VertexId>)
fn get_partition_vertex_id(
    id: ID, graph_partition_manager: Arc<dyn GraphPartitionManager>,
) -> PartitionVertexIds {
    let partition_id = graph_partition_manager.get_partition_id(id as VertexId) as PartitionId;
    (partition_id, vec![id as VertexId])
}
