//
//! Copyright 2020 Alibaba Group Holding Limited.
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

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::{TryFrom, TryInto};
use std::iter::FromIterator;

use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use vec_map::VecMap;

use crate::catalogue::canonical_labeling::CanonicalLabelManager;
use crate::catalogue::codec::{Cipher, Encoder};
use crate::catalogue::extend_step::{
    get_subsets, limit_repeated_element_num, DefiniteExtendEdge, DefiniteExtendStep, ExtendEdge, ExtendStep,
};
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::{DynIter, PatternDirection, PatternId, PatternLabelId};
use crate::error::{IrError, IrResult};
use crate::plan::meta::TagId;

#[derive(Debug, Clone, Copy)]
pub struct PatternVertex {
    id: PatternId,
    label: PatternLabelId,
}

impl PatternVertex {
    pub fn new(id: PatternId, label: PatternLabelId) -> Self {
        PatternVertex { id, label }
    }

    #[inline]
    pub fn get_id(&self) -> PatternId {
        self.id
    }

    #[inline]
    pub fn get_label(&self) -> PatternLabelId {
        self.label
    }
}

#[derive(Debug, Clone, Default)]
struct PatternVertexData {
    group: usize,
    rank: usize,
    out_adjacencies: Vec<Adjacency>,
    in_adjacencies: Vec<Adjacency>,
    tag: Option<TagId>,
    predicate: Option<common_pb::Expression>,
}

#[derive(Debug, Clone)]
pub struct PatternEdge {
    id: PatternId,
    label: PatternLabelId,
    start_vertex: PatternVertex,
    end_vertex: PatternVertex,
}

impl PatternEdge {
    pub fn new(
        id: PatternId, label: PatternLabelId, start_vertex: PatternVertex, end_vertex: PatternVertex,
    ) -> PatternEdge {
        PatternEdge { id, label, start_vertex, end_vertex }
    }

    #[inline]
    pub fn get_id(&self) -> PatternId {
        self.id
    }

    #[inline]
    pub fn get_label(&self) -> PatternLabelId {
        self.label
    }

    #[inline]
    pub fn get_start_vertex(&self) -> PatternVertex {
        self.start_vertex
    }

    #[inline]
    pub fn get_end_vertex(&self) -> PatternVertex {
        self.end_vertex
    }
}

#[derive(Debug, Clone, Default)]
struct PatternEdgeData {
    rank: usize,
    tag: Option<TagId>,
    predicate: Option<common_pb::Expression>,
}

#[derive(Debug, Clone, Copy)]
pub struct Adjacency {
    /// the source vertex connect to the adjacent vertex through this edge
    edge_id: PatternId,
    /// connecting edge's label
    edge_label: PatternLabelId,
    /// the adjacent vertex
    adj_vertex: PatternVertex,
    /// the connecting direction: outgoing or incoming
    direction: PatternDirection,
}

impl Adjacency {
    pub fn new(
        edge_id: PatternId, edge_label: PatternLabelId, adj_vertex: PatternVertex,
        direction: PatternDirection,
    ) -> Adjacency {
        Adjacency { edge_id, edge_label, adj_vertex, direction }
    }

    fn new_by_src_vertex_and_edge(src_vertex: &PatternVertex, edge: &PatternEdge) -> Option<Adjacency> {
        let start_vertex = edge.get_start_vertex();
        let end_vertex = edge.get_end_vertex();
        if (src_vertex.id, src_vertex.label) == (start_vertex.id, start_vertex.label) {
            Some(Adjacency {
                edge_id: edge.get_id(),
                edge_label: edge.get_label(),
                adj_vertex: edge.get_end_vertex(),
                direction: PatternDirection::Out,
            })
        } else if (src_vertex.id, src_vertex.label) == (end_vertex.id, end_vertex.label) {
            Some(Adjacency {
                edge_id: edge.get_id(),
                edge_label: edge.get_label(),
                adj_vertex: edge.get_start_vertex(),
                direction: PatternDirection::In,
            })
        } else {
            None
        }
    }

    #[inline]
    pub fn get_edge_id(&self) -> PatternId {
        self.edge_id
    }

    #[inline]
    pub fn get_edge_label(&self) -> PatternLabelId {
        self.edge_label
    }

    #[inline]
    pub fn get_adj_vertex(&self) -> PatternVertex {
        self.adj_vertex
    }

    #[inline]
    pub fn get_direction(&self) -> PatternDirection {
        self.direction
    }
}

#[derive(Debug, Clone, Default)]
pub struct Pattern {
    /// Key: edge id, Value: struct PatternEdge
    edges: VecMap<PatternEdge>,
    /// Key: vertex id, Value: the vertex in the pattern
    vertices: VecMap<PatternVertex>,
    /// Key: edge id, Value: struct PatternEdgeData
    /// - store data attaching to PatternEdge
    edges_data: VecMap<PatternEdgeData>,
    /// Key: vertex id, Value: struct PatternVertexData
    /// - store data attaching to PatternVertex
    vertices_data: VecMap<PatternVertexData>,
    /// Key: edge rank, Value: edge id
    /// - use an edge rank to locate an Edge
    rank_edge_map: VecMap<PatternId>,
    /// Key: vertex rank, Value: vertex id
    /// - use a vertex rank to locate a Vertex
    rank_vertex_map: VecMap<PatternId>,
    /// Key: edge's Tag info, Value: edge id
    /// - use a Tag to locate an Edge
    tag_edge_map: BTreeMap<TagId, PatternId>,
    /// Key: vertex's Tag info, Value: vertex id
    /// - use a Tag to locate a vertex
    tag_vertex_map: BTreeMap<TagId, PatternId>,
}

/// Initialze a Pattern from just a single Pattern Vertex
impl From<PatternVertex> for Pattern {
    fn from(vertex: PatternVertex) -> Pattern {
        Pattern {
            edges: VecMap::new(),
            vertices: VecMap::from_iter([(vertex.id, vertex)]),
            edges_data: VecMap::new(),
            vertices_data: VecMap::from_iter([(vertex.id, PatternVertexData::default())]),
            rank_edge_map: VecMap::new(),
            rank_vertex_map: VecMap::from_iter([(0, vertex.id)]),
            tag_edge_map: BTreeMap::new(),
            tag_vertex_map: BTreeMap::new(),
        }
    }
}

/// Initialize a Pattern from a vertor of Pattern Edges
impl TryFrom<Vec<PatternEdge>> for Pattern {
    type Error = IrError;

    fn try_from(edges: Vec<PatternEdge>) -> IrResult<Pattern> {
        if !edges.is_empty() {
            let mut new_pattern = Pattern::default();
            for edge in edges {
                // Add the new Pattern Edge to the new Pattern
                new_pattern
                    .edges
                    .insert(edge.get_id(), edge.clone());
                new_pattern
                    .edges_data
                    .insert(edge.get_id(), PatternEdgeData::default());
                // Add or update the start vertex to the new Pattern
                let start_vertex = new_pattern
                    .vertices
                    .entry(edge.get_start_vertex().get_id())
                    .or_insert(edge.get_start_vertex());
                // Update start vertex's outgoing info
                new_pattern
                    .vertices_data
                    .entry(start_vertex.get_id())
                    .or_insert(PatternVertexData::default())
                    .out_adjacencies
                    .push(Adjacency::new_by_src_vertex_and_edge(start_vertex, &edge).unwrap());
                // Add or update the end vertex to the new Pattern
                let end_vertex = new_pattern
                    .vertices
                    .entry(edge.get_end_vertex().get_id())
                    .or_insert(edge.get_end_vertex());
                // Update end vertex's incoming info
                new_pattern
                    .vertices_data
                    .entry(end_vertex.get_id())
                    .or_insert(PatternVertexData::default())
                    .in_adjacencies
                    .push(Adjacency::new_by_src_vertex_and_edge(end_vertex, &edge).unwrap());
            }
            new_pattern.canonical_labeling();
            Ok(new_pattern)
        } else {
            Err(IrError::InvalidPattern("Empty pattern".to_string()))
        }
    }
}

/// Initialize a Pattern from a protobuf Pattern
impl Pattern {
    pub fn from_pb_pattern(pb_pattern: &pb::Pattern, pattern_meta: &PatternMeta) -> IrResult<Pattern> {
        use pb::pattern::binder::Item as BinderItem;
        // next vertex id assign to the vertex picked from the pb pattern
        let mut next_vertex_id = 0;
        // next edge id assign to the edge picked from the pb pattern
        let mut next_edge_id = 0;
        // pattern edges picked from the pb pattern
        let mut pattern_edges = vec![];
        // record the vertices from the pb pattern having tags
        let mut tag_v_id_map: BTreeMap<TagId, PatternId> = BTreeMap::new();
        // record the label for each vertex from the pb pattern
        let mut id_label_map: BTreeMap<PatternId, PatternLabelId> = BTreeMap::new();
        // record the vertices from the pb pattern has predicates
        let mut v_id_predicate_map: BTreeMap<PatternId, common_pb::Expression> = BTreeMap::new();
        // record the edges from the pb pattern has predicates
        let mut e_id_predicate_map: BTreeMap<PatternId, common_pb::Expression> = BTreeMap::new();
        for sentence in &pb_pattern.sentences {
            if sentence.binders.is_empty() {
                return Err(IrError::MissingData("pb::Pattern::Sentence::binders".to_string()));
            }
            // pb pattern sentence must have start tag
            let start_tag = get_tag_from_name_or_id(
                sentence
                    .start
                    .clone()
                    .ok_or(IrError::MissingData("pb::Pattern::Sentence::start".to_string()))?,
            )?;
            // assgin a vertex id to the start vertex of a pb pattern sentence
            let start_tag_v_id = assign_vertex_id_by_tag(start_tag, &mut tag_v_id_map, &mut next_vertex_id);
            // check whether the start tag label is already determined or not
            let start_tag_label = id_label_map.get(&start_tag_v_id).cloned();
            // it is allowed that the pb pattern sentence doesn't have an end tag
            let end_tag = if let Some(name_or_id) = sentence.end.clone() {
                Some(get_tag_from_name_or_id(name_or_id)?)
            } else {
                None
            };
            // if the end tag exists, assign the end vertex with an id
            let end_tag_v_id = if let Some(tag) = end_tag {
                Some(assign_vertex_id_by_tag(tag, &mut tag_v_id_map, &mut next_vertex_id))
            } else {
                None
            };
            // check the end tag label is already determined or not
            let end_tag_label = end_tag_v_id.and_then(|v_id| id_label_map.get(&v_id).cloned());
            // record previous pattern edge's destinated vertex's id
            // init as start vertex's id
            let mut pre_dst_vertex_id: PatternId = start_tag_v_id;
            // record previous pattern edge's destinated vertex's label
            // init as start vertex's label
            let mut pre_dst_vertex_label = start_tag_label;
            // find the first edge expand's index and last edge expand's index;
            let last_expand_index = get_sentence_last_expand_index(sentence);
            // iterate over the binders
            for (i, binder) in sentence.binders.iter().enumerate() {
                if let Some(BinderItem::Edge(edge_expand)) = binder.item.as_ref() {
                    // get edge label's id
                    let edge_label = get_edge_expand_label(edge_expand)?;
                    // assign the new pattern edge with a new id
                    let edge_id = assign_id(&mut next_edge_id);
                    // get edge direction
                    let edge_direction = unsafe {
                        std::mem::transmute::<i32, pb::edge_expand::Direction>(edge_expand.direction)
                    };
                    // add edge predicate
                    if let Some(expr) = get_edge_expand_predicate(edge_expand) {
                        e_id_predicate_map.insert(edge_id, expr.clone());
                    }
                    // assign/pick the souce vertex id and destination vertex id of the pattern edge
                    let src_vertex_id = pre_dst_vertex_id;
                    let dst_vertex_id = assign_expand_dst_vertex_id(
                        i == last_expand_index.unwrap(),
                        end_tag_v_id,
                        edge_expand,
                        &mut tag_v_id_map,
                        &mut next_vertex_id,
                    )?;
                    pre_dst_vertex_id = dst_vertex_id;
                    // assign vertices labels
                    // check which label candidate can connect to the previous determined partial pattern
                    let required_src_vertex_label = pre_dst_vertex_label;
                    let required_dst_vertex_label =
                        if i == last_expand_index.unwrap() { end_tag_label } else { None };
                    // check whether we find proper src vertex label and dst vertex label
                    let (src_vertex_label, dst_vertex_label, direction) = assign_src_dst_vertex_labels(
                        pattern_meta,
                        edge_label,
                        edge_direction,
                        required_src_vertex_label,
                        required_dst_vertex_label,
                    )?;
                    id_label_map.insert(src_vertex_id, src_vertex_label);
                    id_label_map.insert(dst_vertex_id, dst_vertex_label);
                    // generate the new pattern edge and add to the pattern_edges_collection
                    let new_pattern_edge = create_new_pattern_edge(
                        edge_id,
                        edge_label,
                        src_vertex_id,
                        dst_vertex_id,
                        src_vertex_label,
                        dst_vertex_label,
                        direction,
                    );
                    pattern_edges.push(new_pattern_edge);
                    pre_dst_vertex_label = Some(dst_vertex_label);
                } else if let Some(BinderItem::Select(select)) = binder.item.as_ref() {
                    if let Some(predicate) = select.predicate.as_ref() {
                        v_id_predicate_map.insert(pre_dst_vertex_id, predicate.clone());
                    }
                } else {
                    return Err(IrError::MissingData("pb::pattern::binder::Item".to_string()));
                }
            }
        }

        Pattern::try_from(pattern_edges).and_then(|mut pattern| {
            for (tag, v_id) in tag_v_id_map {
                pattern.set_vertex_tag(v_id, tag);
            }
            for (v_id, predicate) in v_id_predicate_map {
                pattern.set_vertex_predicate(v_id, predicate);
            }
            for (e_id, predicate) in e_id_predicate_map {
                pattern.set_edge_predicate(e_id, predicate);
            }
            Ok(pattern)
        })
    }
}

/// Get the tag info from the given name_or_id
/// - in pb::Pattern transformation, tag is required to be id instead of str
fn get_tag_from_name_or_id(name_or_id: common_pb::NameOrId) -> IrResult<TagId> {
    let tag: ir_common::NameOrId = name_or_id.try_into()?;
    match tag {
        ir_common::NameOrId::Id(tag_id) => Ok(tag_id as TagId),
        _ => Err(IrError::TagNotExist(tag)),
    }
}

/// Get the last edge expand's index of a pb pattern sentence among all of its binders
fn get_sentence_last_expand_index(sentence: &pb::pattern::Sentence) -> Option<usize> {
    match sentence
        .binders
        .iter()
        .enumerate()
        .rev()
        .filter(|(_, binder)| {
            if let Some(pb::pattern::binder::Item::Edge(_)) = binder.item.as_ref() {
                true
            } else {
                false
            }
        })
        .next()
    {
        Some((id, _)) => Some(id),
        _ => None,
    }
}

/// Get the edge expand's label
/// - in current realization, edge_expand only allows to have one label
/// - if it has no label or more than one label, give Error
fn get_edge_expand_label(edge_expand: &pb::EdgeExpand) -> IrResult<PatternLabelId> {
    if edge_expand.is_edge {
        return Err(IrError::Unsupported("Expand only edge is not supported".to_string()));
    }
    if let Some(params) = edge_expand.params.as_ref() {
        // TODO: Support Fuzzy Pattern
        if params.tables.is_empty() {
            return Err(IrError::Unsupported("FuzzyPattern: no specific edge expand label".to_string()));
        } else if params.tables.len() > 1 {
            return Err(IrError::Unsupported("FuzzyPattern: more than 1 edge expand label".to_string()));
        }
        // get edge label's id
        match params.tables[0].item.as_ref() {
            Some(common_pb::name_or_id::Item::Id(e_label_id)) => Ok(*e_label_id),
            _ => Err(IrError::InvalidPattern("edge expand doesn't have valid label".to_string())),
        }
    } else {
        Err(IrError::MissingData("pb::EdgeExpand.params".to_string()))
    }
}

/// Get the predicate(if it has) of the edge expand
fn get_edge_expand_predicate(edge_expand: &pb::EdgeExpand) -> Option<common_pb::Expression> {
    if let Some(params) = edge_expand.params.as_ref() {
        params.predicate.clone()
    } else {
        None
    }
}

/// Assign a vertex or edge with the next_id, and add the next_id by one
fn assign_id(next_id: &mut PatternId) -> PatternId {
    let id_to_assign = *next_id;
    *next_id += 1;
    id_to_assign
}

/// If the current vertex has tag, check whether the tag is related to an id or not
/// - if it is related, just use the tag's id
/// - else, assign the vertex with a new id and add the (tag, id) relation to the map
fn assign_vertex_id_by_tag(
    vertex_tag: TagId, tag_v_id_map: &mut BTreeMap<TagId, PatternId>, next_vertex_id: &mut PatternId,
) -> PatternId {
    if let Some(v_id) = tag_v_id_map.get(&vertex_tag) {
        *v_id
    } else {
        let id_to_assign = assign_id(next_vertex_id);
        tag_v_id_map.insert(vertex_tag, id_to_assign);
        id_to_assign
    }
}

/// Assign an id the dst vertex of an edge expand
/// - firstly, check whether the edge expand is the tail of the sentence or not
///   - if it is sentence's end vertex
///     - if the sentence's end vertex's id is already assigned, just use it
///     - else, assign it with a new id
///   - else
///     - if the dst vertex is related with the tag, assign its id by tag
///     - else, assign it with a new id
fn assign_expand_dst_vertex_id(
    is_tail: bool, sentence_end_id: Option<PatternId>, edge_expand: &pb::EdgeExpand,
    tag_v_id_map: &mut BTreeMap<TagId, PatternId>, next_vertex_id: &mut PatternId,
) -> IrResult<PatternId> {
    if is_tail {
        if let Some(v_id) = sentence_end_id {
            Ok(v_id)
        } else {
            Ok(assign_id(next_vertex_id))
        }
    } else {
        // check alias tag
        let dst_vertex_tag = if let Some(name_or_id) = edge_expand.alias.clone() {
            Some(get_tag_from_name_or_id(name_or_id)?)
        } else {
            None
        };
        if let Some(tag) = dst_vertex_tag {
            Ok(assign_vertex_id_by_tag(tag, tag_v_id_map, next_vertex_id))
        } else {
            Ok(assign_id(next_vertex_id))
        }
    }
}

/// Based on the vertex labels candidates and required src/dst vertex label,
/// assign the src and dst vertex with vertex labels meeting the requirement
fn assign_src_dst_vertex_labels(
    pattern_meta: &PatternMeta, edge_label: PatternLabelId, edge_direction: pb::edge_expand::Direction,
    required_src_label: Option<PatternLabelId>, required_dst_label: Option<PatternLabelId>,
) -> IrResult<(PatternLabelId, PatternLabelId, PatternDirection)> {
    // Based on the pattern meta info, find all possible vertex labels candidates:
    // (src vertex label, dst vertex label) with the given edge label and edge direction
    // - if the edge direciton is Outgoting:
    //   we use (start vertex label, end vertex label) as (src vertex label, dst vertex label)
    // - if the edge direction is Incoming:
    //   we use (end vertex label, start vertex label) as (src vertex label, dst vertex label)
    // - if the edge direction is Both:
    //   we connect the iterators returned by Outgoing and Incoming together as they all can be the candidates
    let mut vertex_labels_candis: DynIter<(PatternLabelId, PatternLabelId, PatternDirection)> =
        match edge_direction {
            pb::edge_expand::Direction::Out => Box::new(
                pattern_meta
                    .associated_vlabels_iter_by_elabel(edge_label)
                    .map(|(start_v_label, end_v_label)| {
                        (start_v_label, end_v_label, PatternDirection::Out)
                    }),
            ),
            pb::edge_expand::Direction::In => Box::new(
                pattern_meta
                    .associated_vlabels_iter_by_elabel(edge_label)
                    .map(|(start_v_label, end_v_label)| (end_v_label, start_v_label, PatternDirection::In)),
            ),
            pb::edge_expand::Direction::Both => Box::new(
                pattern_meta
                    .associated_vlabels_iter_by_elabel(edge_label)
                    .map(|(start_v_label, end_v_label)| (start_v_label, end_v_label, PatternDirection::Out))
                    .chain(
                        pattern_meta
                            .associated_vlabels_iter_by_elabel(edge_label)
                            .map(|(start_v_label, end_v_label)| {
                                (end_v_label, start_v_label, PatternDirection::In)
                            }),
                    ),
            ),
        };
    // For a chosen candidates:
    // - if the required src label is some, its src vertex label must match the requirement
    // - if the required dst label is some, its dst vertex label must match the requirement
    (if let (Some(src_label), Some(dst_label)) = (required_src_label, required_dst_label) {
        vertex_labels_candis
            .filter(|&(src_label_candi, dst_label_candi, _)| {
                src_label_candi == src_label && dst_label_candi == dst_label
            })
            .next()
    } else if let Some(src_label) = required_src_label {
        vertex_labels_candis
            .filter(|&(src_label_candi, _, _)| src_label_candi == src_label)
            .next()
    } else if let Some(dst_label) = required_dst_label {
        vertex_labels_candis
            .filter(|&(_, dst_label_candi, _)| dst_label_candi == dst_label)
            .next()
    } else {
        vertex_labels_candis.next()
    })
    .ok_or(IrError::InvalidPattern("Cannot find valid label for some vertices".to_string()))
}

/// Generate a new pattern edge based on the given info
fn create_new_pattern_edge(
    edge_id: PatternId, edge_label: PatternLabelId, src_vertex_id: PatternId, dst_vertex_id: PatternId,
    src_vertex_label: PatternLabelId, dst_vertex_label: PatternLabelId, direction: PatternDirection,
) -> PatternEdge {
    let mut start_vertex = PatternVertex::new(src_vertex_id, src_vertex_label);
    let mut end_vertex = PatternVertex::new(dst_vertex_id, dst_vertex_label);
    if let PatternDirection::In = direction {
        std::mem::swap(&mut start_vertex, &mut end_vertex);
    }
    PatternEdge::new(edge_id, edge_label, start_vertex, end_vertex)
}

/// Getters of fields of Pattern
impl Pattern {
    /// Get a PatternEdge struct from an edge id
    #[inline]
    pub fn get_edge(&self, edge_id: PatternId) -> Option<&PatternEdge> {
        self.edges.get(edge_id)
    }

    /// Get PatternEdge from Given Edge Rank
    #[inline]
    pub fn get_edge_from_rank(&self, edge_rank: usize) -> Option<&PatternEdge> {
        self.rank_edge_map
            .get(edge_rank)
            .and_then(|&edge_id| self.get_edge(edge_id))
    }

    /// Get PatternEdge from Given Tag
    #[inline]
    pub fn get_edge_from_tag(&self, edge_tag: TagId) -> Option<&PatternEdge> {
        self.tag_edge_map
            .get(&edge_tag)
            .and_then(|&edge_id| self.get_edge(edge_id))
    }

    /// Get the total number of edges in the pattern
    #[inline]
    pub fn get_edges_num(&self) -> usize {
        self.edges.len()
    }

    /// Get the minimum edge label id of the current pattern
    #[inline]
    pub fn get_min_edge_label(&self) -> Option<PatternLabelId> {
        self.edges
            .iter()
            .map(|(_, edge)| edge.get_label())
            .min()
    }

    /// Get the maximum edge label id of the current pattern
    #[inline]
    pub fn get_max_edge_label(&self) -> Option<PatternLabelId> {
        self.edges
            .iter()
            .map(|(_, edge)| edge.get_label())
            .max()
    }

    /// Get a PatternEdge's Rank info
    #[inline]
    pub fn get_edge_rank(&self, edge_id: PatternId) -> Option<usize> {
        self.edges_data
            .get(edge_id)
            .map(|edge_data| edge_data.rank)
    }

    /// Get a PatternEdge's Tag info
    #[inline]
    pub fn get_edge_tag(&self, edge_id: PatternId) -> Option<TagId> {
        self.edges_data
            .get(edge_id)
            .and_then(|edge_data| edge_data.tag)
    }

    /// Get the predicate requirement of a PatternEdge
    #[inline]
    pub fn get_edge_predicate(&self, edge_id: PatternId) -> Option<&common_pb::Expression> {
        self.edges_data
            .get(edge_id)
            .and_then(|edge_data| edge_data.predicate.as_ref())
    }

    /// Get a PatternVertex struct from a vertex id
    #[inline]
    pub fn get_vertex(&self, vertex_id: PatternId) -> Option<&PatternVertex> {
        self.vertices.get(vertex_id)
    }

    /// Get PatternVertex Reference from Given Rank
    #[inline]
    pub fn get_vertex_from_rank(&self, vertex_rank: usize) -> Option<&PatternVertex> {
        self.rank_vertex_map
            .get(vertex_rank)
            .and_then(|&vertex_id| self.get_vertex(vertex_id))
    }

    /// Get PatternVertex Reference from Given Tag
    #[inline]
    pub fn get_vertex_from_tag(&self, vertex_tag: TagId) -> Option<&PatternVertex> {
        self.tag_vertex_map
            .get(&vertex_tag)
            .and_then(|&vertex_id| self.get_vertex(vertex_id))
    }

    /// Get the total number of vertices in the pattern
    #[inline]
    pub fn get_vertices_num(&self) -> usize {
        self.vertices.len()
    }

    /// Get the minimum vertex label id of the current pattern
    #[inline]
    pub fn get_min_vertex_label(&self) -> Option<PatternLabelId> {
        self.vertices
            .iter()
            .map(|(_, vertex)| vertex.get_label())
            .min()
    }

    /// Get the maximum vertex label id of the current pattern
    pub fn get_max_vertex_label(&self) -> Option<PatternLabelId> {
        self.vertices
            .iter()
            .map(|(_, vertex)| vertex.get_label())
            .max()
    }

    /// Get Vertex Rank from Vertex ID Reference
    #[inline]
    pub fn get_vertex_group(&self, vertex_id: PatternId) -> Option<usize> {
        self.vertices_data
            .get(vertex_id)
            .map(|vertex_data| vertex_data.group)
    }

    /// Get Vertex Rank from Vertex ID Reference
    #[inline]
    pub fn get_vertex_rank(&self, vertex_id: PatternId) -> Option<usize> {
        self.vertices_data
            .get(vertex_id)
            .map(|vertex_data| vertex_data.rank)
    }

    /// Get a PatternVertex's Tag info
    #[inline]
    pub fn get_vertex_tag(&self, vertex_id: PatternId) -> Option<TagId> {
        self.vertices_data
            .get(vertex_id)
            .and_then(|vertex_data| vertex_data.tag)
    }

    /// Get the predicate requirement of a PatternVertex
    #[inline]
    pub fn get_vertex_predicate(&self, vertex_id: PatternId) -> Option<&common_pb::Expression> {
        self.vertices_data
            .get(vertex_id)
            .and_then(|vertex_data| vertex_data.predicate.as_ref())
    }

    /// Count how many outgoing edges connect to this vertex
    #[inline]
    pub fn get_vertex_out_degree(&self, vertex_id: PatternId) -> usize {
        self.vertices_data
            .get(vertex_id)
            .map(|vertex_data| vertex_data.out_adjacencies.len())
            .unwrap_or(0)
    }

    /// Count how many incoming edges connect to this vertex
    #[inline]
    pub fn get_vertex_in_degree(&self, vertex_id: PatternId) -> usize {
        self.vertices_data
            .get(vertex_id)
            .map(|vertex_data| vertex_data.in_adjacencies.len())
            .unwrap_or(0)
    }

    /// Count how many edges connect to this vertex
    #[inline]
    pub fn get_vertex_degree(&self, vertex_id: PatternId) -> usize {
        self.get_vertex_out_degree(vertex_id) + self.get_vertex_in_degree(vertex_id)
    }
}

/// Iterators of fields of Pattern
impl Pattern {
    /// Iterate Edges
    pub fn edges_iter(&self) -> DynIter<&PatternEdge> {
        Box::new(self.edges.iter().map(|(_, edge)| edge))
    }

    /// Iterate Edges with the given edge label
    pub fn edges_iter_by_label(&self, edge_label: PatternLabelId) -> DynIter<&PatternEdge> {
        Box::new(
            self.edges
                .iter()
                .map(|(_, edge)| edge)
                .filter(move |edge| edge.get_label() == edge_label),
        )
    }

    /// Iterate over edges that has tag
    pub fn edges_with_tag_iter(&self) -> DynIter<&PatternEdge> {
        Box::new(
            self.tag_edge_map
                .iter()
                .map(move |(_, &edge_id)| self.get_edge(edge_id).unwrap()),
        )
    }

    /// Iterate Vertices
    pub fn vertices_iter(&self) -> DynIter<&PatternVertex> {
        Box::new(self.vertices.iter().map(|(_, vertex)| vertex))
    }

    /// Iterate Vertices with the given vertex label
    pub fn vertices_iter_by_label(&self, vertex_label: PatternLabelId) -> DynIter<&PatternVertex> {
        Box::new(
            self.vertices
                .iter()
                .map(|(_, vertex)| vertex)
                .filter(move |vertex| vertex.get_label() == vertex_label),
        )
    }

    /// Iterate over vertices that has tag
    pub fn vertices_with_tag_iter(&self) -> DynIter<&PatternVertex> {
        Box::new(
            self.tag_vertex_map
                .iter()
                .map(move |(_, &vertex_id)| self.get_vertex(vertex_id).unwrap()),
        )
    }

    /// Iterate all outgoing edges from the given vertex
    pub fn out_adjacencies_iter(&self, vertex_id: PatternId) -> DynIter<&Adjacency> {
        if let Some(vertex_data) = self.vertices_data.get(vertex_id) {
            Box::new(vertex_data.out_adjacencies.iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Iterate all incoming edges to the given vertex
    pub fn in_adjacencies_iter(&self, vertex_id: PatternId) -> DynIter<&Adjacency> {
        if let Some(vertex_data) = self.vertices_data.get(vertex_id) {
            Box::new(vertex_data.in_adjacencies.iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Iterate both outgoing and incoming edges of the given vertex
    pub fn adjacencies_iter(&self, vertex_id: PatternId) -> DynIter<&Adjacency> {
        Box::new(
            self.out_adjacencies_iter(vertex_id)
                .chain(self.in_adjacencies_iter(vertex_id)),
        )
    }
}

/// Setters of fields of Pattern
impl Pattern {
    // TODO: Mark as private function
    pub fn set_edge_rank(&mut self, edge_id: PatternId, edge_rank: usize) {
        // Assign the rank to the edge
        if let Some(edge_data) = self.edges_data.get_mut(edge_id) {
            self.rank_edge_map.insert(edge_rank, edge_id);
            edge_data.rank = edge_rank;
        }
    }

    /// Assign a PatternEdge of the Pattern with the Given Tag
    pub fn set_edge_tag(&mut self, edge_tag: TagId, edge_id: PatternId) {
        // If the tag is previously assigned to another edge, remove it
        if let Some(&old_edge_id) = self.tag_edge_map.get(&edge_tag) {
            self.edges_data
                .get_mut(old_edge_id)
                .unwrap()
                .tag = None;
        }
        // Assign the tag to the edge
        if let Some(edge_data) = self.edges_data.get_mut(edge_id) {
            self.tag_edge_map.insert(edge_tag, edge_id);
            edge_data.tag = Some(edge_tag);
        }
    }

    /// Set predicate requirement of a PatternEdge
    pub fn set_edge_predicate(&mut self, edge_id: PatternId, predicate: common_pb::Expression) {
        if let Some(edge_data) = self.edges_data.get_mut(edge_id) {
            edge_data.predicate = Some(predicate);
        }
    }

    // TODO: Mark as private function
    pub fn set_vertex_rank(&mut self, vertex_id: PatternId, vertex_rank: usize) {
        // Assign the rank to the vertex
        if let Some(vertex_data) = self.vertices_data.get_mut(vertex_id) {
            self.rank_vertex_map
                .insert(vertex_rank, vertex_id);
            vertex_data.rank = vertex_rank;
        }
    }

    // TODO: Mark as private function
    /// Assign a PatternVertex with the given group
    pub fn set_vertex_group(&mut self, vertex_id: PatternId, group: usize) {
        if let Some(vertex_data) = self.vertices_data.get_mut(vertex_id) {
            vertex_data.group = group;
        }
    }

    /// Assign a PatternVertex with the given tag
    pub fn set_vertex_tag(&mut self, vertex_id: PatternId, vertex_tag: TagId) {
        // If the tag is previously assigned to another vertex, remove it
        if let Some(&old_vertex_id) = self.tag_vertex_map.get(&vertex_tag) {
            self.vertices_data
                .get_mut(old_vertex_id)
                .unwrap()
                .tag = None;
        }
        // Assign the tag to the vertex
        if let Some(vertex_data) = self.vertices_data.get_mut(vertex_id) {
            self.tag_vertex_map
                .insert(vertex_tag, vertex_id);
            vertex_data.tag = Some(vertex_tag);
        }
    }

    /// Set predicate requirement of a PatternVertex
    pub fn set_vertex_predicate(&mut self, vertex_id: PatternId, predicate: common_pb::Expression) {
        if let Some(vertex_data) = self.vertices_data.get_mut(vertex_id) {
            vertex_data.predicate = Some(predicate);
        }
    }
}

/// Naive Rank Ranking Method
impl Pattern {
    /// Canonical Labeling gives each vertex a unique ID (rank), which is used to encode the pattern.
    ///
    /// It consists of two parts:
    /// - Vertex Grouping (Partition): vertices in the same group (partition) are equivalent in structure.
    /// - Pattern Ranking: given the vertex groups, rank each vertex and edge with a unique ID.
    pub fn canonical_labeling(&mut self) {
        let mut canonical_label_manager = CanonicalLabelManager::init_by_pattern(self);
        self.vertex_grouping(&mut canonical_label_manager);
        self.pattern_ranking(&mut canonical_label_manager);
        // Update the order of vertex adjacencies
        self.update_vertex_adjacences(&canonical_label_manager);
    }

    /// Set Rank for Every Vertex within the Pattern
    ///
    /// Initially, all vertices have rank 0
    ///
    /// Basic Idea: Iteratively update the rank locally (neighbor edges vertices) and update the order of neighbor edges
    fn vertex_grouping(&mut self, canonical_label_manager: &mut CanonicalLabelManager) {
        while !canonical_label_manager.has_vertex_groups_converged() {
            canonical_label_manager.refine_vertex_groups(self);
        }

        // Update vertex groups
        canonical_label_manager
            .vertex_groups_iter()
            .for_each(|(v_id, v_group)| {
                self.set_vertex_group(*v_id, *v_group);
            });
    }

    /// Return the ID of the starting vertex of pattern ranking.
    ///
    /// In our case, It's the vertex with the smallest label and group ID
    fn get_pattern_ranking_start_vertex(&self) -> PatternId {
        let min_v_label = self.get_min_vertex_label().unwrap();
        self.vertices_iter_by_label(min_v_label)
            .map(|vertex| vertex.get_id())
            .min_by(|&v1_id, &v2_id| {
                let v1_group = self.get_vertex_group(v1_id).unwrap();
                let v2_group = self.get_vertex_group(v2_id).unwrap();
                v1_group.cmp(&v2_group)
            })
            .unwrap()
    }

    fn pattern_ranking(&mut self, canonical_label_manager: &mut CanonicalLabelManager) {
        let start_v_id: PatternId = self.get_pattern_ranking_start_vertex();
        canonical_label_manager.pattern_ranking(self, start_v_id);

        // update vertex ranks
        self.rank_vertex_map.clear();
        canonical_label_manager
            .vertex_ranks_iter()
            .map(|(v_id, v_rank)| (*v_id, v_rank.unwrap()))
            .for_each(|(v_id, v_rank)| {
                self.set_vertex_rank(v_id, v_rank);
            });

        // Update edge ranks
        self.rank_edge_map.clear();
        canonical_label_manager
            .edge_ranks_iter()
            .map(|(e_id, e_rank)| (*e_id, e_rank.unwrap()))
            .for_each(|(e_id, e_rank)| {
                self.set_edge_rank(e_id, e_rank);
            });
    }

    /// Compare two adjacencies in the pattern.
    ///
    /// We take the following four properties into consideration:
    /// - Direction of the adjacent edge
    /// - Label of the adjacent edge
    /// - Label of the adjacent vertex
    pub fn cmp_adjacencies(&self, adj1: &Adjacency, adj2: &Adjacency) -> Ordering {
        let adj1_info_tuple =
            (adj1.get_direction(), adj1.get_adj_vertex().get_label(), adj1.get_edge_label());
        let adj2_info_tuple =
            (adj2.get_direction(), adj2.get_adj_vertex().get_label(), adj2.get_edge_label());
        adj1_info_tuple.cmp(&adj2_info_tuple)
    }

    pub fn update_vertex_adjacences(&mut self, canonical_label_manager: &CanonicalLabelManager) {
        canonical_label_manager
            .vertex_adjacencies_iter()
            .for_each(|(v_id, adjacencies)| {
                let updated_out_adjacences = adjacencies
                    .iter()
                    .filter(|adj| adj.get_direction() == PatternDirection::Out)
                    .map(|adj| *adj)
                    .collect();
                self.vertices_data
                    .get_mut(v_id)
                    .unwrap()
                    .out_adjacencies = updated_out_adjacences;

                let updated_in_adjacences = adjacencies
                    .iter()
                    .filter(|adj| adj.get_direction() == PatternDirection::In)
                    .map(|adj| *adj)
                    .collect();
                self.vertices_data
                    .get_mut(v_id)
                    .unwrap()
                    .in_adjacencies = updated_in_adjacences;
            });
    }
}

/// Methods for Pattern Extension
impl Pattern {
    /// Get all the vertices(id) with the same vertex label and vertex group
    ///
    /// These vertices are equivalent in the Pattern
    pub fn get_equivalent_vertices(&self, v_label: PatternLabelId, v_group: usize) -> Vec<PatternVertex> {
        self.vertices_iter()
            .filter(|vertex| {
                vertex.get_label() == v_label && self.get_vertex_group(vertex.get_id()).unwrap() == v_group
            })
            .cloned()
            .collect()
    }

    /// Get the legal id for the future incoming vertex
    fn get_next_pattern_vertex_id(&self) -> PatternId {
        let mut new_vertex_id = self.vertices.len() as PatternId;
        while self.vertices.contains_key(new_vertex_id) {
            new_vertex_id += 1;
        }

        new_vertex_id
    }

    /// Get the legal id for the future incoming vertex
    fn get_next_pattern_edge_id(&self) -> PatternId {
        let mut new_edge_id = self.edges.len() as PatternId;
        while self.edges.contains_key(new_edge_id) {
            new_edge_id += 1;
        }

        new_edge_id
    }

    /// Extend the current Pattern to a new Pattern with the given ExtendStep
    /// - If the ExtendStep is not matched with the current Pattern, the function will return None
    /// - Else, it will return the new Pattern after the extension
    pub fn extend(&self, extend_step: &ExtendStep) -> Option<Pattern> {
        let mut new_pattern = self.clone();
        let target_vertex_label = extend_step.get_target_vertex_label();
        let target_vertex = PatternVertex::new(self.get_next_pattern_vertex_id(), target_vertex_label);
        let target_vertex_data = PatternVertexData::default();
        // Add the newly extended pattern vertex to the new pattern
        new_pattern
            .vertices
            .insert(target_vertex.get_id(), target_vertex);
        new_pattern
            .vertices_data
            .insert(target_vertex.get_id(), target_vertex_data);
        // Iterately add the new pattern edges to the new pattern
        for extend_edge in extend_step.iter() {
            let src_vertex_rank = extend_edge.get_src_vertex_rank();
            if let Some(src_vertex) = self
                .get_vertex_from_rank(src_vertex_rank)
                .cloned()
            {
                let new_pattern_edge_id = new_pattern.get_next_pattern_edge_id();
                let new_pattern_edge_label = extend_edge.get_edge_label();
                let (mut start_vertex, mut end_vertex) = (src_vertex, target_vertex);
                if let PatternDirection::In = extend_edge.get_direction() {
                    std::mem::swap(&mut start_vertex, &mut end_vertex);
                }
                let new_pattern_edge =
                    PatternEdge::new(new_pattern_edge_id, new_pattern_edge_label, start_vertex, end_vertex);
                // Update start vertex and end vertex's adjacency info
                let start_vertex_new_adjacency =
                    Adjacency::new_by_src_vertex_and_edge(&start_vertex, &new_pattern_edge).unwrap();
                new_pattern
                    .vertices_data
                    .get_mut(start_vertex.get_id())
                    .unwrap()
                    .out_adjacencies
                    .push(start_vertex_new_adjacency);
                let end_vertex_new_adjacency =
                    Adjacency::new_by_src_vertex_and_edge(&end_vertex, &new_pattern_edge).unwrap();
                new_pattern
                    .vertices_data
                    .get_mut(end_vertex.get_id())
                    .unwrap()
                    .in_adjacencies
                    .push(end_vertex_new_adjacency);
                new_pattern
                    .edges
                    .insert(new_pattern_edge_id, new_pattern_edge);
                new_pattern
                    .edges_data
                    .insert(new_pattern_edge_id, PatternEdgeData::default());
            } else {
                return None;
            }
        }

        new_pattern.canonical_labeling();
        Some(new_pattern)
    }

    /// Find all possible ExtendSteps of current pattern based on the given Pattern Meta
    pub fn get_extend_steps(
        &self, pattern_meta: &PatternMeta, same_label_vertex_limit: usize,
    ) -> Vec<ExtendStep> {
        let mut extend_steps = vec![];
        // Get all vertex labels from pattern meta as the possible extend target vertex
        let target_v_labels = pattern_meta.vertex_label_ids_iter();
        // For every possible extend target vertex label, find its all adjacent edges to the current pattern
        // Count each vertex's label number
        let mut vertex_label_count_map: HashMap<PatternLabelId, usize> = HashMap::new();
        for vertex in self.vertices_iter() {
            *vertex_label_count_map
                .entry(vertex.get_label())
                .or_insert(0) += 1;
        }
        for target_v_label in target_v_labels {
            if vertex_label_count_map
                .get(&target_v_label)
                .cloned()
                .unwrap_or(0)
                >= same_label_vertex_limit
            {
                continue;
            }
            // The collection of extend edges with a source vertex id
            // The source vertex id is used to specify the extend edge is from which vertex of the pattern
            let mut extend_edges_with_src_id = vec![];
            for (_, src_vertex) in &self.vertices {
                // check whether there are some edges between the target vertex and the current source vertex
                let adjacent_edges =
                    pattern_meta.associated_elabels_iter_by_vlabel(src_vertex.get_label(), target_v_label);
                // Transform all the adjacent edges to ExtendEdge and add to extend_edges_with_src_id
                for (adjacent_edge_label, adjacent_edge_dir) in adjacent_edges {
                    let extend_edge = ExtendEdge::new(
                        self.get_vertex_rank(src_vertex.get_id())
                            .unwrap(),
                        adjacent_edge_label,
                        adjacent_edge_dir,
                    );
                    extend_edges_with_src_id.push((extend_edge, src_vertex.get_id()));
                }
            }
            // Get the subsets of extend_edges_with_src_id, and add every subset to the extend_edges_set_collection
            // The algorithm is BFS Search
            let extend_edges_set_collection =
                get_subsets(extend_edges_with_src_id, |(_, src_id_for_check), extend_edges_set| {
                    limit_repeated_element_num(
                        src_id_for_check,
                        extend_edges_set.iter().map(|(_, v_id)| v_id),
                        1,
                    )
                });
            for extend_edges in extend_edges_set_collection {
                let extend_step = ExtendStep::new(
                    target_v_label,
                    extend_edges
                        .into_iter()
                        .map(|(extend_edge, _)| extend_edge)
                        .collect(),
                );
                extend_steps.push(extend_step);
            }
        }
        extend_steps
    }

    /// Edit the pattern by connect some edges to the current pattern
    fn add_edge(&mut self, edge: &PatternEdge) -> IrResult<()> {
        // Error that the adding edge already exist
        if self.edges.contains_key(edge.get_id()) {
            return Err(IrError::InvalidCode("The adding edge already existed".to_string()));
        }
        let start_vertex = edge.get_start_vertex();
        let end_vertex = edge.get_end_vertex();
        // Error that cannot connect the edge to the pattern
        if let (None, None) =
            (self.vertices.get(start_vertex.get_id()), self.vertices.get(end_vertex.get_id()))
        {
            return Err(IrError::InvalidCode("The adding edge cannot connect to the pattern".to_string()));
        } else if let None = self.vertices.get(start_vertex.get_id()) {
            // end vertex already exists in the pattern, use it to connect
            // add start vertex
            self.vertices
                .insert(start_vertex.get_id(), start_vertex);
            self.vertices_data
                .insert(start_vertex.get_id(), PatternVertexData::default());
        } else if let None = self.vertices.get(end_vertex.get_id()) {
            // start vertex already exists in the pattern, use it to connect
            // add end vertex
            self.vertices
                .insert(end_vertex.get_id(), end_vertex);
            self.vertices_data
                .insert(end_vertex.get_id(), PatternVertexData::default());
        }
        // update start vertex's connection info
        if let Some(start_vertex_data) = self
            .vertices_data
            .get_mut(start_vertex.get_id())
        {
            start_vertex_data
                .out_adjacencies
                .push(Adjacency::new_by_src_vertex_and_edge(&start_vertex, &edge).unwrap());
        }
        // update end vertex's connection info
        if let Some(end_vertex_data) = self.vertices_data.get_mut(end_vertex.get_id()) {
            end_vertex_data
                .in_adjacencies
                .push(Adjacency::new_by_src_vertex_and_edge(&end_vertex, &edge).unwrap());
        }
        // add edge to the pattern
        self.edges.insert(edge.get_id(), edge.clone());
        self.edges_data
            .insert(edge.get_id(), PatternEdgeData::default());
        Ok(())
    }

    /// Add a series of edges to the current pattern to get a new pattern
    pub fn extend_by_edges<'a, T>(&self, edges: T) -> IrResult<Pattern>
    where
        T: Iterator<Item = &'a PatternEdge>,
    {
        let mut new_pattern = self.clone();
        for edge in edges {
            new_pattern.add_edge(edge)?;
        }
        new_pattern.canonical_labeling();
        Ok(new_pattern)
    }

    /// Locate a vertex(id) from the pattern based on the given extend step and target pattern code
    pub fn locate_vertex(
        &self, extend_step: &ExtendStep, target_pattern_code: &Vec<u8>, encoder: &Encoder,
    ) -> Option<PatternId> {
        let mut target_vertex_id: Option<PatternId> = None;
        let target_v_label = extend_step.get_target_vertex_label();
        // mark all the vertices with the same label as the extend step's target vertex as the candidates
        for target_v_cand in self.vertices_iter_by_label(target_v_label) {
            if self.get_vertex_degree(target_v_cand.get_id()) != extend_step.get_extend_edges_num() {
                continue;
            }
            // compare whether the candidate vertex has the same connection info as the extend step
            let cand_e_label_dir_set: BTreeSet<(PatternLabelId, PatternDirection)> = self
                .adjacencies_iter(target_v_cand.get_id())
                .map(|adjacency| (adjacency.get_edge_label(), adjacency.get_direction().reverse()))
                .collect();
            let extend_e_label_dir_set: BTreeSet<(PatternLabelId, PatternDirection)> = extend_step
                .iter()
                .map(|extend_edge| (extend_edge.get_edge_label(), extend_edge.get_direction()))
                .collect();
            // if has the same connection info, check whether the pattern after the removing the target vertex
            // has the same code with the target pattern code
            if cand_e_label_dir_set == extend_e_label_dir_set {
                let mut check_pattern = self.clone();
                check_pattern.remove_vertex(target_v_cand.get_id());
                let check_pattern_code: Vec<u8> = Cipher::encode_to(&check_pattern, encoder);
                // same code means successfully locate the vertex
                if check_pattern_code == *target_pattern_code {
                    target_vertex_id = Some(target_v_cand.get_id());
                    break;
                }
            }
        }
        target_vertex_id
    }

    /// Remove a vertex with all its adjacent edges in the current pattern
    pub fn remove_vertex(&mut self, vertex_id: PatternId) {
        if self.get_vertex(vertex_id).is_some() {
            let adjacencies: Vec<Adjacency> = self
                .adjacencies_iter(vertex_id)
                .cloned()
                .collect();
            // delete target vertex
            // delete in vertices
            self.vertices.remove(vertex_id);
            // delete in vertex tag map
            if let Some(tag) = self.get_vertex_tag(vertex_id) {
                self.tag_vertex_map.remove(&tag);
            }

            self.vertices_data.remove(vertex_id);

            for adjacency in adjacencies {
                let adjacent_vertex_id = adjacency.get_adj_vertex().get_id();
                let adjacent_edge_id = adjacency.get_edge_id();
                // delete adjacent edges
                // delete in edges
                self.edges.remove(adjacent_edge_id);
                // delete in edge tag map
                if let Some(tag) = self.get_edge_tag(adjacent_edge_id) {
                    self.tag_edge_map.remove(&tag);
                }

                self.edges_data.remove(adjacent_edge_id);

                // update adjcent vertices's info
                if let PatternDirection::Out = adjacency.get_direction() {
                    self.vertices_data
                        .get_mut(adjacent_vertex_id)
                        .unwrap()
                        .in_adjacencies
                        .retain(|adj| adj.get_edge_id() != adjacent_edge_id)
                } else {
                    self.vertices_data
                        .get_mut(adjacent_vertex_id)
                        .unwrap()
                        .out_adjacencies
                        .retain(|adj| adj.get_edge_id() != adjacent_edge_id)
                }
            }

            self.canonical_labeling();
        }
    }

    /// Delete a extend step from current pattern to get a new pattern
    ///
    /// The code of the new pattern should be the same as the target pattern code
    pub fn de_extend(
        &self, extend_step: &ExtendStep, target_pattern_code: &Vec<u8>, encoder: &Encoder,
    ) -> Option<Pattern> {
        if let Some(target_vertex_id) = self.locate_vertex(extend_step, target_pattern_code, encoder) {
            let mut new_pattern = self.clone();
            new_pattern.remove_vertex(target_vertex_id);
            Some(new_pattern)
        } else {
            None
        }
    }

    /// Given a vertex id, pick all its neiboring edges and vertices to generate a definite extend step
    pub fn generate_definite_extend_step_by_v_id(
        &self, target_v_id: PatternId,
    ) -> Option<DefiniteExtendStep> {
        if let Some(target_vertex) = self.get_vertex(target_v_id) {
            let target_v_label = target_vertex.get_label();
            let mut extend_edges = vec![];
            for adjacency in self.adjacencies_iter(target_v_id) {
                let edge_id = adjacency.get_edge_id();
                let dir = adjacency.get_direction();
                let edge = self.edges.get(edge_id).unwrap();
                if let PatternDirection::In = dir {
                    extend_edges.push(DefiniteExtendEdge::new(
                        edge.get_start_vertex().get_id(),
                        edge_id,
                        edge.get_label(),
                        PatternDirection::Out,
                    ));
                } else {
                    extend_edges.push(DefiniteExtendEdge::new(
                        edge.get_end_vertex().get_id(),
                        edge_id,
                        edge.get_label(),
                        PatternDirection::In,
                    ));
                }
            }
            Some(DefiniteExtendStep::new(target_v_id, target_v_label, extend_edges))
        } else {
            None
        }
    }
}
