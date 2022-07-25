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
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::convert::{TryFrom, TryInto};
use std::iter::FromIterator;
use std::vec;

use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use vec_map::VecMap;

use super::codec::{Cipher, Encoder};
use crate::catalogue::extend_step::{
    get_subsets, limit_repeated_element_num, DefiniteExtendEdge, DefiniteExtendStep, ExtendEdge, ExtendStep,
};
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::{DynIter, PatternDirection, PatternId, PatternLabelId, PatternRankId};
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
    rank: PatternRankId,
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
            new_pattern.vertex_ranking();
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

    /// Get PatternEdge for Edge from Given Tag
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
    pub fn get_vertex_rank(&self, vertex_id: PatternId) -> Option<PatternRankId> {
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

    /// Assign a PatternVertex with the given rank
    pub fn set_vertex_rank(&mut self, rank: PatternRankId, vertex_id: PatternId) {
        if let Some(vertex_data) = self.vertices_data.get_mut(vertex_id) {
            vertex_data.rank = rank;
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

/// Methods for PatternEdge Reordering inside a Pattern
impl Pattern {
    /// Get the Order of two PatternEdges in a Pattern
    /// Vertex Indices are taken into consideration
    fn cmp_edges(&self, e1_id: PatternId, e2_id: PatternId) -> Ordering {
        let e1 = self.get_edge(e1_id).unwrap();
        let e2 = self.get_edge(e2_id).unwrap();
        if e1_id == e2_id {
            return Ordering::Equal;
        }
        // Compare the label of starting vertex
        match e1
            .get_start_vertex()
            .get_label()
            .cmp(&e2.get_start_vertex().get_label())
        {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }
        // Compare the label of ending vertex
        match e1
            .get_end_vertex()
            .get_label()
            .cmp(&e2.get_end_vertex().get_label())
        {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }
        // Compare Edge Label
        match e1.get_label().cmp(&e2.get_label()) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }
        // Compare the order of the starting vertex
        let e1_start_v_rank = self
            .get_vertex_rank(e1.get_start_vertex().get_id())
            .unwrap();
        let e2_start_v_rank = self
            .get_vertex_rank(e2.get_start_vertex().get_id())
            .unwrap();
        match e1_start_v_rank.cmp(&e2_start_v_rank) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }
        // Compare the order of ending vertex
        let e1_end_v_rank = self
            .get_vertex_rank(e1.get_end_vertex().get_id())
            .unwrap();
        let e2_end_v_rank = self
            .get_vertex_rank(e2.get_end_vertex().get_id())
            .unwrap();
        match e1_end_v_rank.cmp(&e2_end_v_rank) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }
        // Return as equal if still cannot distinguish
        Ordering::Equal
    }
}

/// Naive Rank Ranking Method
impl Pattern {
    /// Set Rank for Every Vertex within the Pattern
    ///
    /// Initially, all vertices have rank 0
    ///
    /// Basic Idea: Iteratively update the rank locally (neighbor edges vertices) and update the order of neighbor edges
    pub fn vertex_ranking(&mut self) {
        // Get the Neighbor Edges for All Vertices, Sorted Simply By Labels
        let mut vertex_adjacencies_map = self.get_vertex_adjacencies_map();
        // Initially, mark all the vertices as unfixed
        let mut vertices_with_unfixed_rank: BTreeSet<PatternId> = self
            .vertices
            .iter()
            .map(|(v_id, _)| v_id)
            .collect();
        // Set initial ranks by comparing the labels & in/out degrees
        let mut is_rank_changed: bool = self
            .set_rank_by_neighbor_info(&vertex_adjacencies_map, &mut vertices_with_unfixed_rank)
            .unwrap();
        // Keep updating ranks by ranks computed in the last iteration
        // Stop until the ranks can no longer be updated
        while is_rank_changed {
            // Update the Order of Neighbor Edges Based on the ranks of end vertices only
            self.update_neighbor_edges_map(&mut vertex_adjacencies_map);
            // Update vertex ranks by newly updated ranks in the last iteration
            is_rank_changed = self.update_rank(&vertex_adjacencies_map, &mut vertices_with_unfixed_rank);
        }
    }

    /// Get neighbor edges of each vertex
    ///
    /// Used for Comparing Vertices when Setting Initial Indices
    ///
    /// The vector of neighbor edges consists of two parts: outgoing edges and incoming edges
    ///
    /// Each neighbor edge element stores 3 values: edge id, target vertex id, and edge direction
    fn get_vertex_adjacencies_map(&self) -> VecMap<Vec<Adjacency>> {
        let mut vertex_adjacencies_map = VecMap::new();
        for v_id in self.vertices_iter().map(|v| v.get_id()) {
            let mut adjacencies: Vec<Adjacency> = self.adjacencies_iter(v_id).cloned().collect();
            // Sort the edges
            adjacencies.sort_by(|adj1, adj2| match adj1.get_direction().cmp(&adj2.get_direction()) {
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
                Ordering::Equal => self.cmp_edges(adj1.get_edge_id(), adj2.get_edge_id()),
            });
            vertex_adjacencies_map.insert(v_id, adjacencies);
        }

        vertex_adjacencies_map
    }

    /// Set Initial Vertex Index by comparing the labels and In/Out Degrees
    ///
    /// Return a bool indicating whether the ranks has changed for this initial rank iteration
    ///
    /// If nothing changed, we conclude that the ranks of all vertices have been set and are stable
    fn set_rank_by_neighbor_info(
        &mut self, vertex_adjacencies_map: &VecMap<Vec<Adjacency>>,
        vertices_with_unfixed_rank: &mut BTreeSet<PatternId>,
    ) -> IrResult<bool> {
        // Mark whether there is a change of vertex rank
        let mut is_rank_changed: bool = false;
        // Stores vertex labels that have been dealth with
        let mut visited_labels: BTreeSet<PatternLabelId> = BTreeSet::new();
        // Store vertices that should be removed from the set of unfixed vertices
        let mut vertices_with_fixed_rank: Vec<PatternId> = Vec::new();
        // Store the mapping from vertex id to rank
        let mut vertex_rank_map: VecMap<PatternRankId> = VecMap::new();
        for &v_id in vertices_with_unfixed_rank.iter() {
            let v_label = self.get_vertex(v_id).unwrap().get_label();
            if visited_labels.contains(&v_label) {
                continue;
            }
            visited_labels.insert(v_label);
            // Collect all vertices with the specified label
            let same_label_vertex_vec: Vec<PatternId> = self
                .vertices_iter_by_label(v_label)
                .map(|vertex| vertex.get_id())
                .collect();
            let vertex_num = same_label_vertex_vec.len();
            // Record the rank and is_rank_fixed properties for each vertex
            let mut vertex_rank_vec: Vec<PatternRankId> = vec![0; vertex_num];
            let mut is_rank_fixed_vec: Vec<bool> = vec![true; vertex_num];
            // To compute the exact rank of a vertex, compare it with all vertices with the same label
            for i in 0..vertex_num {
                for j in (i + 1)..vertex_num {
                    match self.cmp_vertices_by_neighbor_info(
                        same_label_vertex_vec[i],
                        same_label_vertex_vec[j],
                        vertex_adjacencies_map,
                    ) {
                        Ordering::Greater => vertex_rank_vec[i] += 1,
                        Ordering::Less => vertex_rank_vec[j] += 1,
                        Ordering::Equal => {
                            is_rank_fixed_vec[i] = false;
                            is_rank_fixed_vec[j] = false;
                        }
                    }
                }
                // Mark vertices that now have fixed rank
                if is_rank_fixed_vec[i] {
                    vertices_with_fixed_rank.push(same_label_vertex_vec[i]);
                }
                // Update Vertex Rank on the map
                vertex_rank_map.insert(same_label_vertex_vec[i], vertex_rank_vec[i]);
            }
        }
        // Remove vertices that have fixed rank
        for vertex in vertices_with_fixed_rank {
            vertices_with_unfixed_rank.remove(&vertex);
        }
        // Update vertex ranks on pattern
        for (v_id, &v_rank) in vertex_rank_map.iter() {
            if v_rank != 0 {
                is_rank_changed = true;
                self.set_vertex_rank(v_rank, v_id);
            }
        }

        Ok(is_rank_changed)
    }

    /// Update vertex ranks by considering ranks only
    ///
    /// Return a bool indicating whether the ranks has changed for this initial rank iteration
    ///
    /// If nothing changed, we conclude that the ranks of all vertices have been set and are stable
    fn update_rank(
        &mut self, vertex_adjacencies_map: &VecMap<Vec<Adjacency>>,
        vertices_with_unfixed_rank: &mut BTreeSet<PatternId>,
    ) -> bool {
        if vertices_with_unfixed_rank.len() == 0 {
            return false;
        }
        // Mark whether there is a change of vertex rank
        let mut is_rank_changed: bool = false;
        // Stores vertex labels that have been dealt with
        let mut visited_labels: BTreeSet<PatternLabelId> = BTreeSet::new();
        // Store vertices that should be removed from unknown vertex set
        // fixed rank means that the vertex has a unique rank and
        // no other vertices with the same label share the same rank with it
        let mut vertices_with_fixed_rank: Vec<PatternId> = Vec::new();
        // Store the mapping from vertex id to its rank
        let mut vertex_rank_map: VecMap<PatternRankId> = VecMap::new();
        // We only focus on vertices with unfixed ranks
        for &v_id in vertices_with_unfixed_rank.iter() {
            let v_label = self.get_vertex(v_id).unwrap().get_label();
            if visited_labels.contains(&v_label) {
                continue;
            }
            visited_labels.insert(v_label);
            // Store vertices with the same rank and the same label
            let mut same_rank_vertex_map: HashMap<PatternRankId, (Vec<PatternId>, Vec<PatternId>)> =
                HashMap::new();
            // Separate vertices according to their ranks
            for v_id in self
                .vertices_iter_by_label(v_label)
                .map(|vertex| vertex.get_id())
            {
                if vertices_with_unfixed_rank.contains(&v_id) {
                    let v_rank = self.get_vertex_rank(v_id).unwrap();
                    if !same_rank_vertex_map.contains_key(&v_rank) {
                        same_rank_vertex_map.insert(v_rank, (Vec::new(), Vec::new()));
                    }
                    // Mark if anyone of the neighbor vertices have fixed rank
                    let mut has_fixed_rank_neighbor: bool = false;
                    let vertex_neighbor_edges = vertex_adjacencies_map.get(v_id).unwrap();
                    // Store vertices with neighbor vertices having fixed ranks in the first element of the tuple
                    // Otherwise, store in the second element of the tuple
                    for adj_vertex_id in vertex_neighbor_edges
                        .iter()
                        .map(|adj| adj.get_adj_vertex().get_id())
                    {
                        if !vertices_with_unfixed_rank.contains(&adj_vertex_id) {
                            has_fixed_rank_neighbor = true;
                            let (same_rank_vertices_with_fixed_rank_neighbor, _) =
                                same_rank_vertex_map.get_mut(&v_rank).unwrap();
                            same_rank_vertices_with_fixed_rank_neighbor.push(v_id);
                            break;
                        }
                    }
                    if !has_fixed_rank_neighbor {
                        let (_, same_rank_vertices_without_fixed_rank_neighbor) =
                            same_rank_vertex_map.get_mut(&v_rank).unwrap();
                        same_rank_vertices_without_fixed_rank_neighbor.push(v_id);
                    }
                }
            }
            // Perform vertex ranking on each groups of vertices with the same label and rank
            for (vertex_set_with_fixed_rank_neighbor, vertex_set_without_fixed_rank_neighbor) in
                same_rank_vertex_map.values()
            {
                // Case-1: All vertices have no fixed rank neighbor
                if vertex_set_with_fixed_rank_neighbor.len() == 0 {
                    let old_rank = self
                        .get_vertex_rank(vertex_set_without_fixed_rank_neighbor[0])
                        .unwrap();
                    let vertex_num = vertex_set_without_fixed_rank_neighbor.len();
                    let mut vertex_rank_vec: Vec<PatternRankId> = vec![old_rank; vertex_num];
                    let mut is_rank_fixed_vec: Vec<bool> = vec![true; vertex_num];
                    // To compute the exact rank of a vertex, compare it with all vertices with the same label
                    for i in 0..vertex_num {
                        for j in (i + 1)..vertex_num {
                            match self.cmp_vertices_by_rank(
                                vertex_set_without_fixed_rank_neighbor[i],
                                vertex_set_without_fixed_rank_neighbor[j],
                                vertex_adjacencies_map,
                            ) {
                                Ordering::Greater => vertex_rank_vec[i] += 1,
                                Ordering::Less => vertex_rank_vec[j] += 1,
                                Ordering::Equal => {
                                    is_rank_fixed_vec[i] = false;
                                    is_rank_fixed_vec[j] = false;
                                }
                            }
                        }
                        // Mark vertices that now have fixed rank
                        if is_rank_fixed_vec[i] {
                            vertices_with_fixed_rank.push(vertex_set_without_fixed_rank_neighbor[i]);
                        }
                        // Update Vertex Rank on the map
                        vertex_rank_map
                            .insert(vertex_set_without_fixed_rank_neighbor[i], vertex_rank_vec[i]);
                    }
                }
                // Case-2: There exists vertex having fixed rank neighbor
                else {
                    let old_rank: PatternRankId = self
                        .get_vertex_rank(vertex_set_with_fixed_rank_neighbor[0])
                        .unwrap();
                    let mut vertices_for_ranking: Vec<PatternId> =
                        vertex_set_with_fixed_rank_neighbor.clone();
                    // Take one vertex that has no fixed rank neighbor to represent all such vertices
                    if vertex_set_without_fixed_rank_neighbor.len() > 0 {
                        vertices_for_ranking.push(vertex_set_without_fixed_rank_neighbor[0]);
                    }
                    let vertex_num = vertices_for_ranking.len();
                    let mut vertex_rank_vec: Vec<PatternRankId> = vec![old_rank; vertex_num];
                    let mut is_rank_fixed_vec: Vec<bool> = vec![true; vertex_num];
                    // To compute the exact rank of a vertex, compare it with all vertices with the same label
                    for i in 0..vertex_num {
                        for j in (i + 1)..vertex_num {
                            match self.cmp_vertices_by_rank(
                                vertices_for_ranking[i],
                                vertices_for_ranking[j],
                                vertex_adjacencies_map,
                            ) {
                                Ordering::Greater => {
                                    if j == vertex_num - 1
                                        && vertex_set_without_fixed_rank_neighbor.len() > 0
                                    {
                                        vertex_rank_vec[i] +=
                                            vertex_set_without_fixed_rank_neighbor.len() as PatternRankId;
                                    } else {
                                        vertex_rank_vec[i] += 1;
                                    }
                                }
                                Ordering::Less => vertex_rank_vec[j] += 1,
                                Ordering::Equal => {
                                    is_rank_fixed_vec[i] = false;
                                    is_rank_fixed_vec[j] = false;
                                }
                            }
                        }
                        // Mark vertices that now have fixed rank
                        if i != vertex_num - 1 && is_rank_fixed_vec[i] {
                            vertices_with_fixed_rank.push(vertices_for_ranking[i]);
                        }
                        if i == vertex_num - 1
                            && vertex_set_without_fixed_rank_neighbor.len() == 0
                            && is_rank_fixed_vec[i]
                        {
                            vertices_with_fixed_rank.push(vertices_for_ranking[i]);
                        }
                    }
                    // Update Vertex Rank on the map
                    for i in 0..vertex_set_with_fixed_rank_neighbor.len() {
                        vertex_rank_map.insert(vertex_set_with_fixed_rank_neighbor[i], vertex_rank_vec[i]);
                    }
                    for j in 0..vertex_set_without_fixed_rank_neighbor.len() {
                        vertex_rank_map.insert(
                            vertex_set_without_fixed_rank_neighbor[j],
                            vertex_rank_vec[vertex_num - 1],
                        );
                    }
                }
            }
        }

        // Remove vertices that have fixed rank
        for v_id in vertices_with_fixed_rank {
            vertices_with_unfixed_rank.remove(&v_id);
        }
        // Update vertex rank on the pattern
        for (v_id, &v_rank) in vertex_rank_map.iter() {
            let old_rank: PatternRankId = self.get_vertex_rank(v_id).unwrap();
            if v_rank != old_rank {
                is_rank_changed = true;
                self.set_vertex_rank(v_rank, v_id);
            }
        }
        is_rank_changed
    }

    /// Compare the ranks of two PatternVertices
    ///
    /// Consider labels and out/in degrees only
    ///
    /// Called when setting initial ranks
    fn cmp_vertices_by_neighbor_info(
        &self, v1_id: PatternId, v2_id: PatternId, vertex_adjacencies_map: &VecMap<Vec<Adjacency>>,
    ) -> Ordering {
        let v1_label = self.get_vertex(v1_id).unwrap().get_label();
        let v2_label = self.get_vertex(v2_id).unwrap().get_label();
        match v1_label.cmp(&v2_label) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }
        // Compare Vertex Out Degree
        match self
            .get_vertex_out_degree(v1_id)
            .cmp(&self.get_vertex_out_degree(v2_id))
        {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }
        // Compare Vertex In Degree
        match self
            .get_vertex_in_degree(v1_id)
            .cmp(&self.get_vertex_in_degree(v2_id))
        {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }

        let v1_adjacencies = vertex_adjacencies_map.get(v1_id).unwrap();
        let v2_adjacencies = vertex_adjacencies_map.get(v2_id).unwrap();
        for adj_index in 0..v1_adjacencies.len() {
            // Compare the label of adjacent vertex
            let v1_adj_v_label = v1_adjacencies[adj_index]
                .get_adj_vertex()
                .get_label();
            let v2_adj_v_label = v2_adjacencies[adj_index]
                .get_adj_vertex()
                .get_label();
            match v1_adj_v_label.cmp(&v2_adj_v_label) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                Ordering::Equal => (),
            }

            // Compare the label of adjacent edges
            let v1_adj_edge_label = v1_adjacencies[adj_index].get_edge_label();
            let v2_adj_edge_label = v2_adjacencies[adj_index].get_edge_label();
            match v1_adj_edge_label.cmp(&v2_adj_edge_label) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                Ordering::Equal => (),
            }
        }

        // Return Equal if Still Cannot Distinguish
        Ordering::Equal
    }

    /// Compare the ranks of two PatternVertices
    ///
    /// Called when updating vertex ranks as only ranks need to be considered
    fn cmp_vertices_by_rank(
        &self, v1_id: PatternId, v2_id: PatternId, vertex_adjacencies_map: &VecMap<Vec<Adjacency>>,
    ) -> Ordering {
        let v1_rank = self.get_vertex_rank(v1_id).unwrap();
        let v2_rank = self.get_vertex_rank(v2_id).unwrap();
        // Compare the ranks
        match v1_rank.cmp(&v2_rank) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }
        // Compare End Vertex Rank
        let v1_adjacencies = vertex_adjacencies_map.get(v1_id).unwrap();
        let v2_adjacencies = vertex_adjacencies_map.get(v2_id).unwrap();
        for adj_index in 0..v1_adjacencies.len() {
            let v1_adj_v_rank = self
                .get_vertex_rank(
                    v1_adjacencies[adj_index]
                        .get_adj_vertex()
                        .get_id(),
                )
                .unwrap();
            let v2_adj_v_rank = self
                .get_vertex_rank(
                    v2_adjacencies[adj_index]
                        .get_adj_vertex()
                        .get_id(),
                )
                .unwrap();
            match v1_adj_v_rank.cmp(&v2_adj_v_rank) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                Ordering::Equal => (),
            }
        }

        // Return Equal if Still Cannot Distinguish
        Ordering::Equal
    }

    // Update the Order of Neighbor Vertices Based on the ranks of end vertices only
    fn update_neighbor_edges_map(&self, vertex_adjacencies_map: &mut VecMap<Vec<Adjacency>>) {
        for (_, adjacencies) in vertex_adjacencies_map.iter_mut() {
            adjacencies.sort_by(|e1, e2| {
                self.get_vertex_rank(e1.get_adj_vertex().get_id())
                    .cmp(&self.get_vertex_rank(e2.get_adj_vertex().get_id()))
            })
        }
    }
}

/// DFS Sorting
///
/// Find a unique sequence of edges in DFS order and assign each vertex with a unique DFS id for easier decoding process
impl Pattern {
    /// Return the ID of the starting vertex of DFS.
    ///
    /// In our case, It's the vertex with the smallest label and rank
    fn get_dfs_starting_vertex(&self) -> PatternId {
        // Step-1: Find the smallest vertex label
        let min_v_label = self.get_min_vertex_label().unwrap();
        // Step-2: Find the vertex with the smallest rank
        self.vertices_iter_by_label(min_v_label)
            .map(|vertex| vertex.get_id())
            .min_by(|&v1_id, &v2_id| {
                let v1_rank = self.get_vertex_rank(v1_id).unwrap();
                let v2_rank = self.get_vertex_rank(v2_id).unwrap();
                v1_rank.cmp(&v2_rank)
            })
            .unwrap()
    }

    /// Perform DFS Sorting to Pattern Edges Based on Labels and Ranks
    ///
    /// Return a tuple of 2 elements
    ///
    /// dfs_edge_sequence is a vector of edge ids in DFS order
    ///
    /// vertex_dfs_id_map maps from vertex ids to dfs id
    pub fn get_dfs_edge_sequence(&self) -> Option<(Vec<PatternId>, VecMap<PatternRankId>)> {
        // output edge sequence
        let mut dfs_edge_sequence: Vec<PatternId> = Vec::new();
        // DFS Generation Tree
        let mut vertex_dfs_id_map = VecMap::new();
        // get the starting vertex
        let starting_v_id: PatternId = self.get_dfs_starting_vertex();
        vertex_dfs_id_map.insert(starting_v_id, 0);
        let mut next_free_id: PatternRankId = 1;
        // collect neighbor edges info for each vertex
        let vertex_adjacencies_map = self.get_vertex_adjacencies_map();
        // Record which edges have been visited
        let mut visited_edges: BTreeSet<PatternId> = BTreeSet::new();
        // Vertex Stack used for DFS backtracking
        let mut vertex_stack: VecDeque<PatternId> = VecDeque::new();
        vertex_stack.push_back(starting_v_id);
        // Perform DFS on vertices
        while let Some(&current_v_id) = vertex_stack.back() {
            let vertex_adjacencies = vertex_adjacencies_map
                .get(current_v_id)
                .unwrap();
            let mut found_next_edge: bool = false;
            let mut adj_index: usize = 0;
            while adj_index < vertex_adjacencies.len() {
                let mut edge_id = vertex_adjacencies[adj_index].get_edge_id();
                let mut end_v_id = vertex_adjacencies[adj_index]
                    .get_adj_vertex()
                    .get_id();
                let e_dir = vertex_adjacencies[adj_index].get_direction();
                if !visited_edges.contains(&edge_id) {
                    found_next_edge = true;
                    // Case-1: Vertex that has not been tranversed has the highesrt priority
                    if !vertex_dfs_id_map.contains_key(end_v_id) {
                        vertex_dfs_id_map.insert(end_v_id, next_free_id);
                        next_free_id += 1;
                    }
                    // Case-2: Compare the DFS ids between end vertices that have been traversed and with the same ranks
                    // Choose the one with the smallest DFS id
                    else {
                        let mut min_dfs_id: PatternRankId = *vertex_dfs_id_map.get(end_v_id).unwrap();
                        while adj_index < vertex_adjacencies.len() - 1 {
                            adj_index += 1;
                            let adj_edge_id = vertex_adjacencies[adj_index].get_edge_id();
                            let adj_v_id = vertex_adjacencies[adj_index]
                                .get_adj_vertex()
                                .get_id();
                            let adj_edge_dir = vertex_adjacencies[adj_index].get_direction();
                            if visited_edges.contains(&adj_edge_id) {
                                continue;
                            };
                            if adj_edge_dir != e_dir {
                                break;
                            };
                            match self.cmp_edges(edge_id, adj_edge_id) {
                                Ordering::Greater => break,
                                Ordering::Less => break,
                                Ordering::Equal => {
                                    if !vertex_dfs_id_map.contains_key(adj_v_id) {
                                        vertex_dfs_id_map.insert(adj_v_id, next_free_id);
                                        next_free_id += 1;
                                        // Update
                                        edge_id = adj_edge_id;
                                        end_v_id = adj_v_id;
                                        break;
                                    }

                                    let current_dfs_id: PatternRankId =
                                        *vertex_dfs_id_map.get(adj_v_id).unwrap();
                                    if current_dfs_id < min_dfs_id {
                                        min_dfs_id = current_dfs_id;
                                        // Update
                                        edge_id = adj_edge_id;
                                        end_v_id = adj_v_id;
                                    }
                                }
                            }
                        }
                    }

                    // Update
                    dfs_edge_sequence.push(edge_id);
                    visited_edges.insert(edge_id);
                    vertex_stack.push_back(end_v_id);
                    break;
                }

                adj_index += 1;
            }

            // If Cannot find the next edge to traverse
            if !found_next_edge {
                vertex_stack.pop_back();
            }
        }
        // Check if dfs sorting fails
        if visited_edges.len() == self.get_edges_num()
            && dfs_edge_sequence.len() == self.get_edges_num()
            && vertex_dfs_id_map.len() == self.get_vertices_num()
        {
            Some((dfs_edge_sequence, vertex_dfs_id_map))
        } else {
            None
        }
    }
}

/// Methods for Pattern Extension
impl Pattern {
    /// Get all the vertices(id) with the same vertex label and vertex rank
    ///
    /// These vertices are equivalent in the Pattern
    fn get_equivalent_vertices(
        &self, v_label: PatternLabelId, v_rank: PatternRankId,
    ) -> Vec<PatternVertex> {
        self.vertices_iter()
            .filter(|vertex| {
                vertex.get_label() == v_label && self.get_vertex_rank(vertex.get_id()).unwrap() == v_rank
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
    ///
    /// The ExtendStep is not mathced with the current Pattern if:
    /// 1. Some extend edges of the ExtendStep cannot find a correponsponding source vertex in current Pattern
    ///    (the required source vertex doesn't exist or already occupied by other extend edges)
    /// 2. Or meet some limitations(e.g. limit the length of Pattern)
    pub fn extend(&self, extend_step: &ExtendStep) -> Option<Pattern> {
        let mut new_pattern = self.clone();
        let target_v_label = extend_step.get_target_v_label();
        let new_pattern_vertex = PatternVertex::new(self.get_next_pattern_vertex_id(), target_v_label);
        let mut new_pattern_vertex_data = PatternVertexData::default();
        for ((v_label, v_rank), extend_edges) in extend_step.iter() {
            // Get all the vertices which can be used to extend with these extend edges
            let vertices_can_use = self.get_equivalent_vertices(*v_label, *v_rank);
            // There's no enough vertices to extend, just return None
            if vertices_can_use.len() < extend_edges.len() {
                return None;
            }
            // Connect each vertex can be use to each extend edge
            for (i, extend_edge) in extend_edges.iter().enumerate() {
                let extend_vertex = vertices_can_use[i];
                let extend_dir = extend_edge.get_direction();
                // new pattern edge info
                let (start_vertex, end_vertex) = if let PatternDirection::Out = extend_dir {
                    (extend_vertex, new_pattern_vertex)
                } else {
                    (new_pattern_vertex, extend_vertex)
                };
                let new_pattern_edge_id = new_pattern.get_next_pattern_edge_id();
                let new_pattern_edge_label = extend_edge.get_edge_label();
                let new_pattern_edge =
                    PatternEdge::new(new_pattern_edge_id, new_pattern_edge_label, start_vertex, end_vertex);
                let extend_vertex_data = new_pattern
                    .vertices_data
                    .get_mut(extend_vertex.get_id())
                    .unwrap();
                let new_pattern_vertex_adj =
                    Adjacency::new_by_src_vertex_and_edge(&new_pattern_vertex, &new_pattern_edge).unwrap();
                let extend_vertex_adj =
                    Adjacency::new_by_src_vertex_and_edge(&extend_vertex, &new_pattern_edge).unwrap();
                if let PatternDirection::Out = extend_dir {
                    new_pattern_vertex_data
                        .in_adjacencies
                        .push(new_pattern_vertex_adj);
                    extend_vertex_data
                        .out_adjacencies
                        .push(extend_vertex_adj);
                } else {
                    new_pattern_vertex_data
                        .out_adjacencies
                        .push(new_pattern_vertex_adj);
                    extend_vertex_data
                        .in_adjacencies
                        .push(extend_vertex_adj);
                }
                // Add the new pattern edge info to the new Pattern
                new_pattern
                    .edges
                    .insert(new_pattern_edge_id, new_pattern_edge);
                new_pattern
                    .edges_data
                    .insert(new_pattern_edge_id, PatternEdgeData::default());
            }
        }
        // Add the newly extended pattern vertex to the new pattern
        new_pattern
            .vertices
            .insert(new_pattern_vertex.get_id(), new_pattern_vertex);
        new_pattern
            .vertices_data
            .insert(new_pattern_vertex.get_id(), new_pattern_vertex_data);
        new_pattern.vertex_ranking();
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
                        src_vertex.get_label(),
                        self.get_vertex_rank(src_vertex.get_id())
                            .unwrap(),
                        adjacent_edge_label,
                        adjacent_edge_dir,
                    );
                    extend_edges_with_src_id.push((extend_edge, src_vertex.get_id()));
                }
            }
            // Get the subsets of extend_edges_with_src_id, and add every subset to the extend edgess
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
        new_pattern.vertex_ranking();
        Ok(new_pattern)
    }

    /// Locate a vertex(id) from the pattern based on the given extend step and target pattern code
    pub fn locate_vertex(
        &self, extend_step: &ExtendStep, target_pattern_code: &Vec<u8>, encoder: &Encoder,
    ) -> Option<PatternId> {
        let mut target_vertex_id: Option<PatternId> = None;
        let target_v_label = extend_step.get_target_v_label();
        // mark all the vertices with the same label as the extend step's target vertex as the candidates
        for target_v_cand in self.vertices_iter_by_label(target_v_label) {
            if self.get_vertex_degree(target_v_cand.get_id()) != extend_step.get_extend_edges_num() {
                continue;
            }
            // compare whether the candidate vertex has the same connection info as the extend step
            let cand_src_v_e_label_dir_set: BTreeSet<(PatternLabelId, PatternLabelId, PatternDirection)> =
                self.adjacencies_iter(target_v_cand.get_id())
                    .map(|adjacency| {
                        (
                            adjacency.get_adj_vertex().get_label(),
                            adjacency.get_edge_label(),
                            adjacency.get_direction().reverse(),
                        )
                    })
                    .collect();
            let extend_src_v_e_label_dir_set: BTreeSet<(PatternLabelId, PatternLabelId, PatternDirection)> =
                extend_step
                    .extend_edges_iter()
                    .map(|extend_edge| {
                        (
                            extend_edge.get_start_vertex_label(),
                            extend_edge.get_edge_label(),
                            extend_edge.get_direction(),
                        )
                    })
                    .collect();
            // if has the same connection info, check whether the pattern after the removing the target vertex
            // has the same code with the target pattern code
            if cand_src_v_e_label_dir_set == extend_src_v_e_label_dir_set {
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
            self.vertex_ranking();
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
