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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::iter::FromIterator;

use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use ir_common::NameOrId;
use vec_map::VecMap;

use crate::catalogue::extend_step::*;
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::{DynIter, PatternDirection, PatternId, PatternLabelId, PatternRankId};
use crate::error::{IrError, IrResult};

#[derive(Debug, Clone)]
pub struct PatternVertex {
    id: PatternId,
    label: PatternLabelId,
    /// Used to Identify vertices with same label
    rank: PatternRankId,
    /// Key: edge id, Value: (vertex id, direction)
    /// Usage: 1. this map stores all adjacent edges of this vertex
    ///        2. given an adjacent edge id, find the adjacent vertex id through this edge with direction
    adjacent_edges: BTreeMap<PatternId, (PatternId, PatternDirection)>,
    /// Key: vertex id, Value: Vec<(edge id, direction)>
    /// Usage: 1. this map stores all adjacent vertices of this vertex
    ///        2. given an adjacent vertex id, find all possible edges connecting two vertices with direction
    adjacent_vertices: BTreeMap<PatternId, Vec<(PatternId, PatternDirection)>>,
    /// How many out edges adjacent to this vertex
    out_degree: usize,
    /// How many in edges adjacent to this vertex
    in_degree: usize,
}

/// Methods to access the fields of a PatternVertex
impl PatternVertex {
    pub fn get_id(&self) -> PatternId {
        self.id
    }

    pub fn get_label(&self) -> PatternLabelId {
        self.label
    }

    pub fn get_rank(&self) -> PatternRankId {
        self.rank
    }

    pub fn adjacent_edges_iter(&self) -> DynIter<(PatternId, PatternId, PatternDirection)> {
        Box::new(
            self.adjacent_edges
                .iter()
                .map(|(e, (v, dir))| (*e, *v, *dir)),
        )
    }

    pub fn adjacent_vertices_iter(&self) -> DynIter<(PatternId, &Vec<(PatternId, PatternDirection)>)> {
        Box::new(
            self.adjacent_vertices
                .iter()
                .map(|(vertex, edge_with_dir)| (*vertex, edge_with_dir)),
        )
    }

    pub fn get_out_degree(&self) -> usize {
        self.out_degree
    }

    pub fn get_in_degree(&self) -> usize {
        self.in_degree
    }

    /// Get how many connections(both out and in) the current pattern vertex has
    pub fn get_degree(&self) -> usize {
        self.out_degree + self.in_degree
    }

    /// Given a edge id, get the vertex adjacent to the current vertex through the edge with the direction
    pub fn get_adjacent_vertex_by_edge_id(
        &self, edge_id: PatternId,
    ) -> Option<(PatternId, PatternDirection)> {
        self.adjacent_edges.get(&edge_id).cloned()
    }

    /// Given a vertex id, iterate all the edges connecting the given vertex and current vertex with the direction
    pub fn adjacent_edges_iter_by_vertex_id(
        &self, vertex_id: PatternId,
    ) -> DynIter<(PatternId, PatternDirection)> {
        match self.adjacent_vertices.get(&vertex_id) {
            Some(adjacent_edges) => Box::new(
                adjacent_edges
                    .iter()
                    .map(|adjacent_edge| *adjacent_edge),
            ),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Setters
    pub fn set_rank(&mut self, rank: PatternRankId) {
        self.rank = rank;
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PatternEdge {
    id: PatternId,
    label: PatternLabelId,
    start_v_id: PatternId,
    end_v_id: PatternId,
    start_v_label: PatternLabelId,
    end_v_label: PatternLabelId,
}

/// Initializers of PatternEdge
impl PatternEdge {
    /// Initializer
    pub fn new(
        id: PatternId, label: PatternLabelId, start_v_id: PatternId, end_v_id: PatternId,
        start_v_label: PatternLabelId, end_v_label: PatternLabelId,
    ) -> PatternEdge {
        PatternEdge { id, label, start_v_id, end_v_id, start_v_label, end_v_label }
    }
}

/// Methods to access the fields of a PatternEdge
impl PatternEdge {
    pub fn get_id(&self) -> PatternId {
        self.id
    }

    pub fn get_label(&self) -> PatternLabelId {
        self.label
    }

    pub fn get_start_vertex_id(&self) -> PatternId {
        self.start_v_id
    }

    pub fn get_end_vertex_id(&self) -> PatternId {
        self.end_v_id
    }

    pub fn get_start_vertex_label(&self) -> PatternLabelId {
        self.start_v_label
    }

    pub fn get_end_vertex_label(&self) -> PatternLabelId {
        self.end_v_label
    }
}

#[derive(Debug, Clone)]
pub struct Pattern {
    /// Key: edge id, Value: struct PatternEdge
    edges: VecMap<PatternEdge>,
    /// Key: vertex id, Value: struct PatternVertex
    vertices: VecMap<PatternVertex>,
    /// Key: edge label id, Value: BTreeSet<edge id>
    edge_label_map: BTreeMap<PatternLabelId, BTreeSet<PatternId>>,
    /// Key: vertex label id, Value: BTreeSet<vertex id>
    vertex_label_map: BTreeMap<PatternLabelId, BTreeSet<PatternId>>,
    /// Key: edge's Tag info, Value: edge id
    edge_tag_map: BTreeMap<NameOrId, PatternId>,
    /// Key: vertex's Tag info, Value: vertex id
    vertex_tag_map: BTreeMap<NameOrId, PatternId>,
    /// Key: edge id, Value: properties the edge required
    edge_properties_map: BTreeMap<PatternId, Vec<NameOrId>>,
    /// Key: vertex id, Value: properties the vertex required
    vertex_properties_map: BTreeMap<PatternId, Vec<NameOrId>>,
    /// Key: edge id, Value: predicate of the edge
    edge_predicate_map: BTreeMap<PatternId, common_pb::Expression>,
    /// Key: vertex id, Value: predicate of the vertex
    vertex_predicate_map: BTreeMap<PatternId, common_pb::Expression>,
}

/// Initializers of Pattern
/// Initialize a Pattern containing only one vertex from hte vertex's id and label
impl From<(PatternId, PatternLabelId)> for Pattern {
    fn from((vertex_id, vertex_label): (PatternId, PatternLabelId)) -> Pattern {
        let vertex = PatternVertex {
            id: vertex_id,
            label: vertex_label,
            rank: 0,
            adjacent_edges: BTreeMap::new(),
            adjacent_vertices: BTreeMap::new(),
            out_degree: 0,
            in_degree: 0,
        };
        Pattern::from(vertex)
    }
}

/// Initialze a Pattern from just a single Pattern Vertex
impl From<PatternVertex> for Pattern {
    fn from(vertex: PatternVertex) -> Pattern {
        Pattern {
            edges: VecMap::new(),
            vertices: VecMap::from_iter([(vertex.id, vertex.clone())]),
            edge_label_map: BTreeMap::new(),
            vertex_label_map: BTreeMap::from([(vertex.label, BTreeSet::from([vertex.id]))]),
            edge_tag_map: BTreeMap::new(),
            vertex_tag_map: BTreeMap::new(),
            edge_properties_map: BTreeMap::new(),
            vertex_properties_map: BTreeMap::new(),
            edge_predicate_map: BTreeMap::new(),
            vertex_predicate_map: BTreeMap::new(),
        }
    }
}

/// Initialize a Pattern from a vertor of Pattern Edges
impl TryFrom<Vec<PatternEdge>> for Pattern {
    type Error = IrError;

    fn try_from(edges: Vec<PatternEdge>) -> Result<Self, Self::Error> {
        if !edges.is_empty() {
            let mut new_pattern = Pattern {
                edges: VecMap::new(),
                vertices: VecMap::new(),
                edge_label_map: BTreeMap::new(),
                vertex_label_map: BTreeMap::new(),
                edge_tag_map: BTreeMap::new(),
                vertex_tag_map: BTreeMap::new(),
                edge_properties_map: BTreeMap::new(),
                vertex_properties_map: BTreeMap::new(),
                edge_predicate_map: BTreeMap::new(),
                vertex_predicate_map: BTreeMap::new(),
            };
            for edge in edges {
                // Add the new Pattern Edge to the new Pattern
                new_pattern.edges.insert(edge.id, edge);
                let edge_set = new_pattern
                    .edge_label_map
                    .entry(edge.label)
                    .or_insert(BTreeSet::new());
                edge_set.insert(edge.id);
                // Add or update the start vertex to the new Pattern
                let start_vertex = new_pattern
                    .vertices
                    .entry(edge.start_v_id)
                    .or_insert(PatternVertex {
                        id: edge.start_v_id,
                        label: edge.start_v_label,
                        rank: 0,
                        adjacent_edges: BTreeMap::new(),
                        adjacent_vertices: BTreeMap::new(),
                        out_degree: 0,
                        in_degree: 0,
                    });
                start_vertex
                    .adjacent_edges
                    .insert(edge.id, (edge.end_v_id, PatternDirection::Out));
                start_vertex
                    .adjacent_vertices
                    .entry(edge.end_v_id)
                    .or_insert(vec![])
                    .push((edge.id, PatternDirection::Out));
                start_vertex.out_degree += 1;
                new_pattern
                    .vertex_label_map
                    .entry(edge.start_v_label)
                    .or_insert(BTreeSet::new())
                    .insert(edge.start_v_id);
                // Add or update the end vertex to the new Pattern
                let end_vertex = new_pattern
                    .vertices
                    .entry(edge.end_v_id)
                    .or_insert(PatternVertex {
                        id: edge.end_v_id,
                        label: edge.end_v_label,
                        rank: 0,
                        adjacent_edges: BTreeMap::new(),
                        adjacent_vertices: BTreeMap::new(),
                        out_degree: 0,
                        in_degree: 0,
                    });
                end_vertex
                    .adjacent_edges
                    .insert(edge.id, (edge.start_v_id, PatternDirection::In));
                end_vertex
                    .adjacent_vertices
                    .entry(edge.start_v_id)
                    .or_insert(vec![])
                    .push((edge.id, PatternDirection::In));
                end_vertex.in_degree += 1;
                new_pattern
                    .vertex_label_map
                    .entry(edge.end_v_label)
                    .or_insert(BTreeSet::new())
                    .insert(edge.end_v_id);
            }
            new_pattern.vertex_ranking();
            Ok(new_pattern)
        } else {
            Err(IrError::InvalidPattern("Empty pattern".to_string()))
        }
    }
}

/// Initialize a Pattern from a protubuf Pattern and a PatternMeta
impl TryFrom<(&pb::Pattern, &PatternMeta)> for Pattern {
    type Error = IrError;

    fn try_from(
        (pattern_message, pattern_meta): (&pb::Pattern, &PatternMeta),
    ) -> Result<Self, Self::Error> {
        use ir_common::generated::common::name_or_id::Item as TagItem;
        use pb::pattern::binder::Item as BinderItem;

        let mut assign_vertex_id = 0;
        let mut assign_edge_id = 0;
        let mut pattern_edges = vec![];
        let mut tag_v_id_map: BTreeMap<NameOrId, PatternId> = BTreeMap::new();
        let mut id_label_map: BTreeMap<PatternId, PatternLabelId> = BTreeMap::new();
        let mut v_id_properties_map: BTreeMap<PatternId, Vec<NameOrId>> = BTreeMap::new();
        let mut v_id_predicate_map: BTreeMap<PatternId, common_pb::Expression> = BTreeMap::new();
        for sentence in &pattern_message.sentences {
            if sentence.binders.is_empty() {
                return Err(IrError::InvalidPattern("Match sentence has no binder".to_string()));
            }
            let start_tag: NameOrId = sentence
                .start
                .as_ref()
                .cloned()
                .ok_or(IrError::InvalidPattern("Match sentence's start tag is None".to_string()))?
                .try_into()
                .map_err(|err| IrError::ParsePbError(err))?;
            let start_tag_v_id: PatternId;
            if let Some(v_id) = tag_v_id_map.get(&start_tag) {
                start_tag_v_id = *v_id;
            } else {
                start_tag_v_id = assign_vertex_id;
                tag_v_id_map.insert(start_tag.clone(), assign_vertex_id);
                assign_vertex_id += 1;
            }
            let start_tag_label = id_label_map.get(&start_tag_v_id).cloned();
            let end_tag: Option<NameOrId> = sentence
                .end
                .as_ref()
                .cloned()
                .and_then(|name_or_id| name_or_id.try_into().ok());
            let end_tag_v_id: Option<PatternId>;
            if let Some(tag) = end_tag {
                if let Some(v_id) = tag_v_id_map.get(&tag) {
                    end_tag_v_id = Some(*v_id);
                } else {
                    end_tag_v_id = Some(assign_vertex_id);
                    tag_v_id_map.insert(tag.clone(), assign_vertex_id);
                    assign_vertex_id += 1;
                }
            } else {
                end_tag_v_id = None;
            }
            let end_tag_label = end_tag_v_id.and_then(|v_id| id_label_map.get(&v_id).cloned());
            let mut pre_dst_vertex_id: PatternId = PatternId::default();
            let mut pre_dst_vertex_label: PatternLabelId = PatternLabelId::default();
            for (i, binder) in sentence.binders.iter().enumerate() {
                if let Some(BinderItem::Edge(edge_expand)) = binder.item.as_ref() {
                    if edge_expand.is_edge {
                        return Err(IrError::Unsupported("Expand only edge is not supported".to_string()));
                    }
                    if let Some(params) = edge_expand.params.as_ref() {
                        if params.tables.len() != 1 {
                            return Err(IrError::Unsupported(
                                "FuzzyPattern: more than 1 expand label".to_string(),
                            ));
                        }
                        if let Some(TagItem::Name(edge_label_name)) = params.tables[0].item.as_ref() {
                            if let Some(edge_label_id) = pattern_meta.get_edge_label_id(edge_label_name) {
                                let edge_id = assign_edge_id;
                                assign_edge_id += 1;
                                let (is_head, is_tail) = if sentence.binders.len() == 1 {
                                    (true, true)
                                } else if i == 0 {
                                    (true, false)
                                } else if i == sentence.binders.len() - 1 {
                                    (false, true)
                                } else {
                                    (false, false)
                                };
                                let src_vertex_id: PatternId;
                                let dst_vertex_id: PatternId;
                                if is_head {
                                    src_vertex_id = start_tag_v_id;
                                } else {
                                    src_vertex_id = pre_dst_vertex_id;
                                }
                                if is_tail {
                                    if let Some(v_id) = end_tag_v_id {
                                        dst_vertex_id = v_id;
                                    } else {
                                        dst_vertex_id = assign_vertex_id;
                                        assign_vertex_id += 1;
                                    }
                                } else {
                                    dst_vertex_id = assign_vertex_id;
                                    pre_dst_vertex_id = dst_vertex_id;
                                    assign_vertex_id += 1;
                                }
                                // check alias tag
                                if let Some(alias_tag) = edge_expand
                                    .alias
                                    .as_ref()
                                    .and_then(|name_or_id| name_or_id.item.as_ref())
                                {
                                    let tag = match alias_tag {
                                        TagItem::Name(name) => NameOrId::Str(name.clone()),
                                        TagItem::Id(id) => NameOrId::Id(*id),
                                    };
                                    match tag_v_id_map.get(&tag) {
                                        Some(v_id) => {
                                            if *v_id != dst_vertex_id {
                                                return Err(IrError::InvalidPattern(
                                                    "Two vertices use same tag".to_string(),
                                                ));
                                            }
                                        }
                                        None => {
                                            tag_v_id_map.insert(tag, dst_vertex_id);
                                        }
                                    }
                                }
                                // add vertex properties(column)
                                for property in params.columns.iter() {
                                    if let Some(item) = property.item.as_ref() {
                                        v_id_properties_map
                                            .entry(dst_vertex_id)
                                            .or_insert(Vec::new())
                                            .push(match item {
                                                TagItem::Name(name) => NameOrId::Str(name.clone()),
                                                TagItem::Id(id) => NameOrId::Id(*id),
                                            });
                                    }
                                }
                                // add vertex predicate
                                if let Some(expr) = params.predicate.as_ref() {
                                    v_id_predicate_map.insert(dst_vertex_id, expr.clone());
                                }
                                // assign vertices labels
                                let vertex_labels_candies: DynIter<(
                                    PatternLabelId,
                                    PatternLabelId,
                                    PatternDirection,
                                )> = match edge_expand.direction {
                                    // Outgoing
                                    0 => Box::new(
                                        pattern_meta
                                            .associated_vlabels_iter_by_elabel(edge_label_id)
                                            .map(|(src_v_label, dst_v_label)| {
                                                (src_v_label, dst_v_label, PatternDirection::Out)
                                            }),
                                    ),
                                    // Incoming
                                    1 => Box::new(
                                        pattern_meta
                                            .associated_vlabels_iter_by_elabel(edge_label_id)
                                            .map(|(src_v_label, dst_v_label)| {
                                                (dst_v_label, src_v_label, PatternDirection::In)
                                            }),
                                    ),
                                    2 => Box::new(
                                        pattern_meta
                                            .associated_vlabels_iter_by_elabel(edge_label_id)
                                            .map(|(src_v_label, dst_v_label)| {
                                                (src_v_label, dst_v_label, PatternDirection::Out)
                                            })
                                            .chain(
                                                pattern_meta
                                                    .associated_vlabels_iter_by_elabel(edge_label_id)
                                                    .map(|(src_v_label, dst_v_label)| {
                                                        (dst_v_label, src_v_label, PatternDirection::In)
                                                    }),
                                            ),
                                    ),
                                    _ => {
                                        return Err(IrError::InvalidPattern(
                                            "Invalid Direction".to_string(),
                                        ));
                                    }
                                };
                                let mut src_vertex_label: Option<PatternLabelId> = None;
                                let mut dst_vertex_label: Option<PatternLabelId> = None;
                                let mut direction: Option<PatternDirection> = None;
                                if is_head && is_tail {
                                    for (src_vlabel_cand, dst_vlabel_cand, dir) in vertex_labels_candies {
                                        if let (Some(head_label), Some(tail_label)) =
                                            (start_tag_label, end_tag_label)
                                        {
                                            if head_label == src_vlabel_cand
                                                && tail_label == dst_vlabel_cand
                                            {
                                                src_vertex_label = Some(head_label);
                                                dst_vertex_label = Some(tail_label);
                                                direction = Some(dir);
                                                break;
                                            }
                                        } else if let Some(head_label) = start_tag_label {
                                            if head_label == src_vlabel_cand {
                                                src_vertex_label = Some(head_label);
                                                dst_vertex_label = Some(dst_vlabel_cand);
                                                id_label_map.insert(dst_vertex_id, dst_vlabel_cand);
                                                direction = Some(dir);
                                                break;
                                            }
                                        } else if let Some(tail_label) = end_tag_label {
                                            if tail_label == dst_vlabel_cand {
                                                src_vertex_label = Some(src_vlabel_cand);
                                                id_label_map.insert(src_vertex_id, src_vlabel_cand);
                                                dst_vertex_label = Some(tail_label);
                                                direction = Some(dir);
                                                break;
                                            }
                                        } else {
                                            src_vertex_label = Some(src_vlabel_cand);
                                            id_label_map.insert(src_vertex_id, src_vlabel_cand);
                                            dst_vertex_label = Some(dst_vlabel_cand);
                                            id_label_map.insert(dst_vertex_id, dst_vlabel_cand);
                                            direction = Some(dir);
                                            break;
                                        }
                                    }
                                } else if is_head {
                                    for (src_vlabel_cand, dst_vlabel_cand, dir) in vertex_labels_candies {
                                        if let Some(head_label) = start_tag_label {
                                            if head_label == src_vlabel_cand {
                                                src_vertex_label = Some(head_label);
                                                dst_vertex_label = Some(dst_vlabel_cand);
                                                pre_dst_vertex_label = dst_vlabel_cand;
                                                direction = Some(dir);
                                                break;
                                            }
                                        } else {
                                            src_vertex_label = Some(src_vlabel_cand);
                                            id_label_map.insert(src_vertex_id, src_vlabel_cand);
                                            dst_vertex_label = Some(dst_vlabel_cand);
                                            pre_dst_vertex_label = dst_vlabel_cand;
                                            direction = Some(dir);
                                            break;
                                        }
                                    }
                                } else if is_tail {
                                    for (src_vlabel_cand, dst_vlabel_cand, dir) in vertex_labels_candies {
                                        if let Some(tail_label) = end_tag_label {
                                            if tail_label == dst_vlabel_cand
                                                && pre_dst_vertex_label == src_vlabel_cand
                                            {
                                                src_vertex_label = Some(pre_dst_vertex_label);
                                                dst_vertex_label = Some(tail_label);
                                                direction = Some(dir);
                                                break;
                                            }
                                        } else if src_vlabel_cand == pre_dst_vertex_label {
                                            src_vertex_label = Some(pre_dst_vertex_label);
                                            dst_vertex_label = Some(dst_vlabel_cand);
                                            id_label_map.insert(dst_vertex_id, dst_vlabel_cand);
                                            direction = Some(dir);
                                            break;
                                        }
                                    }
                                } else {
                                    for (src_vlabel_cand, dst_vlabel_cand, dir) in vertex_labels_candies {
                                        if src_vlabel_cand == pre_dst_vertex_label {
                                            src_vertex_label = Some(pre_dst_vertex_label);
                                            dst_vertex_label = Some(dst_vlabel_cand);
                                            pre_dst_vertex_label = dst_vlabel_cand;
                                            direction = Some(dir);
                                            break;
                                        }
                                    }
                                }
                                if let (Some(src_vertex_label), Some(dst_vertex_label), Some(direction)) =
                                    (src_vertex_label, dst_vertex_label, direction)
                                {
                                    if let PatternDirection::Out = direction {
                                        pattern_edges.push(PatternEdge::new(
                                            edge_id,
                                            edge_label_id,
                                            src_vertex_id,
                                            dst_vertex_id,
                                            src_vertex_label,
                                            dst_vertex_label,
                                        ));
                                    } else if let PatternDirection::In = direction {
                                        pattern_edges.push(PatternEdge::new(
                                            edge_id,
                                            edge_label_id,
                                            dst_vertex_id,
                                            src_vertex_id,
                                            dst_vertex_label,
                                            src_vertex_label,
                                        ));
                                    }
                                } else {
                                    return Err(IrError::InvalidPattern(
                                        "Cannot find valid label for some vertices".to_string(),
                                    ));
                                }
                            } else {
                                return Err(IrError::InvalidPattern(
                                    "Cannot find edge label info in the pattern meta".to_string(),
                                ));
                            }
                        } else {
                            return Err(IrError::Unsupported(
                                "FuzzyPattern: edge expand doesn't have label".to_string(),
                            ));
                        }
                    } else {
                        return Err(IrError::Unsupported(
                            "FuzzyPattern: edge expand doesn't have parameters".to_string(),
                        ));
                    }
                } else {
                    return Err(IrError::InvalidPattern("Binder's item is none".to_string()));
                }
            }
        }
        Pattern::try_from(pattern_edges).and_then(|mut pattern| {
            pattern.vertex_tag_map = tag_v_id_map;
            pattern.vertex_properties_map = v_id_properties_map;
            pattern.vertex_predicate_map = v_id_predicate_map;
            Ok(pattern)
        })
    }
}

/// Methods to access the fields of a Pattern or get some info from Pattern
impl Pattern {
    /// Iterate Edges
    pub fn edges_iter(&self) -> DynIter<&PatternEdge> {
        Box::new(self.edges.iter().map(|(_, edge)| edge))
    }

    /// Iterate Vertices
    pub fn vertices_iter(&self) -> DynIter<&PatternVertex> {
        Box::new(self.vertices.iter().map(|(_, vertex)| vertex))
    }

    /// Iterate Edge Labels with Edges has this Label
    pub fn edge_label_map_iter(&self) -> DynIter<(PatternLabelId, &BTreeSet<PatternId>)> {
        Box::new(
            self.edge_label_map
                .iter()
                .map(|(edge_label, edges)| (*edge_label, edges)),
        )
    }

    /// Iterate Vertex Labels with Vertices has this Label
    pub fn vertex_label_map_iter(&self) -> DynIter<(PatternLabelId, &BTreeSet<PatternId>)> {
        Box::new(
            self.vertex_label_map
                .iter()
                .map(|(vertex_label, vertices)| (*vertex_label, vertices)),
        )
    }

    /// Get PatternEdge Reference from Edge ID
    pub fn get_edge_from_id(&self, edge_id: PatternId) -> Option<&PatternEdge> {
        self.edges.get(edge_id)
    }

    /// Get PatternVertex Reference from Vertex ID
    pub fn get_vertex_from_id(&self, vertex_id: PatternId) -> Option<&PatternVertex> {
        self.vertices.get(vertex_id)
    }

    /// Get PatternEdge Mutable Reference from Edge ID
    pub fn get_edge_mut_from_id(&mut self, edge_id: PatternId) -> Option<&mut PatternEdge> {
        self.edges.get_mut(edge_id)
    }

    /// Get PatternVertex Mutable Reference from Vertex ID
    pub fn get_vertex_mut_from_id(&mut self, vertex_id: PatternId) -> Option<&mut PatternVertex> {
        self.vertices.get_mut(vertex_id)
    }

    /// Get PatternEdge Reference from Given Tag
    pub fn get_edge_from_tag(&self, edge_tag: &NameOrId) -> Option<&PatternEdge> {
        self.edge_tag_map
            .get(edge_tag)
            .and_then(|id| self.edges.get(*id))
    }

    /// Get PatternVertex Reference from Given Tag
    pub fn get_vertex_from_tag(&self, vertex_tag: &NameOrId) -> Option<&PatternEdge> {
        self.vertex_tag_map
            .get(vertex_tag)
            .and_then(|id| self.edges.get(*id))
    }

    /// Assign a PatternEdge of the Pattern with the Given Tag
    pub fn add_edge_tag(&mut self, edge_tag: &NameOrId, edge_id: PatternId) {
        self.edge_tag_map
            .insert(edge_tag.clone(), edge_id);
    }

    /// Assign a PatternVertex of th Pattern with the Given Tag
    pub fn add_vertex_tag(&mut self, vertex_tag: &NameOrId, vertex_id: PatternId) {
        self.vertex_tag_map
            .insert(vertex_tag.clone(), vertex_id);
    }

    /// Get Vertex Index from Vertex ID Reference
    pub fn get_vertex_rank(&self, v_id: PatternId) -> PatternRankId {
        self.vertices.get(v_id).unwrap().rank
    }

    /// Get the rank of both start and end vertices of an edge
    pub fn get_edge_vertices_rank(&self, edge_id: PatternId) -> Option<(PatternRankId, PatternRankId)> {
        if let Some(edge) = self.get_edge_from_id(edge_id) {
            let start_v_rank = self.get_vertex_rank(edge.start_v_id);
            let end_v_rank = self.get_vertex_rank(edge.end_v_id);
            Some((start_v_rank, end_v_rank))
        } else {
            None
        }
    }

    /// Get the total number of edges in the pattern
    pub fn get_edge_num(&self) -> usize {
        self.edges.len()
    }

    /// Get the total number of vertices in the pattern
    pub fn get_vertex_num(&self) -> usize {
        self.vertices.len()
    }

    /// Get the total number of edge labels in the pattern
    pub fn get_edge_label_num(&self) -> usize {
        self.edge_label_map.len()
    }

    /// Get the total number of vertex labels in the pattern
    pub fn get_vertex_label_num(&self) -> usize {
        self.vertex_label_map.len()
    }

    /// Get the order of both start and end vertices of an edge
    pub fn get_edge_vertices_index(&self, edge_id: PatternId) -> Option<(PatternRankId, PatternRankId)> {
        if let Some(edge) = self.get_edge_from_id(edge_id) {
            let start_v_index = self.get_vertex_rank(edge.start_v_id);
            let end_v_index = self.get_vertex_rank(edge.end_v_id);
            Some((start_v_index, end_v_index))
        } else {
            None
        }
    }

    /// Get the maximum edge label id of the current pattern
    pub fn get_max_edge_label(&self) -> Option<PatternLabelId> {
        match self.edge_label_map.iter().last() {
            Some((max_label, _)) => Some(*max_label),
            None => None,
        }
    }

    /// Get the maximum vertex label id of the current pattern
    pub fn get_max_vertex_label(&self) -> Option<PatternLabelId> {
        match self.vertex_label_map.iter().last() {
            Some((max_label, _)) => Some(*max_label),
            None => None,
        }
    }

    /// Compute at least how many bits are needed to represent edge labels
    /// At least 1 bit
    pub fn get_min_edge_label_bit_num(&self) -> usize {
        if let Some(max_edge_label) = self.get_max_edge_label() {
            std::cmp::max((32 - max_edge_label.leading_zeros()) as usize, 1)
        } else {
            1
        }
    }

    /// Compute at least how many bits are needed to represent vertex labels
    /// At least 1 bit
    pub fn get_min_vertex_label_bit_num(&self) -> usize {
        if let Some(max_vertex_label) = self.get_max_vertex_label() {
            std::cmp::max((32 - max_vertex_label.leading_zeros()) as usize, 1)
        } else {
            1
        }
    }

    /// Compute at least how many bits are needed to represent vertices with the same label
    /// At least 1 bit
    pub fn get_min_vertex_rank_bit_num(&self) -> usize {
        // iterate through the hashmap and compute how many vertices have the same label in one set
        let mut min_rank_bit_num: usize = 1;
        for (_, value) in self.vertex_label_map.iter() {
            let same_label_vertex_num = value.len() as u64;
            min_rank_bit_num =
                std::cmp::max((64 - same_label_vertex_num.leading_zeros()) as usize, min_rank_bit_num);
        }
        min_rank_bit_num
    }

    /// Iterate over a PatternEdge's all required properties
    pub fn edge_properties_iter(&self, edge_id: PatternId) -> DynIter<&NameOrId> {
        match self.edge_properties_map.get(&edge_id) {
            Some(properties) => Box::new(properties.iter()),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Iterate over a PatternVertex's all required properties
    pub fn vertex_properties_iter(&self, vertex_id: PatternId) -> DynIter<&NameOrId> {
        match self.vertex_properties_map.get(&vertex_id) {
            Some(properties) => Box::new(properties.iter()),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Add a new property requirement to the PatternEdge
    pub fn add_edge_property(&mut self, edge_id: PatternId, property: NameOrId) {
        self.edge_properties_map
            .entry(edge_id)
            .or_insert(Vec::new())
            .push(property);
    }

    /// Add a new property requirement to the PatternVertex
    pub fn add_vertex_property(&mut self, vertex_id: PatternId, property: NameOrId) {
        self.vertex_properties_map
            .entry(vertex_id)
            .or_insert(Vec::new())
            .push(property);
    }

    /// Get the predicate requirement of a PatternEdge
    pub fn get_edge_predicate(&self, edge_id: PatternId) -> Option<&common_pb::Expression> {
        self.edge_predicate_map.get(&edge_id)
    }

    /// Get the predicate requirement of a PatternVertex
    pub fn get_vertex_predicate(&self, vertex_id: PatternId) -> Option<&common_pb::Expression> {
        self.vertex_predicate_map.get(&vertex_id)
    }

    /// Add predicate requirement to a PatternEdge
    pub fn add_edge_predicate(&mut self, edge_id: PatternId, predicate: common_pb::Expression) {
        self.edge_predicate_map
            .insert(edge_id, predicate);
    }

    /// Add predicate requirement to a PatternVertex
    pub fn add_vertex_predicate(&mut self, vertex_id: PatternId, predicate: common_pb::Expression) {
        self.vertex_predicate_map
            .insert(vertex_id, predicate);
    }

    /// Given a vertex label, return all vertices with the label
    pub fn get_vertices_from_label(&self, label: PatternLabelId) -> Option<&BTreeSet<PatternId>> {
        self.vertex_label_map.get(&label)
    }
}

/// Methods for PatternEdge Reordering inside a Pattern
impl Pattern {
    /// Get a vector of ordered edges's rankes of a Pattern
    /// The comparison is based on the `cmp_edges` method above to get the Order
    pub fn get_ordered_edges(&self) -> Vec<PatternId> {
        let mut ordered_edges: Vec<PatternId> = self
            .edges
            .iter()
            .map(|(edge_id, _)| edge_id)
            .collect();
        ordered_edges.sort_by(|e1_id, e2_id| self.cmp_edges(*e1_id, *e2_id));
        ordered_edges
    }

    /// Get the Order of two PatternEdges in a Pattern
    /// Vertex Indices are taken into consideration
    fn cmp_edges(&self, e1_id: PatternId, e2_id: PatternId) -> Ordering {
        if e1_id == e2_id {
            return Ordering::Equal;
        }
        let e1 = self.edges.get(e1_id).unwrap();
        let e2 = self.edges.get(e2_id).unwrap();
        // Compare the label of starting vertex
        match e1.start_v_label.cmp(&e2.start_v_label) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
        // Compare the label of ending vertex
        match e1.end_v_label.cmp(&e2.end_v_label) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
        // Compare Edge Label
        match e1.label.cmp(&e2.label) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
        // Get orders for starting vertex
        let (e1_start_v_rank, e1_end_v_rank) = self
            .get_edge_vertices_rank(e1.get_id())
            .unwrap();
        let (e2_start_v_rank, e2_end_v_rank) = self
            .get_edge_vertices_rank(e2.get_id())
            .unwrap();
        // Compare the order of the starting vertex
        match e1_start_v_rank.cmp(&e2_start_v_rank) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
        // Compare the order of ending vertex
        match e1_end_v_rank.cmp(&e2_end_v_rank) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
        // Return as equal if still cannot distinguish
        Ordering::Equal
    }
}

/// Naive Rank Ranking Method
impl Pattern {
    /// Set Rank for Every Vertex within the Pattern
    ///
    /// Basic Idea: Iteratively update the rank locally (neighbor edges vertices) and update the order of neighbor edges
    pub fn vertex_ranking(&mut self) {
        // Get the Neighbor Edges for All Vertices, Sorted Simply By Labels
        let mut vertex_neighbor_edges_map: HashMap<
            PatternId,
            Vec<(PatternId, PatternId, PatternDirection)>,
        > = self.get_vertex_neighbor_edges();
        // Initially, mark all the vertices as unknown
        let mut unknown_vertex_set: HashSet<PatternId> = HashSet::new();
        for vertex in self.vertices_iter() {
            unknown_vertex_set.insert(vertex.get_id());
        }
        // Set initial ranks by considering the labels & in/out degrees
        let mut is_rank_changed: bool = self
            .set_initial_rank(&vertex_neighbor_edges_map, &mut unknown_vertex_set)
            .unwrap();
        loop {
            // Stop until the ranks can no longer be updated
            if !is_rank_changed {
                break;
            }
            // Update the Order of Neighbor Edges Based on the ranks of end vertices only
            self.update_neighbor_edges_map(&mut vertex_neighbor_edges_map);
            // Update vertex ranks by newly updated ranks in the last iteration
            is_rank_changed = self
                .update_rank(&vertex_neighbor_edges_map, &mut unknown_vertex_set)
                .unwrap();
        }
    }

    /// Get a vector of neighboring edges of each vertex
    ///
    /// Used for Comparing Vertices when Setting Initial Indices
    ///
    /// The vector of neighbor edges consists of two parts: outgoing edges and incoming edges
    /// i.e. [ sorted outgoing edges | sorted incoming edges ]
    ///
    /// Each neighbor edge element stores 3 values: edge id, target vertex id, and edge direction
    fn get_vertex_neighbor_edges(
        &self,
    ) -> HashMap<PatternId, Vec<(PatternId, PatternId, PatternDirection)>> {
        let mut vertex_neighbor_edges_map: HashMap<
            PatternId,
            Vec<(PatternId, PatternId, PatternDirection)>,
        > = HashMap::new();
        for vertex in self.vertices_iter() {
            let v_id: PatternId = vertex.get_id();
            let mut outgoing_edges: Vec<(PatternId, PatternId, PatternDirection)> =
                Vec::with_capacity(vertex.get_out_degree());
            let mut incoming_edges: Vec<(PatternId, PatternId, PatternDirection)> =
                Vec::with_capacity(vertex.get_in_degree());
            for (e_id, end_v_id, edge_dir) in vertex.adjacent_edges_iter() {
                match edge_dir {
                    PatternDirection::Out => outgoing_edges.push((e_id, end_v_id, edge_dir)),
                    PatternDirection::In => incoming_edges.push((e_id, end_v_id, edge_dir)),
                }
            }
            // Sort the edges
            outgoing_edges.sort_by(|e1, e2| self.cmp_edges(e1.0, e2.0));
            incoming_edges.sort_by(|e1, e2| self.cmp_edges(e1.0, e2.0));
            // Concat two edge info vector
            outgoing_edges.append(&mut incoming_edges);
            // Insert into the Hashmap
            vertex_neighbor_edges_map.insert(v_id, outgoing_edges);
        }

        vertex_neighbor_edges_map
    }

    /// Set Initial Vertex Index Based on Comparison of Labels and In/Out Degrees
    ///
    /// Return a bool indicating whether the ranks has changed for this initial rank iteration
    ///
    /// If nothing changed, we conclude that the ranks of all vertices have been set and are stable
    fn set_initial_rank(
        &mut self,
        vertex_neighbor_edges_map: &HashMap<PatternId, Vec<(PatternId, PatternId, PatternDirection)>>,
        unknown_vertex_set: &mut HashSet<PatternId>,
    ) -> IrResult<bool> {
        // Mark whether there is a change of vertex rank
        let mut is_rank_changed: bool = false;
        // Stores vertex labels that have been dealth with
        let mut visited_label_set: HashSet<PatternLabelId> = HashSet::new();
        // Store vertices that should be removed from unknown vertex set
        let mut fixed_rank_vertex_vec: Vec<PatternId> = Vec::new();
        // Store the mapping from vertex id to rank
        let mut vertex_rank_map: HashMap<PatternId, PatternRankId> = HashMap::new();
        for v_id in unknown_vertex_set.iter() {
            let v_label: PatternLabelId = self
                .get_vertex_from_id(*v_id)
                .unwrap()
                .get_label();
            if visited_label_set.contains(&v_label) {
                continue;
            }
            visited_label_set.insert(v_label);
            let same_label_vertex_set: &BTreeSet<PatternId> =
                self.get_vertices_from_label(v_label).unwrap();
            let vertex_num = same_label_vertex_set.len();
            let mut vertex_vec: Vec<PatternId> = Vec::with_capacity(vertex_num);
            let mut vertex_rank_vec: Vec<PatternRankId> = vec![0; vertex_num];
            let mut is_rank_fixed_vec: Vec<bool> = vec![true; vertex_num];
            for vertex_id in same_label_vertex_set.iter() {
                vertex_vec.push(*vertex_id);
            }
            for i in 0..vertex_num {
                for j in (i + 1)..vertex_num {
                    match self.cmp_vertices_for_rank(
                        vertex_vec[i],
                        vertex_vec[j],
                        vertex_neighbor_edges_map,
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
                    fixed_rank_vertex_vec.push(vertex_vec[i]);
                }
                // Update Vertex Rank on the map
                vertex_rank_map.insert(vertex_vec[i], vertex_rank_vec[i]);
            }
        }

        // Remove vertices that have fixed rank
        for v_id in fixed_rank_vertex_vec {
            unknown_vertex_set.remove(&v_id);
        }
        // Update vertex rank on the pattern
        for (v_id, v_rank) in vertex_rank_map.iter() {
            if *v_rank != 0 {
                is_rank_changed = true;
                self.get_vertex_mut_from_id(*v_id)
                    .unwrap()
                    .set_rank(*v_rank);
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
        &mut self,
        vertex_neighbor_edges_map: &HashMap<PatternId, Vec<(PatternId, PatternId, PatternDirection)>>,
        unknown_vertex_set: &mut HashSet<PatternId>,
    ) -> IrResult<bool> {
        if unknown_vertex_set.len() == 0 {
            return Ok(false);
        }
        // Mark whether there is a change of vertex rank
        let mut is_rank_changed: bool = false;
        // Stores vertex labels that have been dealth with
        let mut visited_label_set: HashSet<PatternLabelId> = HashSet::new();
        // Store vertices that should be removed from unknown vertex set
        let mut fixed_rank_vertex_vec: Vec<PatternId> = Vec::new();
        // Store the mapping from vertex id to rank
        let mut vertex_rank_map: HashMap<PatternId, PatternRankId> = HashMap::new();
        for v_id in unknown_vertex_set.iter() {
            // let vertex: &PatternVertex = self.get_vertex_from_id(*v_id).unwrap();
            let v_label: PatternLabelId = self
                .get_vertex_from_id(*v_id)
                .unwrap()
                .get_label();
            if visited_label_set.contains(&v_label) {
                continue;
            }
            visited_label_set.insert(v_label);
            let same_label_vertex_set: &BTreeSet<PatternId> =
                self.get_vertices_from_label(v_label).unwrap();
            let mut same_rank_vertex_set: HashMap<PatternRankId, (Vec<PatternId>, Vec<PatternId>)> =
                HashMap::new();
            // Separate vertices according to their ranks
            for vertex_id in same_label_vertex_set.iter() {
                if unknown_vertex_set.contains(vertex_id) {
                    let v_rank: PatternRankId = self
                        .get_vertex_from_id(*vertex_id)
                        .unwrap()
                        .get_rank();
                    if !same_rank_vertex_set.contains_key(&v_rank) {
                        same_rank_vertex_set.insert(v_rank, (Vec::new(), Vec::new()));
                    }
                    // Check if anyone of the neighbor vertices have accurate(unique) rank
                    let mut has_fixed_rank_neighbor: bool = false;
                    let vertex_neighbor_edges: &Vec<(PatternId, PatternId, PatternDirection)> =
                        vertex_neighbor_edges_map
                            .get(vertex_id)
                            .unwrap();
                    for (_, end_v_id, _) in vertex_neighbor_edges.iter() {
                        if !unknown_vertex_set.contains(end_v_id) {
                            has_fixed_rank_neighbor = true;
                            let tuple: &mut (Vec<PatternId>, Vec<PatternId>) =
                                same_rank_vertex_set.get_mut(&v_rank).unwrap();
                            tuple.0.push(*vertex_id);
                            break;
                        }
                    }
                    if !has_fixed_rank_neighbor {
                        let tuple: &mut (Vec<PatternId>, Vec<PatternId>) =
                            same_rank_vertex_set.get_mut(&v_rank).unwrap();
                        tuple.1.push(*vertex_id);
                    }
                }
            }
            // Perform vertex ranking on each groups of vertices with the same label and rank
            for (vertex_set_with_fixed_rank_neighbor, vertex_set_without_fixed_rank_neighbor) in
                same_rank_vertex_set.values()
            {
                // Case-1: All vertices have no fixed rank neighbor
                if vertex_set_with_fixed_rank_neighbor.len() == 0 {
                    let old_rank: PatternRankId = self
                        .get_vertex_from_id(vertex_set_without_fixed_rank_neighbor[0])
                        .unwrap()
                        .get_rank();
                    let vertex_num = vertex_set_without_fixed_rank_neighbor.len();
                    let mut vertex_rank_vec: Vec<PatternRankId> = vec![old_rank; vertex_num];
                    let mut is_rank_fixed_vec: Vec<bool> = vec![true; vertex_num];
                    for i in 0..vertex_num {
                        for j in (i + 1)..vertex_num {
                            match self.cmp_vertices_by_rank(
                                vertex_set_without_fixed_rank_neighbor[i],
                                vertex_set_without_fixed_rank_neighbor[j],
                                vertex_neighbor_edges_map,
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
                            fixed_rank_vertex_vec.push(vertex_set_without_fixed_rank_neighbor[i]);
                        }
                        // Update Vertex Rank on the map
                        vertex_rank_map
                            .insert(vertex_set_without_fixed_rank_neighbor[i], vertex_rank_vec[i]);
                    }
                }
                // Case-2: There exists vertex having fixed rank neighbor
                else {
                    let old_rank: PatternRankId = self
                        .get_vertex_from_id(vertex_set_with_fixed_rank_neighbor[0])
                        .unwrap()
                        .get_rank();
                    let mut sorting_vec: Vec<PatternId> = vertex_set_with_fixed_rank_neighbor.clone();
                    if vertex_set_without_fixed_rank_neighbor.len() > 0 {
                        sorting_vec.push(vertex_set_without_fixed_rank_neighbor[0]);
                    }
                    let vertex_num = sorting_vec.len();
                    let mut vertex_rank_vec: Vec<PatternRankId> = vec![old_rank; vertex_num];
                    let mut is_rank_fixed_vec: Vec<bool> = vec![true; vertex_num];
                    for i in 0..vertex_num {
                        for j in (i + 1)..vertex_num {
                            match self.cmp_vertices_by_rank(
                                sorting_vec[i],
                                sorting_vec[j],
                                vertex_neighbor_edges_map,
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
                            fixed_rank_vertex_vec.push(sorting_vec[i]);
                        }
                        if i == vertex_num - 1
                            && vertex_set_without_fixed_rank_neighbor.len() == 0
                            && is_rank_fixed_vec[i]
                        {
                            fixed_rank_vertex_vec.push(sorting_vec[i]);
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
        for v_id in fixed_rank_vertex_vec {
            unknown_vertex_set.remove(&v_id);
        }
        // Update vertex rank on the pattern
        for (v_id, v_rank) in vertex_rank_map.iter() {
            let old_rank: PatternRankId = self
                .get_vertex_from_id(*v_id)
                .unwrap()
                .get_rank();
            if *v_rank != old_rank {
                is_rank_changed = true;
                self.get_vertex_mut_from_id(*v_id)
                    .unwrap()
                    .set_rank(*v_rank);
            }
        }

        Ok(is_rank_changed)
    }

    /// Compare the ranks of two PatternVertices
    /// 
    /// Called when setting initial ranks
    fn cmp_vertices_for_rank(
        &self,
        v1_id: PatternId,
        v2_id: PatternId,
        vertex_neighbor_edges_map: &HashMap<PatternId, Vec<(PatternId, PatternId, PatternDirection)>>,
    ) -> Ordering {
        let v1 = self.vertices.get(v1_id).unwrap();
        let v2 = self.vertices.get(v2_id).unwrap();
        match v1.get_label().cmp(&v2.get_label()) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
        // Compare Vertex Out Degree
        match v1.get_out_degree().cmp(&v2.get_out_degree()) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
        // Compare Vertex In Degree
        match v1.get_in_degree().cmp(&v2.get_in_degree()) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
        // Compare the ranks
        match v1.get_rank().cmp(&v2.get_rank()) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }

        let v1_connected_edges = vertex_neighbor_edges_map.get(&v1_id).unwrap();
        let v2_connected_edges = vertex_neighbor_edges_map.get(&v2_id).unwrap();
        for i in 0..v1_connected_edges.len() {
            let v1_connected_edge_label = self
                .get_edge_from_id(v1_connected_edges[i].0)
                .unwrap()
                .get_label();
            let v1_connected_edge_end_v_label = self
                .get_vertex_from_id(v1_connected_edges[i].1)
                .unwrap()
                .get_label();
            let v2_connected_edge_label = self
                .get_edge_from_id(v2_connected_edges[i].0)
                .unwrap()
                .get_label();
            let v2_connected_edge_end_v_label = self
                .get_vertex_from_id(v2_connected_edges[i].1)
                .unwrap()
                .get_label();
            // Compare End Vertex Label
            match v1_connected_edge_end_v_label.cmp(&v2_connected_edge_end_v_label) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                _ => (),
            }
            // Compare Edge Label
            match v1_connected_edge_label.cmp(&v2_connected_edge_label) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                _ => (),
            }
            // Compare End Vertex Rank
            let v1_connected_end_v_rank: PatternRankId = self
                .get_vertex_from_id(v1_connected_edges[i].1)
                .unwrap()
                .get_rank();
            let v2_connected_end_v_rank: PatternRankId = self
                .get_vertex_from_id(v2_connected_edges[i].1)
                .unwrap()
                .get_rank();
            match v1_connected_end_v_rank.cmp(&v2_connected_end_v_rank) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                _ => (),
            }
        }
        // Return Equal if Still Cannot Distinguish
        Ordering::Equal
    }

    /// Compare the ranks of two PatternVertices
    /// 
    /// Called when updating vertex ranks as only ranks need to be considered
    fn cmp_vertices_by_rank(
        &self,
        v1_id: PatternId,
        v2_id: PatternId,
        vertex_neighbor_edges_map: &HashMap<PatternId, Vec<(PatternId, PatternId, PatternDirection)>>,
    ) -> Ordering {
        let v1 = self.vertices.get(v1_id).unwrap();
        let v2 = self.vertices.get(v2_id).unwrap();
        // Compare the ranks
        match v1.get_rank().cmp(&v2.get_rank()) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }

        let v1_connected_edges = vertex_neighbor_edges_map.get(&v1_id).unwrap();
        let v2_connected_edges = vertex_neighbor_edges_map.get(&v2_id).unwrap();
        for i in 0..v1_connected_edges.len() {
            // Compare End Vertex Rank
            let v1_connected_end_v_rank: PatternRankId = self
                .get_vertex_from_id(v1_connected_edges[i].1)
                .unwrap()
                .get_rank();
            let v2_connected_end_v_rank: PatternRankId = self
                .get_vertex_from_id(v2_connected_edges[i].1)
                .unwrap()
                .get_rank();
            match v1_connected_end_v_rank.cmp(&v2_connected_end_v_rank) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                _ => (),
            }
        }
        // Return Equal if Still Cannot Distinguish
        Ordering::Equal
    }

    // Update the Order of Neighbor Vertices Based on the ranks of end vertices only
    fn update_neighbor_edges_map(
        &self,
        vertex_neighbor_edges_map: &mut HashMap<PatternId, Vec<(PatternId, PatternId, PatternDirection)>>,
    ) {
        for vertex in self.vertices_iter() {
            vertex_neighbor_edges_map
                .get_mut(&vertex.get_id())
                .unwrap()
                .sort_by(|e1, e2| {
                    self.get_vertex_from_id(e1.1)
                        .unwrap()
                        .get_rank()
                        .cmp(
                            &self
                                .get_vertex_from_id(e2.1)
                                .unwrap()
                                .get_rank(),
                        )
                });
        }
    }
}

/// Methods for Pattern Extension
impl Pattern {
    /// Get all the vertices(id) with the same vertex label and vertex rank
    /// These vertices are equivalent in the Pattern
    fn get_equivalent_vertices(&self, v_label: PatternLabelId, v_rank: PatternRankId) -> Vec<PatternId> {
        let mut equivalent_vertices = vec![];
        if let Some(vs_with_same_label) = self.vertex_label_map.get(&v_label) {
            for v_id in vs_with_same_label {
                if let Some(vertex) = self.vertices.get(*v_id) {
                    if vertex.rank == v_rank {
                        equivalent_vertices.push(*v_id);
                    }
                }
            }
        }
        equivalent_vertices
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

    fn add_edge(&mut self, edge: &PatternEdge) -> IrResult<()> {
        // Error that the adding edge already exist
        if self.edges.contains_key(edge.get_id()) {
            return Err(IrError::InvalidCode("The adding edge already existed".to_string()));
        }
        if let (None, None) = (
            self.get_vertex_from_id(edge.get_start_vertex_id()),
            self.get_vertex_from_id(edge.get_end_vertex_id()),
        ) {
            return Err(IrError::InvalidCode("The adding edge cannot connect to the pattern".to_string()));
        } else if let None = self.get_vertex_from_id(edge.get_start_vertex_id()) {
            let start_vertex_id = edge.get_start_vertex_id();
            let start_vertex_label = edge.get_start_vertex_label();
            self.vertices.insert(
                start_vertex_id,
                PatternVertex {
                    id: start_vertex_id,
                    label: start_vertex_label,
                    rank: 0,
                    adjacent_edges: BTreeMap::new(),
                    adjacent_vertices: BTreeMap::new(),
                    out_degree: 0,
                    in_degree: 0,
                },
            );
            self.vertex_label_map
                .entry(start_vertex_label)
                .or_insert(BTreeSet::new())
                .insert(start_vertex_id);
        } else if let None = self.get_vertex_from_id(edge.get_end_vertex_id()) {
            let end_vertex_id = edge.get_end_vertex_id();
            let end_vertex_label = edge.get_end_vertex_label();
            self.vertices.insert(
                end_vertex_id,
                PatternVertex {
                    id: end_vertex_id,
                    label: end_vertex_label,
                    rank: 0,
                    adjacent_edges: BTreeMap::new(),
                    adjacent_vertices: BTreeMap::new(),
                    out_degree: 0,
                    in_degree: 0,
                },
            );
            self.vertex_label_map
                .entry(end_vertex_label)
                .or_insert(BTreeSet::new())
                .insert(end_vertex_id);
        }
        if let Some(start_vertex) = self.get_vertex_mut_from_id(edge.get_start_vertex_id()) {
            start_vertex
                .adjacent_edges
                .insert(edge.get_id(), (edge.get_end_vertex_id(), PatternDirection::Out));
            start_vertex
                .adjacent_vertices
                .entry(edge.get_end_vertex_id())
                .or_insert(vec![])
                .push((edge.get_id(), PatternDirection::Out));
            start_vertex.out_degree += 1;
        }
        if let Some(end_vertex) = self.get_vertex_mut_from_id(edge.get_end_vertex_id()) {
            end_vertex
                .adjacent_edges
                .insert(edge.get_id(), (edge.get_start_vertex_id(), PatternDirection::In));
            end_vertex
                .adjacent_vertices
                .entry(edge.get_start_vertex_id())
                .or_insert(vec![])
                .push((edge.get_id(), PatternDirection::In));
            end_vertex.in_degree += 1;
        }
        self.edges.insert(edge.get_id(), edge.clone());
        self.edge_label_map
            .entry(edge.get_label())
            .or_insert(BTreeSet::new())
            .insert(edge.get_id());
        Ok(())
    }

    pub fn extend_by_edges<'a, T>(&self, edges: T) -> Option<Pattern>
    where
        T: Iterator<Item = &'a PatternEdge>,
    {
        let mut new_pattern = self.clone();
        for edge in edges {
            if let Err(_) = new_pattern.add_edge(edge) {
                return None;
            }
        }
        new_pattern.vertex_ranking();
        Some(new_pattern)
    }

    /// Extend the current Pattern to a new Pattern with the given ExtendStep
    /// If the ExtendStep is not matched with the current Pattern, the function will return None
    /// Else, it will return the new Pattern after the extension
    /// The ExtendStep is not mathced with the current Pattern if:
    /// 1. Some extend edges of the ExtendStep cannot find a correponsponding source vertex in current Pattern
    /// (the required source vertex doesn't exist or already occupied by other extend edges)
    /// 2. Or meet some limitations(e.g. limit the length of Pattern)
    pub fn extend(&self, extend_step: ExtendStep) -> Option<Pattern> {
        let mut new_pattern = self.clone();
        let target_v_label = extend_step.get_target_v_label();
        let mut new_pattern_vertex = PatternVertex {
            id: new_pattern.get_next_pattern_vertex_id(),
            label: target_v_label,
            rank: 0,
            adjacent_edges: BTreeMap::new(),
            adjacent_vertices: BTreeMap::new(),
            out_degree: 0,
            in_degree: 0,
        };
        for ((v_label, v_rank), extend_edges) in extend_step.iter() {
            // Get all the vertices which can be used to extend with these extend edges
            let vertices_can_use = self.get_equivalent_vertices(*v_label, *v_rank);
            // There's no enough vertices to extend, just return None
            if vertices_can_use.len() < extend_edges.len() {
                return None;
            }
            // Connect each vertex can be use to each extend edge
            for i in 0..extend_edges.len() {
                let extend_vertex_id = vertices_can_use[i];
                let extend_dir = extend_edges[i].get_direction();
                // new pattern edge info
                let (mut start_v_id, mut end_v_id, mut start_v_label, mut end_v_label) = (
                    extend_vertex_id,
                    new_pattern_vertex.id,
                    self.vertices
                        .get(vertices_can_use[i])
                        .unwrap()
                        .label,
                    new_pattern_vertex.label,
                );
                if let PatternDirection::In = extend_dir {
                    // (start_v_id, end_v_id) = (end_v_id, start_v_id);
                    // (start_v_label, end_v_label) = (end_v_label, start_v_label);
                    std::mem::swap(&mut start_v_id, &mut end_v_id);
                    std::mem::swap(&mut start_v_label, &mut end_v_label);
                }
                let new_pattern_edge = PatternEdge {
                    id: new_pattern.get_next_pattern_edge_id(),
                    label: extend_edges[i].get_edge_label(),
                    start_v_id,
                    end_v_id,
                    start_v_label,
                    end_v_label,
                };
                // update newly extended pattern vertex's adjacency info
                new_pattern_vertex
                    .adjacent_edges
                    .insert(new_pattern_edge.id, (extend_vertex_id, extend_dir.reverse()));
                new_pattern_vertex
                    .adjacent_vertices
                    .insert(extend_vertex_id, vec![(new_pattern_edge.id, extend_dir.reverse())]);
                if let PatternDirection::Out = extend_dir {
                    new_pattern_vertex.in_degree += 1;
                } else {
                    new_pattern_vertex.out_degree += 1
                }
                // update extend vertex's adjacancy info
                let extend_vertex = new_pattern
                    .get_vertex_mut_from_id(extend_vertex_id)
                    .unwrap();
                extend_vertex
                    .adjacent_edges
                    .insert(new_pattern_edge.id, (new_pattern_vertex.id, extend_dir));
                extend_vertex
                    .adjacent_vertices
                    .insert(new_pattern_vertex.id, vec![(new_pattern_edge.id, extend_dir)]);
                if let PatternDirection::Out = extend_dir {
                    extend_vertex.out_degree += 1;
                } else {
                    extend_vertex.in_degree += 1
                }
                // Add the new pattern edge info to the new Pattern
                new_pattern
                    .edge_label_map
                    .entry(new_pattern_edge.label)
                    .or_insert(BTreeSet::new())
                    .insert(new_pattern_edge.id);
                new_pattern
                    .edges
                    .insert(new_pattern_edge.id, new_pattern_edge);
            }
        }
        // Add the newly extended pattern vertex to the new pattern
        new_pattern
            .vertex_label_map
            .entry(new_pattern_vertex.label)
            .or_insert(BTreeSet::new())
            .insert(new_pattern_vertex.id);
        new_pattern
            .vertices
            .insert(new_pattern_vertex.id, new_pattern_vertex);
        new_pattern.vertex_ranking();
        Some(new_pattern)
    }

    // pub fn de_extend(&self, extend_step: ExtendStep) -> Option<Pattern> {
    //     let mut new_pattern = self.clone();
    //     let target_v_label = extend_step.get_target_v_label();
    //     let target_vertex =
    // }

    /// Find all possible ExtendSteps of current pattern based on the given Pattern Meta
    pub fn get_extend_steps(
        &self, pattern_meta: &PatternMeta, same_label_vertex_limit: usize,
    ) -> Vec<ExtendStep> {
        let mut extend_steps = vec![];
        // Get all vertex labels from pattern meta as the possible extend target vertex
        let target_v_labels = pattern_meta.vertex_label_ids_iter();
        // For every possible extend target vertex label, find its all adjacent edges to the current pattern
        for target_v_label in target_v_labels {
            if self
                .vertex_label_map
                .get(&target_v_label)
                .map(|v_ids| v_ids.len())
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
                    pattern_meta.associated_elabels_iter_by_vlabel(src_vertex.label, target_v_label);
                // Transform all the adjacent edges to ExtendEdge and add to extend_edges_with_src_id
                for adjacent_edge in adjacent_edges {
                    let extend_edge = ExtendEdge::new(
                        src_vertex.label,
                        src_vertex.rank,
                        adjacent_edge.0,
                        adjacent_edge.1,
                    );
                    extend_edges_with_src_id.push((extend_edge, src_vertex.id));
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
                let extend_step = ExtendStep::from((
                    target_v_label,
                    extend_edges
                        .into_iter()
                        .map(|(extend_edge, _)| extend_edge)
                        .collect(),
                ));
                extend_steps.push(extend_step);
            }
        }
        extend_steps
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use vec_map::VecMap;

    use super::Pattern;
    use super::PatternDirection;
    use crate::catalogue::codec::*;
    use crate::catalogue::pattern::PatternEdge;
    use crate::catalogue::pattern::PatternVertex;
    use crate::catalogue::test_cases::extend_step_cases::*;
    use crate::catalogue::test_cases::pattern_cases::*;
    use crate::catalogue::test_cases::pattern_meta_cases::*;
    use crate::catalogue::PatternId;

    /// Test whether the structure of pattern_case1 is the same as our previous description
    #[test]
    fn test_pattern_case1_structure() {
        let pattern_case1 = build_pattern_case1();
        let edges_num = pattern_case1.edges.len();
        assert_eq!(edges_num, 2);
        let vertices_num = pattern_case1.vertices.len();
        assert_eq!(vertices_num, 2);
        let edges_with_label_0 = pattern_case1.edge_label_map.get(&0).unwrap();
        assert_eq!(edges_with_label_0.len(), 2);
        let mut edges_with_label_0_iter = edges_with_label_0.iter();
        assert_eq!(*edges_with_label_0_iter.next().unwrap(), 0);
        assert_eq!(*edges_with_label_0_iter.next().unwrap(), 1);
        let vertices_with_label_0 = pattern_case1.vertex_label_map.get(&0).unwrap();
        assert_eq!(vertices_with_label_0.len(), 2);
        let mut vertices_with_label_0_iter = vertices_with_label_0.iter();
        assert_eq!(*vertices_with_label_0_iter.next().unwrap(), 0);
        assert_eq!(*vertices_with_label_0_iter.next().unwrap(), 1);
        let edge_0 = pattern_case1.edges.get(0).unwrap();
        assert_eq!(edge_0.id, 0);
        assert_eq!(edge_0.label, 0);
        assert_eq!(edge_0.start_v_id, 0);
        assert_eq!(edge_0.end_v_id, 1);
        assert_eq!(edge_0.start_v_label, 0);
        assert_eq!(edge_0.end_v_label, 0);
        let edge_1 = pattern_case1.edges.get(1).unwrap();
        assert_eq!(edge_1.id, 1);
        assert_eq!(edge_1.label, 0);
        assert_eq!(edge_1.start_v_id, 1);
        assert_eq!(edge_1.end_v_id, 0);
        assert_eq!(edge_1.start_v_label, 0);
        assert_eq!(edge_1.end_v_label, 0);
        let vertex_0 = pattern_case1.vertices.get(0).unwrap();
        assert_eq!(vertex_0.id, 0);
        assert_eq!(vertex_0.label, 0);
        assert_eq!(vertex_0.adjacent_edges.len(), 2);
        let mut vertex_0_adjacent_edges_iter = vertex_0.adjacent_edges.iter();
        let (v0_e0, (v0_v0, v0_d0)) = vertex_0_adjacent_edges_iter.next().unwrap();
        assert_eq!(*v0_e0, 0);
        assert_eq!(*v0_v0, 1);
        assert_eq!(*v0_d0, PatternDirection::Out);
        let (v0_e1, (v0_v1, v0_d1)) = vertex_0_adjacent_edges_iter.next().unwrap();
        assert_eq!(*v0_e1, 1);
        assert_eq!(*v0_v1, 1);
        assert_eq!(*v0_d1, PatternDirection::In);
        assert_eq!(vertex_0.adjacent_vertices.len(), 1);
        let edges_connect_v0_v1 = vertex_0.adjacent_vertices.get(&1).unwrap();
        assert_eq!(edges_connect_v0_v1.len(), 2);
        let mut edges_connect_v0_v1_iter = edges_connect_v0_v1.iter();
        assert_eq!(*edges_connect_v0_v1_iter.next().unwrap(), (0, PatternDirection::Out));
        assert_eq!(*edges_connect_v0_v1_iter.next().unwrap(), (1, PatternDirection::In));
        let vertex_1 = pattern_case1.vertices.get(1).unwrap();
        assert_eq!(vertex_1.id, 1);
        assert_eq!(vertex_1.label, 0);
        assert_eq!(vertex_1.adjacent_edges.len(), 2);
        let mut vertex_1_adjacent_edges_iter = vertex_1.adjacent_edges.iter();
        let (v1_e0, (v1_v0, v1_d0)) = vertex_1_adjacent_edges_iter.next().unwrap();
        assert_eq!(*v1_e0, 0);
        assert_eq!(*v1_v0, 0);
        assert_eq!(*v1_d0, PatternDirection::In);
        let (v1_e1, (v1_v1, v1_d1)) = vertex_1_adjacent_edges_iter.next().unwrap();
        assert_eq!(*v1_e1, 1);
        assert_eq!(*v1_v1, 0);
        assert_eq!(*v1_d1, PatternDirection::Out);
        assert_eq!(vertex_1.adjacent_vertices.len(), 1);
        let edges_connect_v1_v0 = vertex_1.adjacent_vertices.get(&0).unwrap();
        assert_eq!(edges_connect_v1_v0.len(), 2);
        let mut edges_connect_v1_v0_iter = edges_connect_v1_v0.iter();
        assert_eq!(*edges_connect_v1_v0_iter.next().unwrap(), (0, PatternDirection::In));
        assert_eq!(*edges_connect_v1_v0_iter.next().unwrap(), (1, PatternDirection::Out));
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case1_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case1();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.vertices.len(), 3);
            assert_eq!(pattern.edges.len(), 3);
            // 3 Person vertices
            assert_eq!(pattern.vertex_label_map.get(&1).unwrap().len(), 3);
            // 3 knows edges
            assert_eq!(pattern.edge_label_map.get(&12).unwrap().len(), 3);
            // check structure
            // build identical pattern for comparison
            let pattern_edge1 = PatternEdge::new(0, 12, 0, 1, 1, 1);
            let pattern_edge2 = PatternEdge::new(1, 12, 0, 2, 1, 1);
            let pattern_edge3 = PatternEdge::new(2, 12, 1, 2, 1, 1);
            let pattern_for_comparison =
                Pattern::try_from(vec![pattern_edge1, pattern_edge2, pattern_edge3]).unwrap();
            // check whether the two pattern has the same code
            let encoder = Encoder::init(4, 4, 1, 2);
            let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
            let pattern_for_comparison_code: Vec<u8> = pattern_for_comparison.encode_to(&encoder);
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_A.into())
                    .unwrap()
                    .id,
                0
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_B.into())
                    .unwrap()
                    .id,
                1
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_C.into())
                    .unwrap()
                    .id,
                2
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case2_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case2();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.vertices.len(), 3);
            assert_eq!(pattern.edges.len(), 3);
            // 2 Person vertices
            assert_eq!(pattern.vertex_label_map.get(&1).unwrap().len(), 2);
            // 1 University vertex
            assert_eq!(pattern.vertex_label_map.get(&12).unwrap().len(), 1);
            // 1 knows edge
            assert_eq!(pattern.edge_label_map.get(&12).unwrap().len(), 1);
            // 2 studyat edges
            assert_eq!(pattern.edge_label_map.get(&15).unwrap().len(), 2);
            // check structure
            // build identical pattern for comparison
            for pattern_edge in pattern.edges_iter() {
                println!("{:?}", pattern_edge);
            }
            let pattern_edge1 = PatternEdge::new(0, 15, 1, 0, 1, 12);
            let pattern_edge2 = PatternEdge::new(1, 15, 2, 0, 1, 12);
            let pattern_edge3 = PatternEdge::new(2, 12, 1, 2, 1, 1);
            let pattern_for_comparison =
                Pattern::try_from(vec![pattern_edge1, pattern_edge2, pattern_edge3]).unwrap();
            // check whether the two pattern has the same code
            let encoder = Encoder::init(4, 4, 1, 2);
            let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
            let pattern_for_comparison_code: Vec<u8> = pattern_for_comparison.encode_to(&encoder);
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_A.into())
                    .unwrap()
                    .id,
                0
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_B.into())
                    .unwrap()
                    .id,
                1
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_C.into())
                    .unwrap()
                    .id,
                2
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case3_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case3();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.vertices.len(), 4);
            assert_eq!(pattern.edges.len(), 6);
            // 4 Person vertices
            assert_eq!(pattern.vertex_label_map.get(&1).unwrap().len(), 4);
            // 6 knows edges
            assert_eq!(pattern.edge_label_map.get(&12).unwrap().len(), 6);
            // check structure
            // build identical pattern for comparison
            let pattern_edge1 = PatternEdge::new(0, 12, 0, 1, 1, 1);
            let pattern_edge2 = PatternEdge::new(1, 12, 0, 2, 1, 1);
            let pattern_edge3 = PatternEdge::new(2, 12, 1, 2, 1, 1);
            let pattern_edge4 = PatternEdge::new(3, 12, 0, 3, 1, 1);
            let pattern_edge5 = PatternEdge::new(4, 12, 1, 3, 1, 1);
            let pattern_edge6 = PatternEdge::new(5, 12, 2, 3, 1, 1);
            let pattern_for_comparison = Pattern::try_from(vec![
                pattern_edge1,
                pattern_edge2,
                pattern_edge3,
                pattern_edge4,
                pattern_edge5,
                pattern_edge6,
            ])
            .unwrap();
            // check whether the two pattern has the same code
            let encoder = Encoder::init(4, 4, 1, 3);
            let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
            let pattern_for_comparison_code: Vec<u8> = pattern_for_comparison.encode_to(&encoder);
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_A.into())
                    .unwrap()
                    .id,
                0
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_B.into())
                    .unwrap()
                    .id,
                1
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_C.into())
                    .unwrap()
                    .id,
                2
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_D.into())
                    .unwrap()
                    .id,
                3
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case4_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case4();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.vertices.len(), 4);
            assert_eq!(pattern.edges.len(), 4);
            // 2 Person vertices
            assert_eq!(pattern.vertex_label_map.get(&1).unwrap().len(), 2);
            // 1 City vertex
            assert_eq!(pattern.vertex_label_map.get(&9).unwrap().len(), 1);
            // 1 Comment vertex
            assert_eq!(pattern.vertex_label_map.get(&2).unwrap().len(), 1);
            // 1 has creator edge
            assert_eq!(pattern.edge_label_map.get(&0).unwrap().len(), 1);
            // 1 likes edge
            assert_eq!(pattern.edge_label_map.get(&13).unwrap().len(), 1);
            // 2 islocated edges
            assert_eq!(pattern.edge_label_map.get(&11).unwrap().len(), 2);
            // check structure
            // build identical pattern for comparison
            let pattern_edge1 = PatternEdge::new(0, 11, 0, 1, 1, 9);
            let pattern_edge2 = PatternEdge::new(1, 11, 2, 1, 1, 9);
            let pattern_edge3 = PatternEdge::new(2, 13, 0, 3, 1, 2);
            let pattern_edge4 = PatternEdge::new(3, 0, 3, 2, 2, 1);
            let pattern_for_comparison =
                Pattern::try_from(vec![pattern_edge1, pattern_edge2, pattern_edge3, pattern_edge4])
                    .unwrap();
            // check whether the two pattern has the same code
            let encoder = Encoder::init(4, 4, 1, 2);
            let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
            let pattern_for_comparison_code: Vec<u8> = pattern_for_comparison.encode_to(&encoder);
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_A.into())
                    .unwrap()
                    .id,
                0
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_B.into())
                    .unwrap()
                    .id,
                2
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_C.into())
                    .unwrap()
                    .id,
                1
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_D.into())
                    .unwrap()
                    .id,
                3
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case5_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case5();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.vertices.len(), 6);
            assert_eq!(pattern.edges.len(), 6);
            // 6 Person vertices
            assert_eq!(pattern.vertex_label_map.get(&1).unwrap().len(), 6);
            // 6 knows edges
            assert_eq!(pattern.edge_label_map.get(&12).unwrap().len(), 6);
            // check structure
            // build identical pattern for comparison
            let pattern_edge1 = PatternEdge::new(0, 12, 0, 1, 1, 1);
            let pattern_edge2 = PatternEdge::new(1, 12, 2, 1, 1, 1);
            let pattern_edge3 = PatternEdge::new(2, 12, 2, 3, 1, 1);
            let pattern_edge4 = PatternEdge::new(3, 12, 4, 3, 1, 1);
            let pattern_edge5 = PatternEdge::new(4, 12, 4, 5, 1, 1);
            let pattern_edge6 = PatternEdge::new(5, 12, 0, 5, 1, 1);
            let pattern_for_comparison = Pattern::try_from(vec![
                pattern_edge1,
                pattern_edge2,
                pattern_edge3,
                pattern_edge4,
                pattern_edge5,
                pattern_edge6,
            ])
            .unwrap();
            // check whether the two pattern has the same code
            let encoder = Encoder::init(4, 4, 1, 3);
            let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
            let pattern_for_comparison_code: Vec<u8> = pattern_for_comparison.encode_to(&encoder);
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_A.into())
                    .unwrap()
                    .id,
                0
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_B.into())
                    .unwrap()
                    .id,
                1
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_C.into())
                    .unwrap()
                    .id,
                3
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case6_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case6();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.vertices.len(), 6);
            assert_eq!(pattern.edges.len(), 6);
            // 4 Persons vertices
            assert_eq!(pattern.vertex_label_map.get(&1).unwrap().len(), 4);
            // 1 City vertex
            assert_eq!(pattern.vertex_label_map.get(&9).unwrap().len(), 1);
            // 1 Comment vertex
            assert_eq!(pattern.vertex_label_map.get(&2).unwrap().len(), 1);
            // 1 has creator edge
            assert_eq!(pattern.edge_label_map.get(&0).unwrap().len(), 1);
            // 1 likes edge
            assert_eq!(pattern.edge_label_map.get(&13).unwrap().len(), 1);
            // 2 islocated edges
            assert_eq!(pattern.edge_label_map.get(&11).unwrap().len(), 2);
            // 2 knows edges
            assert_eq!(pattern.edge_label_map.get(&12).unwrap().len(), 2);
            // check structure
            // build identical pattern for comparison
            let pattern_edge1 = PatternEdge::new(0, 11, 0, 1, 1, 9);
            let pattern_edge2 = PatternEdge::new(1, 11, 2, 1, 1, 9);
            let pattern_edge3 = PatternEdge::new(2, 12, 2, 3, 1, 1);
            let pattern_edge4 = PatternEdge::new(3, 12, 3, 4, 1, 1);
            let pattern_edge5 = PatternEdge::new(4, 0, 5, 4, 2, 1);
            let pattern_edge6 = PatternEdge::new(5, 13, 0, 5, 1, 2);
            let pattern_for_comparison = Pattern::try_from(vec![
                pattern_edge1,
                pattern_edge2,
                pattern_edge3,
                pattern_edge4,
                pattern_edge5,
                pattern_edge6,
            ])
            .unwrap();
            // check whether the two pattern has the same code
            let encoder = Encoder::init(4, 4, 1, 3);
            let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
            let pattern_for_comparison_code: Vec<u8> = pattern_for_comparison.encode_to(&encoder);
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_A.into())
                    .unwrap()
                    .id,
                0
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_B.into())
                    .unwrap()
                    .id,
                1
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(&TAG_C.into())
                    .unwrap()
                    .id,
                4
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    /// Test whether pattern_case1 + extend_step_case1 = pattern_case2
    #[test]
    fn test_pattern_case1_extend() {
        let pattern_case1 = build_pattern_case1();
        let extend_step = build_extend_step_case1();
        let pattern_after_extend = pattern_case1.extend(extend_step).unwrap();
        assert_eq!(pattern_after_extend.edges.len(), 4);
        assert_eq!(pattern_after_extend.vertices.len(), 3);
        // Pattern after extend should be exactly the same as pattern case2
        let pattern_case2 = build_pattern_case2();
        assert_eq!(pattern_after_extend.edges.len(), pattern_case2.edges.len());
        for i in 0..pattern_after_extend.edges.len() as PatternId {
            let edge1 = pattern_after_extend.edges.get(i).unwrap();
            let edge2 = pattern_case2.edges.get(i).unwrap();
            assert_eq!(edge1.id, edge2.id);
            assert_eq!(edge1.label, edge2.label);
            assert_eq!(edge1.start_v_id, edge2.start_v_id);
            assert_eq!(edge1.start_v_label, edge2.start_v_label);
            assert_eq!(edge1.end_v_id, edge2.end_v_id);
            assert_eq!(edge1.end_v_label, edge2.end_v_label);
        }
        assert_eq!(pattern_after_extend.edges.len(), pattern_case2.edges.len());
        for i in 0..pattern_after_extend.vertices.len() as PatternId {
            let vertex1 = pattern_after_extend.vertices.get(i).unwrap();
            let vertex2 = pattern_after_extend.vertices.get(i).unwrap();
            assert_eq!(vertex1.id, vertex2.id);
            assert_eq!(vertex1.label, vertex2.label);
            assert_eq!(vertex1.rank, vertex2.rank);
            assert_eq!(vertex1.in_degree, vertex2.in_degree);
            assert_eq!(vertex1.out_degree, vertex2.out_degree);
            assert_eq!(vertex1.adjacent_edges.len(), vertex2.adjacent_edges.len());
            assert_eq!(vertex1.adjacent_vertices.len(), vertex2.adjacent_vertices.len());
            for (adjacent_edge1_id, (adjacent_vertex1_id, dir1)) in &vertex1.adjacent_edges {
                let (adjacent_vertex2_id, dir2) = vertex2
                    .adjacent_edges
                    .get(adjacent_edge1_id)
                    .unwrap();
                assert_eq!(*adjacent_vertex1_id, *adjacent_vertex2_id);
                assert_eq!(*dir1, *dir2);
            }
            for (adjacent_vertex1_id, edges_with_dirs1) in &vertex1.adjacent_vertices {
                let edges_with_dirs2 = vertex2
                    .adjacent_vertices
                    .get(adjacent_vertex1_id)
                    .unwrap();
                let (adjacent_edge1_id, dir1) = edges_with_dirs1[0];
                let (adjacent_edge2_id, dir2) = edges_with_dirs2[0];
                assert_eq!(adjacent_edge1_id, adjacent_edge2_id);
                assert_eq!(dir1, dir2);
            }
        }
        assert_eq!(pattern_after_extend.edge_label_map.len(), pattern_case2.edge_label_map.len());
        for i in 0..=1 {
            let edges_with_labeli_1 = pattern_after_extend
                .edge_label_map
                .get(&i)
                .unwrap();
            let edges_with_labeli_2 = pattern_case2.edge_label_map.get(&i).unwrap();
            assert_eq!(edges_with_labeli_1.len(), edges_with_labeli_2.len());
            let mut edges_with_labeli_1_iter = edges_with_labeli_1.iter();
            let mut edges_with_labeli_2_iter = edges_with_labeli_2.iter();
            let mut edges_with_labeli_1_element = edges_with_labeli_1_iter.next();
            let mut edges_with_labeli_2_element = edges_with_labeli_2_iter.next();
            while edges_with_labeli_1_element.is_some() {
                assert_eq!(*edges_with_labeli_1_element.unwrap(), *edges_with_labeli_2_element.unwrap());
                edges_with_labeli_1_element = edges_with_labeli_1_iter.next();
                edges_with_labeli_2_element = edges_with_labeli_2_iter.next();
            }
        }
        assert_eq!(pattern_after_extend.vertex_label_map.len(), pattern_case2.vertex_label_map.len());
        for i in 0..=1 {
            let vertices_with_labeli_1 = pattern_after_extend
                .vertex_label_map
                .get(&i)
                .unwrap();
            let vertices_with_labeli_2 = pattern_case2.vertex_label_map.get(&i).unwrap();
            assert_eq!(vertices_with_labeli_1.len(), vertices_with_labeli_2.len());
            let mut vertices_with_labeli_1_iter = vertices_with_labeli_1.iter();
            let mut vertices_with_labeli_2_iter = vertices_with_labeli_2.iter();
            let mut vertices_with_labeli_1_element = vertices_with_labeli_1_iter.next();
            let mut vertices_with_labeli_2_element = vertices_with_labeli_2_iter.next();
            while vertices_with_labeli_1_element.is_some() {
                assert_eq!(
                    *vertices_with_labeli_1_element.unwrap(),
                    *vertices_with_labeli_2_element.unwrap()
                );
                vertices_with_labeli_1_element = vertices_with_labeli_1_iter.next();
                vertices_with_labeli_2_element = vertices_with_labeli_2_iter.next();
            }
        }
    }

    #[test]
    fn test_get_extend_steps_of_modern_case1() {
        let modern_pattern_meta = get_modern_pattern_meta();
        let person_only_pattern = build_modern_pattern_case1();
        let all_extend_steps = person_only_pattern.get_extend_steps(&modern_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 3);
        let mut out_0_0_0 = 0;
        let mut incoming_0_0_0 = 0;
        let mut out_0_0_1 = 0;
        for extend_step in all_extend_steps {
            let extend_edges = extend_step
                .get_extend_edges_by_start_v(0, 0)
                .unwrap();
            assert_eq!(extend_edges.len(), 1);
            let extend_edge = extend_edges[0];
            assert_eq!(extend_edge.get_start_vertex_label(), 0);
            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
            if extend_step.get_target_v_label() == 0 {
                if extend_edge.get_direction() == PatternDirection::Out {
                    out_0_0_0 += 1;
                }
                if extend_edge.get_direction() == PatternDirection::In {
                    incoming_0_0_0 += 1;
                }
            }
            if extend_step.get_target_v_label() == 1 && extend_edge.get_direction() == PatternDirection::Out
            {
                out_0_0_1 += 1;
            }
        }
        assert_eq!(out_0_0_0, 1);
        assert_eq!(incoming_0_0_0, 1);
        assert_eq!(out_0_0_1, 1);
    }

    #[test]
    fn test_get_extend_steps_of_modern_case2() {
        let modern_pattern_meta = get_modern_pattern_meta();
        let person_only_pattern = build_modern_pattern_case2();
        let all_extend_steps = person_only_pattern.get_extend_steps(&modern_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 1);
        assert_eq!(all_extend_steps[0].get_target_v_label(), 0);
        assert_eq!(all_extend_steps[0].get_diff_start_v_num(), 1);
        let extend_edge = all_extend_steps[0]
            .get_extend_edges_by_start_v(1, 0)
            .unwrap()[0];
        assert_eq!(extend_edge.get_start_vertex_label(), 1);
        assert_eq!(extend_edge.get_start_vertex_rank(), 0);
        assert_eq!(extend_edge.get_edge_label(), 1);
        assert_eq!(extend_edge.get_direction(), PatternDirection::In);
    }

    #[test]
    fn test_get_extend_steps_of_modern_case3() {
        let modern_pattern_meta = get_modern_pattern_meta();
        let person_knows_person = build_modern_pattern_case3();
        let all_extend_steps = person_knows_person.get_extend_steps(&modern_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 11);
        let mut extend_steps_with_label_0_count = 0;
        let mut extend_steps_with_label_1_count = 0;
        let mut out_0_0_0_count = 0;
        let mut incoming_0_0_0_count = 0;
        let mut out_0_1_0_count = 0;
        let mut incoming_0_1_0_count = 0;
        let mut out_0_0_1_count = 0;
        let mut out_0_1_1_count = 0;
        let mut out_0_0_0_out_0_1_0_count = 0;
        let mut out_0_0_0_incoming_0_1_0_count = 0;
        let mut incoming_0_0_0_out_0_1_0_count = 0;
        let mut incoming_0_0_0_incoming_0_1_0_count = 0;
        let mut out_0_0_1_out_0_1_1_count = 0;
        for extend_step in all_extend_steps {
            if extend_step.get_target_v_label() == 0 {
                extend_steps_with_label_0_count += 1;
                if extend_step.get_diff_start_v_num() == 1 {
                    if extend_step.has_extend_from_start_v(0, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1
                            }
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_0_1_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_1_1_count += 1;
                            }
                        }
                    } else if extend_step.has_extend_from_start_v(0, 1) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 1)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 1);
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_1_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_1_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_0_1_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_1_1_count += 1;
                            }
                        }
                    }
                } else if extend_step.get_diff_start_v_num() == 2 {
                    let mut found_out_0_0_0 = false;
                    let mut found_incoming_0_0_0 = false;
                    let mut found_out_0_1_0 = false;
                    let mut found_incoming_0_1_0 = false;
                    if extend_step.has_extend_from_start_v(0, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                found_out_0_0_0 = true;
                            } else if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                found_incoming_0_0_0 = true;
                            }
                        }
                    }
                    if extend_step.has_extend_from_start_v(0, 1) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 1)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 1);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                found_out_0_1_0 = true;
                            } else if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                found_incoming_0_1_0 = true;
                            }
                        }
                    }
                    if found_out_0_0_0 && found_out_0_1_0 {
                        out_0_0_0_out_0_1_0_count += 1;
                    } else if found_out_0_0_0 && found_incoming_0_1_0 {
                        out_0_0_0_incoming_0_1_0_count += 1;
                    } else if found_incoming_0_0_0 && found_out_0_1_0 {
                        incoming_0_0_0_out_0_1_0_count += 1;
                    } else if found_incoming_0_0_0 && found_incoming_0_1_0 {
                        incoming_0_0_0_incoming_0_1_0_count += 1;
                    }
                }
            } else if extend_step.get_target_v_label() == 1 {
                extend_steps_with_label_1_count += 1;
                if extend_step.get_diff_start_v_num() == 1 {
                    if extend_step.has_extend_from_start_v(0, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_0_1_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1
                            }
                        }
                    } else if extend_step.has_extend_from_start_v(0, 1) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 1)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 1);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_1_1_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1
                            }
                        }
                    }
                } else if extend_step.get_diff_start_v_num() == 2 {
                    let mut found_out_0_0_1 = false;
                    let mut found_out_0_1_1 = false;
                    if extend_step.has_extend_from_start_v(0, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                found_out_0_0_1 = true;
                            }
                        }
                    }
                    if extend_step.has_extend_from_start_v(0, 1) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 1)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 1);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                found_out_0_1_1 = true;
                            }
                        }
                    }
                    if found_out_0_0_1 && found_out_0_1_1 {
                        out_0_0_1_out_0_1_1_count += 1;
                    }
                }
            }
        }
        assert_eq!(extend_steps_with_label_0_count, 8);
        assert_eq!(extend_steps_with_label_1_count, 3);
        assert_eq!(out_0_0_0_count, 1);
        assert_eq!(incoming_0_0_0_count, 1);
        assert_eq!(out_0_1_0_count, 1);
        assert_eq!(incoming_0_1_0_count, 1);
        assert_eq!(out_0_0_1_count, 1);
        assert_eq!(out_0_1_1_count, 1);
        assert_eq!(out_0_0_0_out_0_1_0_count, 1);
        assert_eq!(out_0_0_0_incoming_0_1_0_count, 1);
        assert_eq!(incoming_0_0_0_out_0_1_0_count, 1);
        assert_eq!(incoming_0_0_0_incoming_0_1_0_count, 1);
        assert_eq!(out_0_0_1_out_0_1_1_count, 1);
    }

    #[test]
    fn test_get_extend_steps_of_modern_case4() {
        let modern_pattern_meta = get_modern_pattern_meta();
        let person_created_software = build_modern_pattern_case4();
        let all_extend_steps = person_created_software.get_extend_steps(&modern_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 6);
        let mut extend_steps_with_label_0_count = 0;
        let mut extend_steps_with_label_1_count = 0;
        let mut out_0_0_0_count = 0;
        let mut incoming_0_0_0_count = 0;
        let mut incoming_1_0_1_count = 0;
        let mut out_0_0_0_incoming_1_0_1_count = 0;
        let mut incoming_0_0_0_incoming_1_0_1_count = 0;
        for extend_step in all_extend_steps {
            if extend_step.get_target_v_label() == 0 {
                extend_steps_with_label_0_count += 1;
                if extend_step.get_diff_start_v_num() == 1 {
                    if extend_step.has_extend_from_start_v(0, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 1
                            {
                                incoming_1_0_1_count += 1;
                            }
                        }
                    } else if extend_step.has_extend_from_start_v(1, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(1, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 1);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 1
                            {
                                incoming_1_0_1_count += 1;
                            }
                        }
                    }
                } else if extend_step.get_diff_start_v_num() == 2 {
                    let mut found_out_0_0_0 = false;
                    let mut found_incoming_1_0_1 = false;
                    let mut found_incoming_0_0_0 = false;
                    if extend_step.has_extend_from_start_v(0, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                found_out_0_0_0 = true;
                            } else if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                found_incoming_0_0_0 = true;
                            }
                        }
                    }
                    if extend_step.has_extend_from_start_v(1, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(1, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 1);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 1
                            {
                                found_incoming_1_0_1 = true;
                            }
                        }
                    }
                    if found_out_0_0_0 && found_incoming_1_0_1 {
                        out_0_0_0_incoming_1_0_1_count += 1;
                    } else if found_incoming_0_0_0 && found_incoming_1_0_1 {
                        incoming_0_0_0_incoming_1_0_1_count += 1;
                    }
                }
            } else if extend_step.get_target_v_label() == 1 {
                extend_steps_with_label_1_count += 1;
            }
        }
        assert_eq!(extend_steps_with_label_0_count, 5);
        assert_eq!(extend_steps_with_label_1_count, 1);
        assert_eq!(out_0_0_0_count, 1);
        assert_eq!(incoming_0_0_0_count, 1);
        assert_eq!(incoming_1_0_1_count, 1);
        assert_eq!(out_0_0_0_incoming_1_0_1_count, 1);
        assert_eq!(incoming_0_0_0_incoming_1_0_1_count, 1);
    }

    #[test]
    fn test_get_extend_steps_of_ldbc_case1() {
        let ldbc_pattern_meta = get_ldbc_pattern_meta();
        let person_knows_person = build_ldbc_pattern_case1();
        let all_extend_steps = person_knows_person.get_extend_steps(&ldbc_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 44);
    }

    #[test]
    fn set_accurate_rank_case1() {
        let pattern = build_pattern_case1();
        let vertices: VecMap<&PatternVertex> = pattern.vertices_iter().enumerate().collect();
        assert_eq!(vertices.get(0).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(1).unwrap().get_rank(), 0);
    }

    #[test]
    fn set_accurate_rank_case2() {
        let pattern = build_pattern_case2();
        let vertices = &pattern.vertices;
        assert_eq!(vertices.get(0).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(1).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(2).unwrap().get_rank(), 0);
    }

    #[test]
    fn set_accurate_rank_case3() {
        let pattern = build_pattern_case3();
        let vertices = &pattern.vertices;
        assert_eq!(vertices.get(0).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(1).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(2).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(3).unwrap().get_rank(), 0);
    }

    #[test]
    fn set_accurate_rank_case4() {
        let pattern = build_pattern_case4();
        let vertices = &pattern.vertices;
        assert_eq!(vertices.get(0).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(1).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(2).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(3).unwrap().get_rank(), 0);
    }

    #[test]
    fn set_accurate_rank_case5() {
        let pattern = build_pattern_case5();
        let id_vec_a: Vec<PatternId> = vec![100, 200, 300, 400];
        let id_vec_b: Vec<PatternId> = vec![10, 20, 30];
        let id_vec_c: Vec<PatternId> = vec![1, 2, 3];
        let id_vec_d: Vec<PatternId> = vec![1000];
        let vertices = &pattern.vertices;
        // A
        assert_eq!(vertices.get(id_vec_a[0]).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(id_vec_a[1]).unwrap().get_rank(), 3);
        assert_eq!(vertices.get(id_vec_a[2]).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(id_vec_a[3]).unwrap().get_rank(), 2);
        // B
        assert_eq!(vertices.get(id_vec_b[0]).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(id_vec_b[1]).unwrap().get_rank(), 2);
        assert_eq!(vertices.get(id_vec_b[2]).unwrap().get_rank(), 1);
        // C
        assert_eq!(vertices.get(id_vec_c[0]).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(id_vec_c[1]).unwrap().get_rank(), 2);
        assert_eq!(vertices.get(id_vec_c[2]).unwrap().get_rank(), 0);
        // D
        assert_eq!(vertices.get(id_vec_d[0]).unwrap().get_rank(), 0);
    }

    #[test]
    fn rank_ranking_case1() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case1();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case2() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case2();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case3() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case3();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case4() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case4();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
    }

    #[test]
    fn rank_ranking_case5() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case5();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case6() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case6();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case7() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case7();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case8() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case8();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case9() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case9();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case10() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case10();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case11() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case11();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B2").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case12() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case12();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B2").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B3").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case13() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case13();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B2").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case14() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case14();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap()
                .get_rank(),
            3
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B2").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B3").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("C0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case15() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case15();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            3
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B2").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("C0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("C1").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
    }

    #[test]
    fn rank_ranking_case16() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case16();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            3
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B2").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("C0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("C1").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("D0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case17() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case17();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            3
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            5
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap()
                .get_rank(),
            4
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A4").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A5").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case17_even_num_chain() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case17_even_num_chain();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            3
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            5
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap()
                .get_rank(),
            6
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A4").unwrap())
                .unwrap()
                .get_rank(),
            4
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A5").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A6").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn rank_ranking_case17_long_chain() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case17_long_chain();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            3
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            5
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap()
                .get_rank(),
            7
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A4").unwrap())
                .unwrap()
                .get_rank(),
            9
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A5").unwrap())
                .unwrap()
                .get_rank(),
            10
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A6").unwrap())
                .unwrap()
                .get_rank(),
            8
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A7").unwrap())
                .unwrap()
                .get_rank(),
            6
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A8").unwrap())
                .unwrap()
                .get_rank(),
            4
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A9").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A10").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn index_ranking_case17_special_id_situation_1() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case17_special_id_situation_1();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            3
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            5
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap()
                .get_rank(),
            4
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A4").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A5").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn index_ranking_case17_special_id_situation_2() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case17_special_id_situation_2();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            3
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            5
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap()
                .get_rank(),
            4
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A4").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A5").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn index_ranking_case18() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case18();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A4").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A5").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn index_ranking_case19() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case19();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            8
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            9
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap()
                .get_rank(),
            2
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A4").unwrap())
                .unwrap()
                .get_rank(),
            1
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A5").unwrap())
                .unwrap()
                .get_rank(),
            3
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A6").unwrap())
                .unwrap()
                .get_rank(),
            4
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A7").unwrap())
                .unwrap()
                .get_rank(),
            6
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A8").unwrap())
                .unwrap()
                .get_rank(),
            5
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A9").unwrap())
                .unwrap()
                .get_rank(),
            7
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("C0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("D0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("E0").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }

    #[test]
    fn index_ranking_case20() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case20();
        let vertices = &pattern.vertices;
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap()
                .get_rank(),
            3
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap()
                .get_rank(),
            3
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
        assert_eq!(
            vertices
                .get(*vertex_id_map.get("A4").unwrap())
                .unwrap()
                .get_rank(),
            0
        );
    }
}
