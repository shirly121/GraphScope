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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::iter::FromIterator;

use ir_common::generated::algebra as pb;
use vec_map::VecMap;

use crate::catalogue::extend_step::{ExtendEdge, ExtendStep};
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::{DynIter, PatternDirection, PatternId, PatternLabelId, PatternRankId};
use crate::error::IrError;

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
    /// Usage: 1. this mao stores all adjacent vertices of this vertex
    ///        2. given an adjacent vertex id, find all possible edges connecting two vertices with direction
    adjacent_vertices: BTreeMap<PatternId, Vec<(PatternId, PatternDirection)>>,
    /// How many out edges connected to this vertex
    out_degree: usize,
    /// How many in edges connected to this vertex
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

    pub fn get_connected_edges(&self) -> &BTreeMap<PatternId, (PatternId, PatternDirection)> {
        &self.adjacent_edges
    }

    pub fn get_connected_vertices(&self) -> &BTreeMap<PatternId, Vec<(PatternId, PatternDirection)>> {
        &self.adjacent_vertices
    }

    pub fn get_out_degree(&self) -> usize {
        self.out_degree
    }

    pub fn get_in_degree(&self) -> usize {
        self.in_degree
    }

    /// Get how many connections(both out and in) the current pattern vertex has
    pub fn get_connect_num(&self) -> usize {
        self.adjacent_edges.len()
    }

    /// Given a edge id, get the vertex connected to the current vertex through the edge with the connect direction
    pub fn get_connect_vertex_by_edge_id(
        &self, edge_id: PatternId,
    ) -> Option<(PatternId, PatternDirection)> {
        self.adjacent_edges.get(&edge_id).cloned()
    }

    /// Given a vertex id, get all the edges connecting the given vertex and current vertex with the connect direction
    pub fn get_connect_edges_by_vertex_id<'a>(
        &'a self, vertex_id: PatternId,
    ) -> DynIter<(PatternId, PatternDirection)> {
        match self.adjacent_vertices.get(&vertex_id) {
            Some(connect_edges) => Box::new(
                connect_edges
                    .iter()
                    .map(|connect_edge| *connect_edge),
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
            Ok(new_pattern)
        } else {
            Err(IrError::InvalidPattern("Empty pattern".to_string()))
        }
    }
}

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
        let mut tag_id_map: HashMap<String, PatternId> = HashMap::new();
        for sentence in &pattern_message.sentences {
            if sentence.binders.is_empty() {
                return Err(IrError::Unsupported("FuzzyPattern is not supported".to_string()));
            }
            let start_tag = sentence
                .start
                .as_ref()
                .ok_or(IrError::Unsupported("FuzzyPattern is not supported".to_string()))?
                .item
                .as_ref()
                .ok_or(IrError::Unsupported("FuzzyPattern is not supported".to_string()))?;
            if let TagItem::Name(start_tag_name) = start_tag {
                if !tag_id_map.contains_key(start_tag_name) {
                    tag_id_map.insert(start_tag_name.clone(), assign_vertex_id);
                }
            } else {
                return Err(IrError::Unsupported("FuzzyPattern is not supported".to_string()));
            }
            for (i, binder) in sentence.binders.iter().enumerate() {
                if let Some(BinderItem::Edge(edge_expand)) = binder.item.as_ref() {
                    if edge_expand.is_edge {
                        return Err(IrError::Unsupported("FuzzyPattern is not supported".to_string()));
                    }
                    if let Some(params) = edge_expand.params.as_ref() {
                        if params.tables.len() != 1 {
                            return Err(IrError::Unsupported("FuzzyPattern is not supported".to_string()));
                        }
                        if !params.columns.is_empty() {
                            return Err(IrError::Unsupported("FuzzyPattern is not supported".to_string()));
                        }
                        if let Some(TagItem::Name(edge_label_name)) = params.tables[0].item.as_ref() {
                            if let Some(edge_label_id) = pattern_meta.get_edge_label_id(edge_label_name) {
                                let src_dst_vertex_pairs =
                                    pattern_meta.get_associated_vlabels(edge_label_id);
                                if i == 0 {
                                } else if i == sentence.binders.len() - 1 {
                                }
                                let src_vertex_id = assign_vertex_id;
                                let dst_vertex_id = assign_vertex_id + 1;
                                let edge_id = assign_edge_id;
                                assign_vertex_id += 1;
                                assign_edge_id += 1;
                            } else {
                                return Err(IrError::Unsupported(
                                    "FuzzyPattern is not supported".to_string(),
                                ));
                            }
                        } else {
                            return Err(IrError::Unsupported("FuzzyPattern is not supported".to_string()));
                        }
                    } else {
                        return Err(IrError::Unsupported("FuzzyPattern is not supported".to_string()));
                    }
                } else {
                    return Err(IrError::Unsupported("FuzzyPattern is not supported".to_string()));
                }
            }
        }
        Pattern::try_from(pattern_edges)
    }
}

/// Methods to access the fields of a Pattern or get some info from Pattern
impl Pattern {
    /// Get Edges References
    pub fn get_edges(&self) -> &VecMap<PatternEdge> {
        &self.edges
    }

    /// Get Vertices References
    pub fn get_vertices(&self) -> &VecMap<PatternVertex> {
        &self.vertices
    }

    /// Get Edge Label Map Reference
    pub fn get_edge_label_map(&self) -> &BTreeMap<PatternLabelId, BTreeSet<PatternId>> {
        &self.edge_label_map
    }

    /// Get Vertex Label Map Reference
    pub fn get_vertex_label_map(&self) -> &BTreeMap<PatternLabelId, BTreeSet<PatternId>> {
        &self.vertex_label_map
    }

    /// Get PatternEdge Reference from Edge ID
    pub fn get_edge_from_id(&self, edge_id: PatternId) -> Option<&PatternEdge> {
        self.edges.get(edge_id)
    }

    /// Get PatternVertex Reference from Vertex ID
    pub fn get_vertex_from_id(&self, vertex_id: PatternId) -> Option<&PatternVertex> {
        self.vertices.get(vertex_id)
    }

    pub fn get_edge_mut_from_id(&mut self, edge_id: PatternId) -> Option<&mut PatternEdge> {
        self.edges.get_mut(edge_id)
    }

    pub fn get_vertex_mut_from_id(&mut self, vertex_id: PatternId) -> Option<&mut PatternVertex> {
        self.vertices.get_mut(vertex_id)
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

/// Methods of Index Ranking
impl Pattern {
    /// Index Ranking Is Done In Four Steps:
    ///
    /// Step-1: Get the Neighbor Edges for All Vertices, Sorted Simply By Labels
    ///
    /// Step-2: Set Initial Index Based on Local Information of Vertices (labels, in/out degree, neighboring edges info)
    ///
    /// Step-3: Update the Neighbor Edges for All Vertices, Sorted now with Initial Indices
    ///
    /// Step-4: Set Accurate Index Based on Iterative Traversal
    pub fn rank_ranking(&mut self) {
        // Step-1: Get the Neighbor Edges for All Vertices, Sorted Simply By Labels
        let mut vertex_neighbor_edges_map: HashMap<PatternId, Vec<(PatternId, PatternId)>> =
            self.get_vertex_neighbor_edges();
        // Step-2: Set Initial Index Based on Local Information of Vertices (labels, in/out degree, neighboring edges info)
        self.set_initial_rank(&vertex_neighbor_edges_map);
        // Step-3: Update the Neighbor Edges for All Vertices, Sorted now with Initial Indices
        self.update_vertex_neighbor_edges_map(&mut vertex_neighbor_edges_map);
        // Step-4: Set Accurate Index Based on Iterative Traversal
        self.set_accurate_rank(&vertex_neighbor_edges_map);
    }

    /// Get a vector of neighboring edges of each vertex
    ///
    /// Used for Comparing Vertices when Setting Initial Indices
    ///
    /// The vector contains of two parts: outgoing edges and incoming edges
    ///
    /// i.e. [ sorted outgoing edges | sorted incoming edges ]
    ///
    /// The Edge info is a 4-element tuple: (edge_id, edge_label, end_v_id, end_v_label)
    fn get_vertex_neighbor_edges(&self) -> HashMap<PatternId, Vec<(PatternId, PatternId)>> {
        let mut vertex_neighbor_edges_map: HashMap<PatternId, Vec<(PatternId, PatternId)>> = HashMap::new();
        for (v_id, vertex) in self.get_vertices() {
            let mut outgoing_edges: Vec<(PatternId, PatternId)> =
                Vec::with_capacity(vertex.get_out_degree());
            let mut incoming_edges: Vec<(PatternId, PatternId)> =
                Vec::with_capacity(vertex.get_in_degree());
            for (e_id, (end_v_id, edge_dir)) in vertex.get_connected_edges().iter() {
                match edge_dir {
                    PatternDirection::Out => outgoing_edges.push((*e_id, *end_v_id)),
                    PatternDirection::In => incoming_edges.push((*e_id, *end_v_id)),
                }
            }
            // Sort the edges
            outgoing_edges.sort_by(|e1, e2| self.cmp_edges(e1.0, e2.0));
            incoming_edges.sort_by(|e1, e2| self.cmp_edges(e1.0, e2.0));
            // Concat two edge info vector
            outgoing_edges.append(&mut incoming_edges);
            // Store in the Hashmap
            vertex_neighbor_edges_map.insert(v_id, outgoing_edges);
        }

        vertex_neighbor_edges_map
    }

    /// Set Initial Vertex Index Based on Comparison of Labels and In/Out Degrees
    fn set_initial_rank(
        &mut self, vertex_neighbor_edges_map: &HashMap<PatternId, Vec<(PatternId, PatternId)>>,
    ) {
        for (_, vertex_set) in self.vertex_label_map.iter() {
            let mut vertex_vec = Vec::with_capacity(vertex_set.len());
            for v_id in vertex_set.iter() {
                vertex_vec.push(*v_id);
            }
            // Sort vertices from small to large
            vertex_vec.sort_by(|v1_id, v2_id| {
                self.cmp_vertices_for_initial_rank(*v1_id, *v2_id, vertex_neighbor_edges_map)
            });
            // Vertex Index is the value to be set to vertices.
            // Isomorphic Vertices may share the same vertex rank
            let mut vertex_rank = 0;
            // Vertex Index Implicit is to make sure that vertices sharing the same vertex rank still occupy a place
            // eg: 0, 0, 2, 2, 2, 5
            let mut vertex_rank_implicit = 0;
            let mut max_v_id = vertex_vec[0];
            for v_id in vertex_vec.iter() {
                let order = self.cmp_vertices_for_initial_rank(*v_id, max_v_id, vertex_neighbor_edges_map);
                let vertex: &mut PatternVertex = self.vertices.get_mut(*v_id).unwrap();
                match order {
                    Ordering::Greater => {
                        vertex_rank = vertex_rank_implicit;
                        vertex.set_rank(vertex_rank);
                        max_v_id = *v_id;
                    }
                    Ordering::Equal => {
                        vertex.set_rank(vertex_rank);
                    }
                    Ordering::Less => {
                        panic!("Error in setting initial vertex rank, vertex_vec is not sorted")
                    }
                }
                vertex_rank_implicit += 1;
            }
        }
    }

    /// Get the Order of two PatternVertices of a Pattern
    fn cmp_vertices_for_initial_rank(
        &self, v1_id: PatternId, v2_id: PatternId,
        vertex_neighbor_edges_map: &HashMap<PatternId, Vec<(PatternId, PatternId)>>,
    ) -> Ordering {
        if v1_id == v2_id {
            return Ordering::Equal;
        }
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

        let v1_connected_edges = vertex_neighbor_edges_map.get(&v1_id).unwrap();
        let v2_connected_edges = vertex_neighbor_edges_map.get(&v2_id).unwrap();
        for i in 0..v1_connected_edges.len() {
            // Compare Edge Label
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
            match v1_connected_edge_label.cmp(&v2_connected_edge_label) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                _ => (),
            }
            match v1_connected_edge_end_v_label.cmp(&v2_connected_edge_end_v_label) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                _ => (),
            }
        }
        // Return Equal if Still Cannot Distinguish
        Ordering::Equal
    }

    /// Update the Vertex Neighbor Edges Map
    ///
    /// The reason is that after setting initial indices for each vertex, we have more information to sort the edges and vertices
    fn update_vertex_neighbor_edges_map(
        &self, vertex_neighbor_edges_map: &mut HashMap<PatternId, Vec<(PatternId, PatternId)>>,
    ) {
        for (v_id, _) in self.get_vertices().iter() {
            vertex_neighbor_edges_map
                .get_mut(&v_id)
                .unwrap()
                .sort_by(|e1, e2| self.cmp_edges(e1.0, e2.0));
        }
    }

    /// Set Accurate Indices According to the Initial Indices Set in Step-1
    fn set_accurate_rank(
        &mut self, vertex_neighbor_edges_map: &HashMap<PatternId, Vec<(PatternId, PatternId)>>,
    ) {
        // Initializde a HashSet Indicating Whether Vertices Has Been Visited
        let mut visited_vertex_set: HashSet<PatternId> = HashSet::new();
        // Iteratively find a group of vertices sharing the same index
        let same_rank_vertex_groups: Vec<Vec<PatternId>> = self.get_same_rank_vertex_groups();
        // Iteratively Compare the Indices of Vertices
        for vertex_group in same_rank_vertex_groups {
            if vertex_group.len() == 1 {
                continue;
            }
            if vertex_group.len() > 1 {
                let initial_rank: PatternRankId = self
                    .get_vertex_from_id(vertex_group[0])
                    .unwrap()
                    .get_rank();
                let mut accurate_rank_vec: Vec<PatternRankId> = Vec::with_capacity(vertex_group.len());
                for _ in 0..vertex_group.len() {
                    accurate_rank_vec.push(initial_rank);
                }
                for i in 0..(vertex_group.len()) {
                    // We only cares about how many vertices are (smaller) than Vertex
                    for j in (i + 1)..vertex_group.len() {
                        match self.cmp_vertices_for_accurate_rank(
                            vertex_group[i],
                            vertex_group[j],
                            vertex_neighbor_edges_map,
                            &mut visited_vertex_set,
                        ) {
                            // Add index of vertex j by 1 if i > j
                            Ordering::Less => accurate_rank_vec[j] += 1,
                            // Add index of vertex i by 1 if i < j
                            Ordering::Greater => accurate_rank_vec[i] += 1,
                            // Do nothing if i == j
                            Ordering::Equal => (),
                        }
                    }
                    // Set rank to Vertex i
                    self.get_vertex_mut_from_id(vertex_group[i])
                        .unwrap()
                        .set_rank(accurate_rank_vec[i]);
                }
            }
        }
    }

    /// Get all the vertex groups that share the same initial indices
    ///
    /// Only these groups are considered when setting accurate indices
    ///
    /// Return: A vector of vertex groups sharing the same initial indices
    fn get_same_rank_vertex_groups(&mut self) -> Vec<Vec<PatternId>> {
        let mut same_rank_vertex_groups: Vec<Vec<PatternId>> = vec![];
        for (_, vertex_set) in self.get_vertex_label_map().iter() {
            let mut vertex_vec: Vec<PatternId> = vec![];
            // Push all the vertices with the same label into a vector
            for v_id in vertex_set.iter() {
                vertex_vec.push(*v_id);
            }
            // Sort vertices by their ranks
            vertex_vec.sort_by(|v1_id, v2_id| {
                self.get_vertex_from_id(*v1_id)
                    .unwrap()
                    .get_rank()
                    .cmp(
                        &self
                            .get_vertex_from_id(*v2_id)
                            .unwrap()
                            .get_rank(),
                    )
            });
            let mut max_v_rank = -1;
            for i in 0..vertex_vec.len() {
                let current_v_rank = self
                    .get_vertex_from_id(vertex_vec[i])
                    .unwrap()
                    .get_rank();
                match current_v_rank.cmp(&max_v_rank) {
                    Ordering::Greater => {
                        same_rank_vertex_groups.push(vec![vertex_vec[i]]);
                        max_v_rank = current_v_rank;
                    }
                    Ordering::Equal => same_rank_vertex_groups
                        .last_mut()
                        .unwrap()
                        .push(vertex_vec[i]),
                    Ordering::Less => {
                        panic!("Error in setting accurate rank: vertex_vec is not well sorted")
                    }
                }
            }
        }

        same_rank_vertex_groups.retain(|vertex_group| vertex_group.len() > 1);
        same_rank_vertex_groups
    }

    fn cmp_vertices_for_accurate_rank(
        &mut self, v1_id: PatternId, v2_id: PatternId,
        vertex_neighbor_info_map: &HashMap<PatternId, Vec<(PatternId, PatternId)>>,
        visited_vertex_set: &mut HashSet<PatternId>,
    ) -> Ordering {
        // Return Equal if the Two Vertices Have the Same Index
        if v1_id == v2_id {
            return Ordering::Equal;
        }
        // Compare their Indices
        let v1_rank = self
            .get_vertex_from_id(v1_id)
            .unwrap()
            .get_rank();
        let v2_rank = self
            .get_vertex_from_id(v2_id)
            .unwrap()
            .get_rank();
        match v1_rank.cmp(&v2_rank) {
            Ordering::Greater => return Ordering::Greater,
            Ordering::Less => return Ordering::Less,
            Ordering::Equal => (),
        }
        // Return Equal if the Two Vertices Have Been Set As Visited
        if visited_vertex_set.contains(&v1_id) && visited_vertex_set.contains(&v2_id) {
            return Ordering::Equal;
        }
        // Set these two vertices as visited
        // HashSet handles the case when inserting an existing value, so no worry for duplication
        visited_vertex_set.insert(v1_id);
        visited_vertex_set.insert(v2_id);
        // What about the case when neighbor info retriving fails?
        let v1_neighbor_info = vertex_neighbor_info_map.get(&v1_id).unwrap();
        let v2_neighbor_info = vertex_neighbor_info_map.get(&v2_id).unwrap();
        if v1_neighbor_info.len() != v2_neighbor_info.len() {
            panic!("Error in cmp_vertices_for_accurate_rank function: failed to set vertex neighbor info");
        }
        for i in 0..v1_neighbor_info.len() {
            let v1_neighbor_vertex_id = v1_neighbor_info[i].1;
            let v2_neighbor_vertex_id = v2_neighbor_info[i].1;
            // Skip the steps below if the two neighbor vertices are visited
            if visited_vertex_set.contains(&v1_neighbor_vertex_id)
                && visited_vertex_set.contains(&v2_neighbor_vertex_id)
            {
                continue;
            }
            let order: Ordering = self.cmp_vertices_for_accurate_rank(
                v1_neighbor_vertex_id,
                v2_neighbor_vertex_id,
                vertex_neighbor_info_map,
                visited_vertex_set,
            );
            match order {
                Ordering::Greater => {
                    visited_vertex_set.remove(&v1_id);
                    visited_vertex_set.remove(&v2_id);
                    return Ordering::Greater;
                }
                Ordering::Less => {
                    visited_vertex_set.remove(&v1_id);
                    visited_vertex_set.remove(&v2_id);
                    return Ordering::Less;
                }
                Ordering::Equal => continue,
            }
        }
        // Return Equal if Still Cannot Distinguish
        Ordering::Equal
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

    /// Extend the current Pattern to a new Pattern with the given ExtendStep
    /// If the ExtendStep is not matched with the current Pattern, the function will return None
    /// Else, it will return the new Pattern after the extension
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
                match extend_edges[i].get_direction() {
                    // Case that the extend edge's direciton is Out
                    PatternDirection::Out => {
                        // new pattern edge info
                        let new_pattern_edge = PatternEdge {
                            id: new_pattern.get_next_pattern_edge_id(),
                            label: extend_edges[i].get_edge_label(),
                            start_v_id: vertices_can_use[i],
                            end_v_id: new_pattern_vertex.id,
                            start_v_label: self
                                .vertices
                                .get(vertices_can_use[i])
                                .unwrap()
                                .label,
                            end_v_label: new_pattern_vertex.label,
                        };
                        // update newly extended pattern vertex's connection info
                        new_pattern_vertex
                            .adjacent_edges
                            .insert(new_pattern_edge.id, (vertices_can_use[i], PatternDirection::In));
                        new_pattern_vertex.adjacent_vertices.insert(
                            vertices_can_use[i],
                            vec![(new_pattern_edge.id, PatternDirection::Out)],
                        );
                        new_pattern_vertex.in_degree += 1;
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
                    // Case that the extend edge's direction is Incoming
                    PatternDirection::In => {
                        let new_pattern_edge = PatternEdge {
                            id: new_pattern.get_next_pattern_edge_id(),
                            label: extend_edges[i].get_edge_label(),
                            start_v_id: new_pattern_vertex.id,
                            end_v_id: vertices_can_use[i],
                            start_v_label: new_pattern_vertex.label,
                            end_v_label: self
                                .vertices
                                .get(vertices_can_use[i])
                                .unwrap()
                                .label,
                        };
                        new_pattern_vertex
                            .adjacent_edges
                            .insert(new_pattern_edge.id, (vertices_can_use[i], PatternDirection::Out));
                        new_pattern_vertex
                            .adjacent_vertices
                            .insert(vertices_can_use[i], vec![(new_pattern_edge.id, PatternDirection::In)]);
                        new_pattern_vertex.out_degree += 1;
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
        Some(new_pattern)
    }

    /// Find all possible ExtendSteps of current pattern based on the given Pattern Meta
    pub fn get_extend_steps(&self, pattern_meta: &PatternMeta) -> Vec<ExtendStep> {
        let mut extend_steps = vec![];
        // Get all vertex labels from pattern meta as the possible extend target vertex
        let target_v_labels = pattern_meta.get_all_vertex_label_ids();
        // For every possible extend target vertex label, find its all connect edges to the current pattern
        for target_v_label in target_v_labels {
            // The collection of (the collection of extend edges)
            let mut extend_edgess = vec![];
            // The collection of extend edges with a source vertex id
            // The source vertex id is used to specify the extend edge is from which vertex of the pattern
            let mut extend_edges_with_src_id = vec![];
            for (_, src_vertex) in &self.vertices {
                // check whether there are some edges between the target vertex and the current source vertex
                let connect_edges = pattern_meta.get_associated_elabels(src_vertex.label, target_v_label);
                // Transform all the connect edges to ExtendEdge and add to extend_edges_with_src_id
                for connect_edge in connect_edges {
                    let extend_edge =
                        ExtendEdge::new(src_vertex.label, src_vertex.rank, connect_edge.0, connect_edge.1);
                    extend_edges_with_src_id.push((extend_edge, src_vertex.id));
                }
            }
            // Get the subsets of extend_edges_with_src_id, and add every subset to the extend edgess
            // The algorithm is BFS Search
            let mut queue = VecDeque::new();
            for (i, extend_edge) in extend_edges_with_src_id.iter().enumerate() {
                queue.push_back((vec![extend_edge.clone()], i + 1));
            }
            while queue.len() > 0 {
                let (extend_edges_combinations, max_rank) = queue.pop_front().unwrap();
                let mut extend_edges = Vec::with_capacity(extend_edges_combinations.len());
                for (extend_edge, _) in &extend_edges_combinations {
                    extend_edges.push(*extend_edge);
                }
                extend_edgess.push(extend_edges);
                // Can't have more than one edge between two vertices (may be canceled in the future)
                'outer: for i in max_rank..extend_edges_with_src_id.len() {
                    for (_, src_id) in &extend_edges_combinations {
                        if *src_id == extend_edges_with_src_id[i].1 {
                            continue 'outer;
                        }
                    }
                    let mut new_extend_edges_combinations = extend_edges_combinations.clone();
                    new_extend_edges_combinations.push(extend_edges_with_src_id[i]);
                    queue.push_back((new_extend_edges_combinations, i + 1));
                }
            }
            for extend_edges in extend_edgess {
                let extend_step = ExtendStep::from((target_v_label, extend_edges));
                extend_steps.push(extend_step);
            }
        }
        extend_steps
    }
}

#[cfg(test)]
mod tests {
    use super::Pattern;
    use super::PatternDirection;
    use crate::catalogue::codec::*;
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
        let mut vertex_0_connect_edges_iter = vertex_0.adjacent_edges.iter();
        let (v0_e0, (v0_v0, v0_d0)) = vertex_0_connect_edges_iter.next().unwrap();
        assert_eq!(*v0_e0, 0);
        assert_eq!(*v0_v0, 1);
        assert_eq!(*v0_d0, PatternDirection::Out);
        let (v0_e1, (v0_v1, v0_d1)) = vertex_0_connect_edges_iter.next().unwrap();
        assert_eq!(*v0_e1, 1);
        assert_eq!(*v0_v1, 1);
        assert_eq!(*v0_d1, PatternDirection::In);
        assert_eq!(vertex_0.adjacent_vertices.len(), 1);
        let v0_v1_connected_edges = vertex_0.adjacent_vertices.get(&1).unwrap();
        assert_eq!(v0_v1_connected_edges.len(), 2);
        let mut v0_v1_connected_edges_iter = v0_v1_connected_edges.iter();
        assert_eq!(*v0_v1_connected_edges_iter.next().unwrap(), (0, PatternDirection::Out));
        assert_eq!(*v0_v1_connected_edges_iter.next().unwrap(), (1, PatternDirection::In));
        let vertex_1 = pattern_case1.vertices.get(1).unwrap();
        assert_eq!(vertex_1.id, 1);
        assert_eq!(vertex_1.label, 0);
        assert_eq!(vertex_1.adjacent_edges.len(), 2);
        let mut vertex_1_connect_edges_iter = vertex_1.adjacent_edges.iter();
        let (v1_e0, (v1_v0, v1_d0)) = vertex_1_connect_edges_iter.next().unwrap();
        assert_eq!(*v1_e0, 0);
        assert_eq!(*v1_v0, 0);
        assert_eq!(*v1_d0, PatternDirection::In);
        let (v1_e1, (v1_v1, v1_d1)) = vertex_1_connect_edges_iter.next().unwrap();
        assert_eq!(*v1_e1, 1);
        assert_eq!(*v1_v1, 0);
        assert_eq!(*v1_d1, PatternDirection::Out);
        assert_eq!(vertex_1.adjacent_vertices.len(), 1);
        let v1_v0_connected_edges = vertex_1.adjacent_vertices.get(&0).unwrap();
        assert_eq!(v1_v0_connected_edges.len(), 2);
        let mut v1_v0_connected_edges_iter = v1_v0_connected_edges.iter();
        assert_eq!(*v1_v0_connected_edges_iter.next().unwrap(), (0, PatternDirection::In));
        assert_eq!(*v1_v0_connected_edges_iter.next().unwrap(), (1, PatternDirection::Out));
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
            for (connect_edge1_id, (connect_vertex1_id, dir1)) in &vertex1.adjacent_edges {
                let (connect_vertex2_id, dir2) = vertex2
                    .adjacent_edges
                    .get(connect_edge1_id)
                    .unwrap();
                assert_eq!(*connect_vertex1_id, *connect_vertex2_id);
                assert_eq!(*dir1, *dir2);
            }
            for (connect_vertex1_id, edge_connections1) in &vertex1.adjacent_vertices {
                let edge_connections2 = vertex2
                    .adjacent_vertices
                    .get(connect_vertex1_id)
                    .unwrap();
                let (connect_edge1_id, dir1) = edge_connections1[0];
                let (connect_edge2_id, dir2) = edge_connections2[0];
                assert_eq!(connect_edge1_id, connect_edge2_id);
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
        let all_extend_steps = person_only_pattern.get_extend_steps(&modern_pattern_meta);
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
        let all_extend_steps = person_only_pattern.get_extend_steps(&modern_pattern_meta);
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
        let all_extend_steps = person_knows_person.get_extend_steps(&modern_pattern_meta);
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
        let all_extend_steps = person_created_software.get_extend_steps(&modern_pattern_meta);
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
        let all_extend_steps = person_knows_person.get_extend_steps(&ldbc_pattern_meta);
        assert_eq!(all_extend_steps.len(), 41);
    }

    #[test]
    fn set_initial_rank_case1() {
        let mut pattern = build_pattern_case1();
        let vertex_neighbor_edges_map = pattern.get_vertex_neighbor_edges();
        pattern.set_initial_rank(&vertex_neighbor_edges_map);
        let vertices = pattern.get_vertices();
        assert_eq!(vertices.get(0).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(1).unwrap().get_rank(), 0);
    }

    #[test]
    fn set_initial_rank_case2() {
        let mut pattern = build_pattern_case2();
        let vertex_neighbor_edges_map = pattern.get_vertex_neighbor_edges();
        pattern.set_initial_rank(&vertex_neighbor_edges_map);
        let vertices = pattern.get_vertices();
        assert_eq!(vertices.get(0).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(1).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(2).unwrap().get_rank(), 0);
    }

    #[test]
    fn set_initial_rank_case3() {
        let mut pattern = build_pattern_case3();
        let vertex_neighbor_edges_map = pattern.get_vertex_neighbor_edges();
        pattern.set_initial_rank(&vertex_neighbor_edges_map);
        let vertices = pattern.get_vertices();
        assert_eq!(vertices.get(0).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(1).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(2).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(3).unwrap().get_rank(), 0);
    }

    #[test]
    fn set_initial_rank_case4() {
        let mut pattern = build_pattern_case4();
        let vertex_neighbor_edges_map = pattern.get_vertex_neighbor_edges();
        pattern.set_initial_rank(&vertex_neighbor_edges_map);
        let vertices = pattern.get_vertices();
        assert_eq!(vertices.get(0).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(1).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(2).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(3).unwrap().get_rank(), 0);
    }

    #[test]
    fn set_initial_rank_case5() {
        let mut pattern = build_pattern_case5();
        let vertex_neighbor_edges_map = pattern.get_vertex_neighbor_edges();
        pattern.set_initial_rank(&vertex_neighbor_edges_map);
        let id_vec_a: Vec<PatternId> = vec![100, 200, 300, 400];
        let id_vec_b: Vec<PatternId> = vec![10, 20, 30];
        let id_vec_c: Vec<PatternId> = vec![1, 2, 3];
        let id_vec_d: Vec<PatternId> = vec![1000];
        let vertices = pattern.get_vertices();
        // A
        assert_eq!(vertices.get(id_vec_a[0]).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(id_vec_a[1]).unwrap().get_rank(), 3);
        assert_eq!(vertices.get(id_vec_a[2]).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(id_vec_a[3]).unwrap().get_rank(), 1);
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
    fn set_accurate_rank_case1() {
        let mut pattern = build_pattern_case1();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
        assert_eq!(vertices.get(0).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(1).unwrap().get_rank(), 0);
    }

    #[test]
    fn set_accurate_rank_case2() {
        let mut pattern = build_pattern_case2();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
        assert_eq!(vertices.get(0).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(1).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(2).unwrap().get_rank(), 0);
    }

    #[test]
    fn set_accurate_rank_case3() {
        let mut pattern = build_pattern_case3();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
        assert_eq!(vertices.get(0).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(1).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(2).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(3).unwrap().get_rank(), 0);
    }

    #[test]
    fn set_accurate_rank_case4() {
        let mut pattern = build_pattern_case4();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
        assert_eq!(vertices.get(0).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(1).unwrap().get_rank(), 0);
        assert_eq!(vertices.get(2).unwrap().get_rank(), 1);
        assert_eq!(vertices.get(3).unwrap().get_rank(), 0);
    }

    #[test]
    fn set_accurate_rank_case5() {
        let mut pattern = build_pattern_case5();
        pattern.rank_ranking();
        let id_vec_a: Vec<PatternId> = vec![100, 200, 300, 400];
        let id_vec_b: Vec<PatternId> = vec![10, 20, 30];
        let id_vec_c: Vec<PatternId> = vec![1, 2, 3];
        let id_vec_d: Vec<PatternId> = vec![1000];
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case1();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case2();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case3();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case4();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case5();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case6();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case7();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case8();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case9();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case10();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case11();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case12();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case13();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case14();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case15();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case16();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case17();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
    fn index_ranking_case17_special_id_situation_1() {
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case17_special_id_situation_1();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case17_special_id_situation_2();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
        let (mut pattern, vertex_id_map) = build_pattern_rank_ranking_case18();
        pattern.rank_ranking();
        let vertices = pattern.get_vertices();
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
    fn test_encode_decode_of_case1() {
        let mut pattern = build_pattern_case1();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        pattern.rank_ranking();
        let code1: Vec<u8> = pattern.encode_to(&encoder);
        let mut pattern: Pattern = Pattern::decode_from(code1.clone(), &encoder).unwrap();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        pattern.rank_ranking();
        let code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }
}
