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

use std::cmp::{max, Ordering};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use fast_math::log2;

use crate::extend_step::{ExtendEdge, ExtendStep};
use crate::pattern_meta::PatternMeta;
use crate::{Direction, Index, LabelID, ID};

#[derive(Debug, Clone)]
pub struct PatternVertex {
    id: ID,
    label: LabelID,
    /// Used to Identify vertices with same label
    index: Index,
    /// Key: edge id, Value: (vertex id, direction)
    connect_edges: BTreeMap<ID, (ID, Direction)>,
    /// Key: vertex id, Value: Vec<(edge id, direction)>
    connect_vertices: BTreeMap<ID, Vec<(ID, Direction)>>,
    /// How many out edges connected to this vertex
    out_degree: usize,
    /// How many in edges connected to this vertex
    in_degree: usize,
}

/// Methods to access the fields of a PatternVertex
impl PatternVertex {
    pub fn get_id(&self) -> ID {
        self.id
    }

    pub fn get_label(&self) -> LabelID {
        self.label
    }

    pub fn get_index(&self) -> Index {
        self.index
    }

    pub fn get_connected_edges(&self) -> &BTreeMap<i32, (i32, Direction)> {
        &self.connect_edges
    }

    pub fn get_connected_vertices(&self) -> &BTreeMap<i32, Vec<(i32, Direction)>> {
        &self.connect_vertices
    }

    pub fn get_out_degree(&self) -> usize {
        self.out_degree
    }

    pub fn get_in_degree(&self) -> usize {
        self.in_degree
    }

    /// Get how many connections(both out and in) the current pattern vertex has
    pub fn get_connect_num(&self) -> usize {
        self.connect_edges.len()
    }

    /// Given a edge id, get the vertex connected to the current vertex through the edge with the connect direction
    pub fn get_connect_vertex_by_edge_id(&self, edge_id: ID) -> Option<(ID, Direction)> {
        self.connect_edges.get(&edge_id).cloned()
    }

    /// Given a vertex id, get all the edges connecting the given vertex and current vertex with the connect direction
    pub fn get_connect_edges_by_vertex_id(&self, vertex_id: ID) -> Vec<(ID, Direction)> {
        match self.connect_vertices.get(&vertex_id) {
            Some(connect_edges) => connect_edges.clone(),
            None => Vec::new(),
        }
    }

    /// Setters
    pub fn set_index(&mut self, index: i32) {
        self.index = index;
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PatternEdge {
    id: ID,
    label: LabelID,
    start_v_id: ID,
    end_v_id: ID,
    start_v_label: LabelID,
    end_v_label: LabelID,
}

/// Initializers of PatternEdge
impl PatternEdge {
    /// Initializer
    pub fn new(
        id: ID, label: LabelID, start_v_id: ID, end_v_id: ID, start_v_label: LabelID, end_v_label: LabelID,
    ) -> PatternEdge {
        PatternEdge { id, label, start_v_id, end_v_id, start_v_label, end_v_label }
    }
}

/// Methods to access the fields of a PatternEdge
impl PatternEdge {
    pub fn get_id(&self) -> ID {
        self.id
    }

    pub fn get_label(&self) -> LabelID {
        self.label
    }

    pub fn get_start_vertex_id(&self) -> ID {
        self.start_v_id
    }

    pub fn get_end_vertex_id(&self) -> ID {
        self.end_v_id
    }

    pub fn get_start_vertex_label(&self) -> LabelID {
        self.start_v_label
    }

    pub fn get_end_vertex_label(&self) -> LabelID {
        self.end_v_label
    }
}

#[derive(Debug, Clone)]
pub struct Pattern {
    /// Key: edge id, Value: struct PatternEdge
    edges: BTreeMap<ID, PatternEdge>,
    /// Key: vertex id, Value: struct PatternVertex
    vertices: BTreeMap<ID, PatternVertex>,
    /// Key: edge label id, Value: BTreeSet<edge id>
    edge_label_map: BTreeMap<LabelID, BTreeSet<ID>>,
    /// Key: vertex label id, Value: BTreeSet<vertex id>
    vertex_label_map: BTreeMap<LabelID, BTreeSet<ID>>,
}

/// Initializers of Pattern
/// Initialize a Pattern containing only one vertex from hte vertex's id and label
impl From<(ID, LabelID)> for Pattern {
    fn from((vertex_id, vertex_label): (ID, LabelID)) -> Pattern {
        let vertex = PatternVertex {
            id: vertex_id,
            label: vertex_label,
            index: 0,
            connect_edges: BTreeMap::new(),
            connect_vertices: BTreeMap::new(),
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
            edges: BTreeMap::new(),
            vertices: BTreeMap::from([(vertex.id, vertex.clone())]),
            edge_label_map: BTreeMap::new(),
            vertex_label_map: BTreeMap::from([(vertex.label, BTreeSet::from([vertex.id]))]),
        }
    }
}

/// Initialize a Pattern from a vertor of Pattern Edges
impl From<Vec<PatternEdge>> for Pattern {
    fn from(edges: Vec<PatternEdge>) -> Pattern {
        if edges.len() == 0 {
            panic!(
                "There should be at least one pattern edge to get a pattern. 
                   To get a pattern with single vertex, it shoud call from Pattern Vertex"
            )
        }
        let mut new_pattern = Pattern {
            edges: BTreeMap::new(),
            vertices: BTreeMap::new(),
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
            // Add or update the start & end vertex to the new Pattern
            match new_pattern.vertices.get_mut(&edge.start_v_id) {
                // the start vertex existed, just update the connection info
                Some(start_vertex) => {
                    start_vertex
                        .connect_edges
                        .insert(edge.id, (edge.end_v_id, Direction::Out));
                    let start_vertex_connect_vertices_vec = start_vertex
                        .connect_vertices
                        .entry(edge.end_v_id)
                        .or_insert(Vec::new());
                    start_vertex_connect_vertices_vec.push((edge.id, Direction::Out));
                    start_vertex.out_degree += 1;
                }
                // the start vertex not existed, add to the new Pattern
                None => {
                    new_pattern.vertices.insert(
                        edge.start_v_id,
                        PatternVertex {
                            id: edge.start_v_id,
                            label: edge.start_v_label,
                            index: 0,
                            connect_edges: BTreeMap::from([(edge.id, (edge.end_v_id, Direction::Out))]),
                            connect_vertices: BTreeMap::from([(
                                edge.end_v_id,
                                vec![(edge.id, Direction::Out)],
                            )]),
                            out_degree: 1,
                            in_degree: 0,
                        },
                    );
                    let vertex_set = new_pattern
                        .vertex_label_map
                        .entry(edge.start_v_label)
                        .or_insert(BTreeSet::new());
                    vertex_set.insert(edge.start_v_id);
                }
            }

            // Add or update the end vertex to the new Pattern
            match new_pattern.vertices.get_mut(&edge.end_v_id) {
                // the end vertex existed, just update the connection info
                Some(end_vertex) => {
                    end_vertex
                        .connect_edges
                        .insert(edge.id, (edge.start_v_id, Direction::In));
                    let end_vertex_connect_vertices_vec = end_vertex
                        .connect_vertices
                        .entry(edge.start_v_id)
                        .or_insert(Vec::new());
                    end_vertex_connect_vertices_vec.push((edge.id, Direction::In));
                    end_vertex.in_degree += 1;
                }
                // the end vertex not existed, add the new Pattern
                None => {
                    new_pattern.vertices.insert(
                        edge.end_v_id,
                        PatternVertex {
                            id: edge.end_v_id,
                            label: edge.end_v_label,
                            index: 0,
                            connect_edges: BTreeMap::from([(edge.id, (edge.start_v_id, Direction::In))]),
                            connect_vertices: BTreeMap::from([(
                                edge.start_v_id,
                                vec![(edge.id, Direction::In)],
                            )]),
                            out_degree: 0,
                            in_degree: 1,
                        },
                    );
                    let vertex_set = new_pattern
                        .vertex_label_map
                        .entry(edge.end_v_label)
                        .or_insert(BTreeSet::new());
                    vertex_set.insert(edge.end_v_id);
                }
            }
        }

        new_pattern
    }
}

/// Methods to access the fields of a Pattern or get some info from Pattern
impl Pattern {
    /// Get Edges References
    pub fn get_edges(&self) -> &BTreeMap<ID, PatternEdge> {
        &self.edges
    }

    /// Get Vertices References
    pub fn get_vertices(&self) -> &BTreeMap<ID, PatternVertex> {
        &self.vertices
    }

    /// Get Edge Label Map Reference
    pub fn get_edge_label_map(&self) -> &BTreeMap<LabelID, BTreeSet<ID>> {
        &self.edge_label_map
    }

    /// Get Vertex Label Map Reference
    pub fn get_vertex_label_map(&self) -> &BTreeMap<LabelID, BTreeSet<ID>> {
        &self.vertex_label_map
    }

    /// Get PatternEdge Reference from Edge ID
    pub fn get_edge_from_id(&self, edge_id: ID) -> Option<&PatternEdge> {
        self.edges.get(&edge_id)
    }

    /// Get PatternVertex Reference from Vertex ID
    pub fn get_vertex_from_id(&self, vertex_id: ID) -> Option<&PatternVertex> {
        self.vertices.get(&vertex_id)
    }

    pub fn get_edge_mut_from_id(&mut self, edge_id: ID) -> Option<&mut PatternEdge> {
        self.edges.get_mut(&edge_id)
    }

    pub fn get_vertex_mut_from_id(&mut self, vertex_id: ID) -> Option<&mut PatternVertex> {
        self.vertices.get_mut(&vertex_id)
    }

    /// Get Vertex Index from Vertex ID Reference
    pub fn get_vertex_index(&self, v_id: ID) -> Index {
        self.vertices.get(&v_id).unwrap().index
    }

    /// [Public] Get the order of both start and end vertices of an edge
    pub fn get_edge_vertices_index(&self, edge_id: ID) -> Option<(Index, Index)> {
        if let Some(edge) = self.get_edge_from_id(edge_id) {
            let start_v_index = self.get_vertex_index(edge.start_v_id);
            let end_v_index = self.get_vertex_index(edge.end_v_id);
            Some((start_v_index, end_v_index))
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

    /// Get the maximum edge label id of the current pattern
    pub fn get_max_edge_label(&self) -> Option<LabelID> {
        match self.edge_label_map.iter().last() {
            Some((max_label, _)) => Some(*max_label),
            None => None,
        }
    }

    /// Get the maximum vertex label id of the current pattern
    pub fn get_max_vertex_label(&self) -> Option<LabelID> {
        match self.vertex_label_map.iter().last() {
            Some((max_label, _)) => Some(*max_label),
            None => None,
        }
    }

    /// Compute at least how many bits are needed to represent edge labels
    /// At least 1 bit
    pub fn get_min_edge_label_bit_num(&self) -> usize {
        if let Some(max_edge_label) = self.get_max_edge_label() {
            max(1, log2((max_edge_label + 1) as f32).ceil() as usize)
        } else {
            1
        }
    }

    /// Compute at least how many bits are needed to represent vertex labels
    /// At least 1 bit
    pub fn get_min_vertex_label_bit_num(&self) -> usize {
        if let Some(max_vertex_label) = self.get_max_vertex_label() {
            max(1, log2((max_vertex_label + 1) as f32).ceil() as usize)
        } else {
            1
        }
    }

    /// Compute at least how many bits are needed to represent vertices with the same label
    /// At least 1 bit
    pub fn get_min_vertex_index_bit_num(&self) -> usize {
        // iterate through the hashmap and compute how many vertices have the same label in one set
        let mut min_index_bit_num: usize = 1;
        for (_, value) in self.vertex_label_map.iter() {
            let same_label_vertex_num = value.len() as u64;
            let index_bit_num: usize = log2(same_label_vertex_num as f32).ceil() as usize;
            if index_bit_num > min_index_bit_num {
                min_index_bit_num = index_bit_num;
            }
        }
        min_index_bit_num
    }
}

/// Methods for Pattern Encoding and Decoding
/// Include PatternVertex Reordering and PatternEdge Reordering
impl Pattern {
    fn reorder_label_vertices(&mut self, _v_label: i32) {}

    pub fn reorder_vertices(&mut self) {
        let mut v_labels = Vec::with_capacity(self.vertex_label_map.len());
        for (v_label, _) in &self.vertex_label_map {
            v_labels.push(*v_label)
        }
        for v_label in v_labels {
            self.reorder_label_vertices(v_label)
        }
    }

    /// Get a vector of ordered edges's indexes of a Pattern
    /// The comparison is based on the `cmp_edges` method above to get the Order
    pub fn get_ordered_edges(&self) -> Vec<ID> {
        let mut ordered_edges = Vec::new();
        for (&edge, _) in &self.edges {
            ordered_edges.push(edge);
        }
        ordered_edges.sort_by(|e1_id, e2_id| self.cmp_edges(*e1_id, *e2_id));
        ordered_edges
    }

    /// Get the Order of two PatternVertices of a Pattern
    fn cmp_vertices(&self, v1_id: ID, v2_id: ID) -> Ordering {
        if v1_id == v2_id {
            return Ordering::Equal;
        }
        let v1 = self.vertices.get(&v1_id).unwrap();
        let v2 = self.vertices.get(&v2_id).unwrap();
        match v1.label.cmp(&v2.label) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
        // Compare Vertex Out Degree
        match v1.out_degree.cmp(&v2.out_degree) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
        // Compare Vertex In Degree
        match v1.in_degree.cmp(&v2.in_degree) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
        // Compare the edge label and end_v_label for each connected edges of v1 and v2
        // Here, the incoming and outgoing edges should be compared separately to deal with bidirectional edge case
        // The 3-element tuple stores (edge_id, edge_label, end_v_label)
        let v1_connected_edges_iter = v1.get_connected_edges().iter();
        let v2_connected_edges_iter = v2.get_connected_edges().iter();
        let mut v1_connected_out_edges_info_array: Vec<(ID, LabelID, LabelID)> =
            Vec::with_capacity(v1.out_degree);
        let mut v1_connected_in_edges_info_array: Vec<(ID, LabelID, LabelID)> =
            Vec::with_capacity(v1.in_degree);
        let mut v2_connected_out_edges_info_array: Vec<(ID, LabelID, LabelID)> =
            Vec::with_capacity(v2.out_degree);
        let mut v2_connected_in_edges_info_array: Vec<(ID, LabelID, LabelID)> =
            Vec::with_capacity(v2.in_degree);
        for (v1_connected_edge_id, (v1_connected_edge_end_v_id, v1_connected_edge_dir)) in
            v1_connected_edges_iter
        {
            let v1_connected_edge_label = self
                .get_edge_from_id(*v1_connected_edge_id)
                .unwrap()
                .get_label();
            let v1_connected_edge_end_v_label = self
                .get_vertex_from_id(*v1_connected_edge_end_v_id)
                .unwrap()
                .get_label();
            match v1_connected_edge_dir {
                Direction::Out => v1_connected_out_edges_info_array.push((
                    *v1_connected_edge_id,
                    v1_connected_edge_label,
                    v1_connected_edge_end_v_label,
                )),
                Direction::In => v1_connected_in_edges_info_array.push((
                    *v1_connected_edge_id,
                    v1_connected_edge_label,
                    v1_connected_edge_end_v_label,
                )),
            }
        }
        for (v2_connected_edge_id, (v2_connected_edge_end_v_id, v2_connected_edge_dir)) in
            v2_connected_edges_iter
        {
            let v2_connected_edge_label = self
                .get_edge_from_id(*v2_connected_edge_id)
                .unwrap()
                .get_label();
            let v2_connected_edge_end_v_label = self
                .get_vertex_from_id(*v2_connected_edge_end_v_id)
                .unwrap()
                .get_label();
            match v2_connected_edge_dir {
                Direction::Out => v2_connected_out_edges_info_array.push((
                    *v2_connected_edge_id,
                    v2_connected_edge_label,
                    v2_connected_edge_end_v_label,
                )),
                Direction::In => v2_connected_in_edges_info_array.push((
                    *v2_connected_edge_id,
                    v2_connected_edge_label,
                    v2_connected_edge_end_v_label,
                )),
            }
        }
        // Double check the vector length in case of segmentation fault
        if (v1_connected_out_edges_info_array.len() != v2_connected_out_edges_info_array.len())
            || (v1_connected_in_edges_info_array.len() != v2_connected_in_edges_info_array.len())
        {
            panic!("Error in comparing vertices: failed to check out/in degree of vertices");
        }
        // Sort the edge arrays with respect to edge id
        v1_connected_out_edges_info_array
            .sort_by(|e1_info, e2_info| self.cmp_edges_without_index(e1_info.0, e2_info.0));
        v1_connected_in_edges_info_array
            .sort_by(|e1_info, e2_info| self.cmp_edges_without_index(e1_info.0, e2_info.0));
        v2_connected_out_edges_info_array
            .sort_by(|e1_info, e2_info| self.cmp_edges_without_index(e1_info.0, e2_info.0));
        v2_connected_in_edges_info_array
            .sort_by(|e1_info, e2_info| self.cmp_edges_without_index(e1_info.0, e2_info.0));
        // Compare the edge label and end_v_label for each connected edges of v1 and v2
        for index in 0..v1_connected_out_edges_info_array.len() {
            let v1_connected_out_edge_info =
                (v1_connected_out_edges_info_array[index].1, v1_connected_out_edges_info_array[index].2);
            let v2_connected_out_edge_info =
                (v2_connected_out_edges_info_array[index].1, v2_connected_out_edges_info_array[index].2);
            match v1_connected_out_edge_info.cmp(&v2_connected_out_edge_info) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                _ => (),
            }
        }
        for index in 0..v1_connected_in_edges_info_array.len() {
            let v1_connected_in_edge_info =
                (v1_connected_in_edges_info_array[index].1, v1_connected_in_edges_info_array[index].2);
            let v2_connected_in_edge_info =
                (v2_connected_in_edges_info_array[index].1, v2_connected_in_edges_info_array[index].2);
            match v1_connected_in_edge_info.cmp(&v2_connected_in_edge_info) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                _ => (),
            }
        }
        Ordering::Equal
    }

    /// ### Compare two edges by id
    /// Considers only the edge label, start/end vertex label
    ///
    /// Vertex Infices are not considered
    fn cmp_edges_without_index(&self, e1_id: ID, e2_id: ID) -> Ordering {
        if e1_id == e2_id {
            return Ordering::Equal;
        }
        let e1 = self.edges.get(&e1_id).unwrap();
        let e2 = self.edges.get(&e2_id).unwrap();
        // Compare Edge Label
        match e1.label.cmp(&e2.label) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
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
        // Return as equal if still cannot distinguish
        Ordering::Equal
    }

    /// ### Get the Order of two PatternEdges in a Pattern
    /// Vertex Indices are taken into consideration
    fn cmp_edges(&self, e1_id: ID, e2_id: ID) -> Ordering {
        if e1_id == e2_id {
            return Ordering::Equal;
        }
        let e1 = self.edges.get(&e1_id).unwrap();
        let e2 = self.edges.get(&e2_id).unwrap();
        // Compare Edge Label
        match e1.label.cmp(&e2.label) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
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
        // Get orders for starting vertex
        let (e1_start_v_index, e1_end_v_index) = self
            .get_edge_vertices_index(e1.get_id())
            .unwrap();
        let (e2_start_v_order, e2_end_v_order) = self
            .get_edge_vertices_index(e2.get_id())
            .unwrap();
        // Compare the order of the starting vertex
        match e1_start_v_index.cmp(&e2_start_v_order) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            _ => (),
        }
        // Compare the order of ending vertex
        match e1_end_v_index.cmp(&e2_end_v_order) {
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
    pub fn index_ranking(&mut self) {
        self.set_initial_index();
        self.set_accurate_index();
    }

    /// ### Step-1: Set Initial Indices
    /// Set Initial Vertex Index Based on Comparison of Labels and In/Out Degrees
    fn set_initial_index(&mut self) {
        for (_, vertex_set) in self.vertex_label_map.iter() {
            let mut vertex_vec = Vec::with_capacity(vertex_set.len());
            for v_id in vertex_set.iter() {
                vertex_vec.push(*v_id);
            }
            // Sort vertices from small to large
            vertex_vec.sort_by(|v1_id, v2_id| self.cmp_vertices(*v1_id, *v2_id));
            // Vertex Index is the value to be set to vertices.
            // Isomorphic Vertices may share the same vertex index
            let mut vertex_index = 0;
            // Vertex Index Implicit is to make sure that vertices sharing the same vertex index still occupy a place
            // eg: 0, 0, 2, 2, 2, 5
            let mut vertex_index_implicit = 0;
            let mut current_max_v_id = vertex_vec[0];
            for v_id in vertex_vec.iter() {
                let order = self.cmp_vertices(*v_id, current_max_v_id);
                let vertex: &mut PatternVertex = self.vertices.get_mut(v_id).unwrap();
                match order {
                    Ordering::Greater => {
                        vertex_index = vertex_index_implicit;
                        vertex.set_index(vertex_index);
                        current_max_v_id = *v_id;
                    }
                    Ordering::Equal => {
                        vertex.set_index(vertex_index);
                    }
                    Ordering::Less => {
                        panic!("Error in setting initial vertex index, vertex_vec is not sorted")
                    }
                }
                vertex_index_implicit += 1;
            }
        }
    }

    /// ### Step-2: Set Accurate Indices
    /// Set Accurate Indices According to the Initial Indices Set in Step-1
    fn set_accurate_index(&mut self) {
        // Initializde the visited Hashmap for all the vertices
        let mut visited_map: HashMap<ID, bool> = HashMap::new();
        for (v_id, _) in self.get_vertices().iter() {
            visited_map.insert(*v_id, false);
        }
        // Iteratively find a group of vertices sharing the same index
        let mut same_index_vertex_groups: Vec<Vec<ID>> = Vec::new();
        for (_, vertex_set) in self.get_vertex_label_map().iter() {
            let mut vertex_vec: Vec<ID> = Vec::new();
            // Push all the vertices with the same label into a vector
            for v_id in vertex_set.iter() {
                vertex_vec.push(*v_id);
            }
            // Sort vertices by their indices
            vertex_vec.sort_by(|v1_id, v2_id| {
                self.get_vertex_from_id(*v1_id)
                    .unwrap()
                    .get_index()
                    .cmp(
                        &self
                            .get_vertex_from_id(*v2_id)
                            .unwrap()
                            .get_index(),
                    )
            });
            let mut max_v_index = -1;
            for i in 0..vertex_vec.len() {
                let current_v_index = self
                    .get_vertex_from_id(vertex_vec[i])
                    .unwrap()
                    .get_index();
                match current_v_index.cmp(&max_v_index) {
                    Ordering::Greater => {
                        same_index_vertex_groups.push(vec![vertex_vec[i]]);
                        max_v_index = current_v_index;
                    }
                    Ordering::Equal => same_index_vertex_groups
                        .last_mut()
                        .unwrap()
                        .push(vertex_vec[i]),
                    Ordering::Less => {
                        panic!("Error in setting accurate index: vertex_vec is not well sorted")
                    }
                }
            }
        }
        same_index_vertex_groups.retain(|vertex_group| vertex_group.len() > 1);

        // Constructing Neighboring Information for Vertex Groups Sharing the Same Index
        let mut vertex_neighbor_info_map: HashMap<ID, Vec<ID>> = HashMap::new();
        for i in 0..same_index_vertex_groups.len() {
            let vertex_group: &Vec<i32> = &same_index_vertex_groups[i];
            if vertex_group.len() == 1 {
                continue;
            } else {
                for v_id in vertex_group {
                    let neighbor_info: &mut Vec<(ID, ID)> = &mut Vec::new();
                    for (edge_id, (target_v_id, _)) in self
                        .get_vertex_from_id(*v_id)
                        .unwrap()
                        .get_connected_edges()
                        .iter()
                    {
                        neighbor_info.push((*edge_id, *target_v_id));
                    }
                    neighbor_info.sort_by(|edge_1, edge_2| self.cmp_edges(edge_1.0, edge_2.0));
                    let mut neighbor_vertices: Vec<ID> = Vec::with_capacity(neighbor_info.len());
                    for i in 0..neighbor_info.len() {
                        neighbor_vertices.push(neighbor_info[i].1);
                    }
                    neighbor_vertices.dedup();
                    vertex_neighbor_info_map.insert(*v_id, neighbor_vertices);
                }
            }
        }
        // Iteratively Compare the Indices of Vertices
        for vertex_group in same_index_vertex_groups {
            if vertex_group.len() == 1 {
                continue;
            }
            if vertex_group.len() > 1 {
                let initial_index: Index = self
                    .get_vertex_from_id(vertex_group[0])
                    .unwrap()
                    .get_index();
                let mut accurate_index_vec: Vec<Index> = Vec::with_capacity(vertex_group.len());
                for _ in 0..vertex_group.len() {
                    accurate_index_vec.push(initial_index);
                }
                for i in 0..(vertex_group.len()) {
                    // We only cares about how many vertices are (smaller) than Vertex i
                    for j in (i + 1)..vertex_group.len() {
                        match self.cmp_vertices_for_accurate_index(
                            vertex_group[i],
                            vertex_group[j],
                            &mut vertex_neighbor_info_map,
                            &mut visited_map,
                        ) {
                            Ordering::Less => accurate_index_vec[j] += 1,
                            Ordering::Greater => accurate_index_vec[i] += 1,
                            Ordering::Equal => (),
                        }
                    }
                    // Set index to Vertex i
                    self.get_vertex_mut_from_id(vertex_group[i])
                        .unwrap()
                        .set_index(accurate_index_vec[i]);
                }
            }
        }
    }

    fn cmp_vertices_for_accurate_index(
        &mut self, v1_id: ID, v2_id: ID, vertex_neighbor_info_map: &HashMap<ID, Vec<ID>>,
        visited_map: &mut HashMap<ID, bool>,
    ) -> Ordering {
        // Return Equal if the Two Vertices Have the Same Index
        if v1_id == v2_id {
            return Ordering::Equal;
        }
        // Compare their Indices
        let v1_index = self
            .get_vertex_from_id(v1_id)
            .unwrap()
            .get_index();
        let v2_index = self
            .get_vertex_from_id(v2_id)
            .unwrap()
            .get_index();
        match v1_index.cmp(&v2_index) {
            Ordering::Greater => return Ordering::Greater,
            Ordering::Less => return Ordering::Less,
            Ordering::Equal => (),
        }
        // Return Equal if the Two Vertices Have Been Set As Visited
        if *visited_map.get(&v1_id).unwrap() && *visited_map.get(&v2_id).unwrap() {
            return Ordering::Equal;
        }
        // Set these two vertices as visited
        *visited_map.get_mut(&v1_id).unwrap() = true;
        *visited_map.get_mut(&v2_id).unwrap() = true;
        // What about the case when neighbor info retriving fails?
        let v1_neighbor_info = vertex_neighbor_info_map.get(&v1_id).unwrap();
        let v2_neighbor_info = vertex_neighbor_info_map.get(&v2_id).unwrap();
        if v1_neighbor_info.len() != v2_neighbor_info.len() {
            panic!("Error in cmp_vertices_for_accurate_index function: failed to set vertex neighbor info");
        }
        let neighbor_num: usize = v1_neighbor_info.len();
        for i in 0..neighbor_num {
            let v1_neighbor_vertex_id = v1_neighbor_info[i];
            let v2_neighbor_vertex_id = v2_neighbor_info[i];
            // Skip the steps below if the two neighbor vertices are visited
            if *visited_map.get(&v1_neighbor_vertex_id).unwrap()
                && *visited_map.get(&v2_neighbor_vertex_id).unwrap()
            {
                continue;
            }
            let order: Ordering = self.cmp_vertices_for_accurate_index(
                v1_neighbor_vertex_id,
                v2_neighbor_vertex_id,
                vertex_neighbor_info_map,
                visited_map,
            );
            match order {
                Ordering::Greater => {
                    *visited_map.get_mut(&v1_id).unwrap() = false;
                    *visited_map.get_mut(&v2_id).unwrap() = false;
                    return Ordering::Greater;
                }
                Ordering::Less => {
                    *visited_map.get_mut(&v1_id).unwrap() = false;
                    *visited_map.get_mut(&v2_id).unwrap() = false;
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
    /// Get all the vertices(id) with the same vertex label and vertex index
    /// These vertices are equivalent in the Pattern
    fn get_equivalent_vertices(&self, v_label: LabelID, v_index: Index) -> Vec<ID> {
        let mut equivalent_vertices = Vec::new();
        if let Some(vs_with_same_label) = self.vertex_label_map.get(&v_label) {
            for v_id in vs_with_same_label {
                if let Some(vertex) = self.vertices.get(v_id) {
                    if vertex.index == v_index {
                        equivalent_vertices.push(*v_id);
                    }
                }
            }
        }
        equivalent_vertices
    }

    /// Get the legal id for the future incoming vertex
    fn get_next_pattern_vertex_id(&self) -> ID {
        let mut new_vertex_id = self.vertices.len() as ID;
        while self.vertices.contains_key(&new_vertex_id) {
            new_vertex_id += 1;
        }
        new_vertex_id
    }

    /// Get the legal id for the future incoming vertex
    fn get_next_pattern_edge_id(&self) -> ID {
        let mut new_edge_id = self.edges.len() as ID;
        while self.edges.contains_key(&new_edge_id) {
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
            index: 0,
            connect_edges: BTreeMap::new(),
            connect_vertices: BTreeMap::new(),
            out_degree: 0,
            in_degree: 0,
        };
        for ((v_label, v_index), extend_edges) in extend_step.iter() {
            // Get all the vertices which can be used to extend with these extend edges
            let vertices_can_use = self.get_equivalent_vertices(*v_label, *v_index);
            // There's no enough vertices to extend, just return None
            if vertices_can_use.len() < extend_edges.len() {
                return None;
            }
            // Connect each vertex can be use to each extend edge
            for i in 0..extend_edges.len() {
                match extend_edges[i].get_direction() {
                    // Case that the extend edge's direciton is Out
                    Direction::Out => {
                        // new pattern edge info
                        let new_pattern_edge = PatternEdge {
                            id: new_pattern.get_next_pattern_edge_id(),
                            label: extend_edges[i].get_edge_label(),
                            start_v_id: vertices_can_use[i],
                            end_v_id: new_pattern_vertex.id,
                            start_v_label: self
                                .vertices
                                .get(&vertices_can_use[i])
                                .unwrap()
                                .label,
                            end_v_label: new_pattern_vertex.label,
                        };
                        // update newly extended pattern vertex's connection info
                        new_pattern_vertex
                            .connect_edges
                            .insert(new_pattern_edge.id, (vertices_can_use[i], Direction::In));
                        new_pattern_vertex
                            .connect_vertices
                            .insert(vertices_can_use[i], vec![(new_pattern_edge.id, Direction::Out)]);
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
                    Direction::In => {
                        let new_pattern_edge = PatternEdge {
                            id: new_pattern.get_next_pattern_edge_id(),
                            label: extend_edges[i].get_edge_label(),
                            start_v_id: new_pattern_vertex.id,
                            end_v_id: vertices_can_use[i],
                            start_v_label: new_pattern_vertex.label,
                            end_v_label: self
                                .vertices
                                .get(&vertices_can_use[i])
                                .unwrap()
                                .label,
                        };
                        new_pattern_vertex
                            .connect_edges
                            .insert(new_pattern_edge.id, (vertices_can_use[i], Direction::Out));
                        new_pattern_vertex
                            .connect_vertices
                            .insert(vertices_can_use[i], vec![(new_pattern_edge.id, Direction::In)]);
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
        new_pattern.reorder_label_vertices(target_v_label);
        Some(new_pattern)
    }

    /// Find all possible ExtendSteps of current pattern based on the given Pattern Meta
    pub fn get_extend_steps(&self, pattern_meta: &PatternMeta) -> Vec<ExtendStep> {
        let mut extend_steps = Vec::new();
        // Get all vertex labels from pattern meta as the possible extend target vertex
        let target_v_labels = pattern_meta.get_all_vertex_label_ids();
        // For every possible extend target vertex label, find its all connect edges to the current pattern
        for target_v_label in target_v_labels {
            // The collection of (the collection of extend edges)
            let mut extend_edgess = Vec::new();
            // The collection of extend edges with a source vertex id
            // The source vertex id is used to specify the extend edge is from which vertex of the pattern
            let mut extend_edges_with_src_id = Vec::new();
            for (_, src_vertex) in &self.vertices {
                // check whether there are some edges between the target vertex and the current source vertex
                let connect_edges =
                    pattern_meta.get_edges_between_vertices(src_vertex.label, target_v_label);
                // Transform all the connect edges to ExtendEdge and add to extend_edges_with_src_id
                for connect_edge in connect_edges {
                    let extend_edge =
                        ExtendEdge::new(src_vertex.label, src_vertex.index, connect_edge.0, connect_edge.1);
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
                let (extend_edges_combinations, max_index) = queue.pop_front().unwrap();
                let mut extend_edges = Vec::with_capacity(extend_edges_combinations.len());
                for (extend_edge, _) in &extend_edges_combinations {
                    extend_edges.push(*extend_edge);
                }
                extend_edgess.push(extend_edges);
                // Can't have more than one edge between two vertices (may be canceled in the future)
                'outer: for i in max_index..extend_edges_with_src_id.len() {
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
    use std::collections::{BTreeMap, HashMap};
    use std::fs::File;

    use ir_core::{plan::meta::Schema, JsonIO};
    use rand::Rng;

    use super::Direction;
    use super::Pattern;
    use super::PatternEdge;
    use super::PatternMeta;
    use super::{ExtendEdge, ExtendStep};
    use crate::codec::*;
    use crate::ID;

    /// The pattern looks like:
    /// A <-> A
    /// where <-> means two edges
    /// A's label's id is 0
    /// The edges's labels' id are both 0
    /// The left A has id 0
    /// The right A has id 1
    fn build_pattern_case1() -> Pattern {
        let pattern_vec = vec![
            PatternEdge { id: 0, label: 0, start_v_id: 0, end_v_id: 1, start_v_label: 0, end_v_label: 0 },
            PatternEdge { id: 1, label: 0, start_v_id: 1, end_v_id: 0, start_v_label: 0, end_v_label: 0 },
        ];
        Pattern::from(pattern_vec)
    }

    /// The pattern looks like:
    ///         B
    ///       /   \
    ///      A <-> A
    /// where <-> means two edges
    /// A's label id is 0, B's label id is 1
    /// The edges between two As have label id 0
    /// The edges between A and B have label id 1
    /// The left A has id 0
    /// The right A has id 1
    /// B has id 2
    fn build_pattern_case2() -> Pattern {
        let pattern_vec = vec![
            PatternEdge { id: 0, label: 0, start_v_id: 0, end_v_id: 1, start_v_label: 0, end_v_label: 0 },
            PatternEdge { id: 1, label: 0, start_v_id: 1, end_v_id: 0, start_v_label: 0, end_v_label: 0 },
            PatternEdge { id: 2, label: 1, start_v_id: 0, end_v_id: 2, start_v_label: 0, end_v_label: 1 },
            PatternEdge { id: 3, label: 1, start_v_id: 1, end_v_id: 2, start_v_label: 0, end_v_label: 1 },
        ];
        Pattern::from(pattern_vec)
    }

    /// The pattern looks like:
    ///     B(2) -> B(3)
    ///     |       |
    ///     A(0) -> A(1)
    /// where <-> means two edges
    /// Vertex Label Map:
    ///     A: 0, B: 1,
    /// Edge Label Map:
    ///     A-A: 0, A->B: 1, B-B: 2,
    fn build_pattern_case3() -> Pattern {
        let mut rng = rand::thread_rng();
        let pattern_vec = vec![
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 0,
                start_v_id: 0,
                end_v_id: 1,
                start_v_label: 0,
                end_v_label: 0,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 1,
                start_v_id: 0,
                end_v_id: 2,
                start_v_label: 0,
                end_v_label: 1,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 1,
                start_v_id: 1,
                end_v_id: 3,
                start_v_label: 0,
                end_v_label: 1,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 2,
                start_v_id: 2,
                end_v_id: 3,
                start_v_label: 1,
                end_v_label: 1,
            },
        ];
        Pattern::from(pattern_vec)
    }

    /// The pattern looks like:
    ///     B(2)  -> B(3)
    ///     |        |
    ///     A(0) <-> A(1)
    /// where <-> means two edges
    /// Vertex Label Map:
    ///     A: 0, B: 1,
    /// Edge Label Map:
    ///     A-A: 0, A->B: 1, B-B: 2,
    fn build_pattern_case4() -> Pattern {
        let mut rng = rand::thread_rng();
        let pattern_vec = vec![
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 0,
                start_v_id: 0,
                end_v_id: 1,
                start_v_label: 0,
                end_v_label: 0,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 0,
                start_v_id: 1,
                end_v_id: 0,
                start_v_label: 0,
                end_v_label: 0,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 1,
                start_v_id: 0,
                end_v_id: 2,
                start_v_label: 0,
                end_v_label: 1,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 1,
                start_v_id: 1,
                end_v_id: 3,
                start_v_label: 0,
                end_v_label: 1,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 2,
                start_v_id: 2,
                end_v_id: 3,
                start_v_label: 1,
                end_v_label: 1,
            },
        ];
        Pattern::from(pattern_vec)
    }

    /// The pattern looks like
    ///         A(0) -> B(0)    A(1) <-> A(2)
    ///         |               |
    /// C(0) -> B(1) <- A(3) -> B(2) <- C(1) <- D(0)
    ///         |
    ///         C(2)
    /// where <-> means bidirectional edges
    /// Vertex Label Map
    ///     A: 3, B: 2, C: 1, D: 0
    /// Edge Label Map:
    ///     A-A: 20, A-B: 30, B-C: 15, C-D: 5
    fn build_pattern_case5() -> Pattern {
        let mut rng = rand::thread_rng();
        let label_a = 3;
        let label_b = 2;
        let label_c = 1;
        let label_d = 0;
        let id_vec_a: Vec<i32> = vec![100, 200, 300, 400];
        let id_vec_b: Vec<i32> = vec![10, 20, 30];
        let id_vec_c: Vec<i32> = vec![1, 2, 3];
        let id_vec_d: Vec<i32> = vec![1000];
        let pattern_vec = vec![
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 15,
                start_v_id: id_vec_c[0],
                end_v_id: id_vec_b[1],
                start_v_label: label_c,
                end_v_label: label_b,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 30,
                start_v_id: id_vec_a[0],
                end_v_id: id_vec_b[1],
                start_v_label: label_a,
                end_v_label: label_b,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 15,
                start_v_id: id_vec_c[2],
                end_v_id: id_vec_b[1],
                start_v_label: label_c,
                end_v_label: label_b,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 30,
                start_v_id: id_vec_a[0],
                end_v_id: id_vec_b[0],
                start_v_label: label_a,
                end_v_label: label_b,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 30,
                start_v_id: id_vec_a[3],
                end_v_id: id_vec_b[1],
                start_v_label: label_a,
                end_v_label: label_b,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 30,
                start_v_id: id_vec_a[3],
                end_v_id: id_vec_b[2],
                start_v_label: label_a,
                end_v_label: label_b,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 30,
                start_v_id: id_vec_a[1],
                end_v_id: id_vec_b[2],
                start_v_label: label_a,
                end_v_label: label_b,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 20,
                start_v_id: id_vec_a[1],
                end_v_id: id_vec_a[2],
                start_v_label: label_a,
                end_v_label: label_a,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 20,
                start_v_id: id_vec_a[2],
                end_v_id: id_vec_a[1],
                start_v_label: label_a,
                end_v_label: label_a,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 15,
                start_v_id: id_vec_c[1],
                end_v_id: id_vec_b[2],
                start_v_label: label_c,
                end_v_label: label_b,
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: 5,
                start_v_id: id_vec_d[0],
                end_v_id: id_vec_c[1],
                start_v_label: label_d,
                end_v_label: label_c,
            },
        ];
        Pattern::from(pattern_vec)
    }

    /// Test Cases for Index Ranking
    fn build_pattern_index_ranking_case1() -> (Pattern, HashMap<String, i32>) {
        let mut rng = rand::thread_rng();
        let mut vertex_label_map: HashMap<String, i32> = HashMap::new();
        let mut vertex_id_map: HashMap<String, i32> = HashMap::new();
        let mut edge_label_map: HashMap<String, i32> = HashMap::new();
        edge_label_map.insert(String::from("A->A"), rng.gen::<i32>());
        edge_label_map.insert(String::from("A->B"), rng.gen::<i32>());
        vertex_label_map.insert(String::from("A"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A0"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A1"), rng.gen::<i32>());
        let pattern_vec = vec![PatternEdge {
            id: rng.gen::<i32>(),
            label: *edge_label_map.get("A->A").unwrap(),
            start_v_id: *vertex_id_map.get("A0").unwrap(),
            end_v_id: *vertex_id_map.get("A1").unwrap(),
            start_v_label: *vertex_label_map.get("A").unwrap(),
            end_v_label: *vertex_label_map.get("A").unwrap(),
        }];
        (Pattern::from(pattern_vec), vertex_id_map)
    }

    fn build_pattern_index_ranking_case2() -> (Pattern, HashMap<String, i32>) {
        let mut rng = rand::thread_rng();
        let mut vertex_label_map: HashMap<String, i32> = HashMap::new();
        let mut vertex_id_map: HashMap<String, i32> = HashMap::new();
        let mut edge_label_map: HashMap<String, i32> = HashMap::new();
        edge_label_map.insert(String::from("A->A"), rng.gen::<i32>());
        edge_label_map.insert(String::from("A->B"), rng.gen::<i32>());
        vertex_label_map.insert(String::from("A"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A0"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A1"), rng.gen::<i32>());
        let pattern_vec = vec![
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->A").unwrap(),
                start_v_id: *vertex_id_map.get("A0").unwrap(),
                end_v_id: *vertex_id_map.get("A1").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("A").unwrap(),
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->A").unwrap(),
                start_v_id: *vertex_id_map.get("A1").unwrap(),
                end_v_id: *vertex_id_map.get("A0").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("A").unwrap(),
            },
        ];
        (Pattern::from(pattern_vec), vertex_id_map)
    }

    fn build_pattern_index_ranking_case3() -> (Pattern, HashMap<String, i32>) {
        let mut rng = rand::thread_rng();
        let mut vertex_label_map: HashMap<String, i32> = HashMap::new();
        let mut vertex_id_map: HashMap<String, i32> = HashMap::new();
        let mut edge_label_map: HashMap<String, i32> = HashMap::new();
        edge_label_map.insert(String::from("A->A"), rng.gen::<i32>());
        edge_label_map.insert(String::from("A->B"), rng.gen::<i32>());
        vertex_label_map.insert(String::from("A"), rng.gen::<i32>());
        vertex_label_map.insert(String::from("B"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A0"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A1"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("B0"), rng.gen::<i32>());
        let pattern_vec = vec![
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->A").unwrap(),
                start_v_id: *vertex_id_map.get("A0").unwrap(),
                end_v_id: *vertex_id_map.get("A1").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("A").unwrap(),
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->B").unwrap(),
                start_v_id: *vertex_id_map.get("A0").unwrap(),
                end_v_id: *vertex_id_map.get("B0").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("B").unwrap(),
            },
        ];
        (Pattern::from(pattern_vec), vertex_id_map)
    }

    fn build_pattern_index_ranking_case4() -> (Pattern, HashMap<String, i32>) {
        let mut rng = rand::thread_rng();
        let mut vertex_label_map: HashMap<String, i32> = HashMap::new();
        let mut vertex_id_map: HashMap<String, i32> = HashMap::new();
        let mut edge_label_map: HashMap<String, i32> = HashMap::new();
        edge_label_map.insert(String::from("A->A"), rng.gen::<i32>());
        edge_label_map.insert(String::from("A->B"), rng.gen::<i32>());
        vertex_label_map.insert(String::from("A"), rng.gen::<i32>());
        vertex_label_map.insert(String::from("B"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A0"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A1"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A2"), rng.gen::<i32>());
        let pattern_vec = vec![
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->A").unwrap(),
                start_v_id: *vertex_id_map.get("A0").unwrap(),
                end_v_id: *vertex_id_map.get("A1").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("A").unwrap(),
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->A").unwrap(),
                start_v_id: *vertex_id_map.get("A0").unwrap(),
                end_v_id: *vertex_id_map.get("A2").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("A").unwrap(),
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->A").unwrap(),
                start_v_id: *vertex_id_map.get("A2").unwrap(),
                end_v_id: *vertex_id_map.get("A0").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("A").unwrap(),
            },
        ];
        (Pattern::from(pattern_vec), vertex_id_map)
    }

    fn build_pattern_index_ranking_case5() -> (Pattern, HashMap<String, i32>) {
        let mut rng = rand::thread_rng();
        let mut vertex_label_map: HashMap<String, i32> = HashMap::new();
        let mut vertex_id_map: HashMap<String, i32> = HashMap::new();
        let mut edge_label_map: HashMap<String, i32> = HashMap::new();
        edge_label_map.insert(String::from("A->A"), rng.gen::<i32>());
        edge_label_map.insert(String::from("A->B"), rng.gen::<i32>());
        vertex_label_map.insert(String::from("A"), rng.gen::<i32>());
        vertex_label_map.insert(String::from("B"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A0"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A1"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A2"), rng.gen::<i32>());
        let pattern_vec = vec![
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->A").unwrap(),
                start_v_id: *vertex_id_map.get("A0").unwrap(),
                end_v_id: *vertex_id_map.get("A1").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("A").unwrap(),
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->A").unwrap(),
                start_v_id: *vertex_id_map.get("A1").unwrap(),
                end_v_id: *vertex_id_map.get("A2").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("A").unwrap(),
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->A").unwrap(),
                start_v_id: *vertex_id_map.get("A2").unwrap(),
                end_v_id: *vertex_id_map.get("A0").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("A").unwrap(),
            },
        ];
        (Pattern::from(pattern_vec), vertex_id_map)
    }

    fn build_pattern_index_ranking_case6() -> (Pattern, HashMap<String, i32>) {
        let mut rng = rand::thread_rng();
        let mut vertex_label_map: HashMap<String, i32> = HashMap::new();
        let mut vertex_id_map: HashMap<String, i32> = HashMap::new();
        let mut edge_label_map: HashMap<String, i32> = HashMap::new();
        edge_label_map.insert(String::from("A->A"), rng.gen::<i32>());
        edge_label_map.insert(String::from("A->B"), rng.gen::<i32>());
        edge_label_map.insert(String::from("B->A"), rng.gen::<i32>());
        vertex_label_map.insert(String::from("A"), rng.gen::<i32>());
        vertex_label_map.insert(String::from("B"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A0"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("A1"), rng.gen::<i32>());
        vertex_id_map.insert(String::from("B0"), rng.gen::<i32>());
        let pattern_vec = vec![
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->A").unwrap(),
                start_v_id: *vertex_id_map.get("A0").unwrap(),
                end_v_id: *vertex_id_map.get("A1").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("A").unwrap(),
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->A").unwrap(),
                start_v_id: *vertex_id_map.get("A1").unwrap(),
                end_v_id: *vertex_id_map.get("A0").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("A").unwrap(),
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("A->B").unwrap(),
                start_v_id: *vertex_id_map.get("A1").unwrap(),
                end_v_id: *vertex_id_map.get("B0").unwrap(),
                start_v_label: *vertex_label_map.get("A").unwrap(),
                end_v_label: *vertex_label_map.get("B").unwrap(),
            },
            PatternEdge {
                id: rng.gen::<i32>(),
                label: *edge_label_map.get("B->A").unwrap(),
                start_v_id: *vertex_id_map.get("B0").unwrap(),
                end_v_id: *vertex_id_map.get("A0").unwrap(),
                start_v_label: *vertex_label_map.get("B").unwrap(),
                end_v_label: *vertex_label_map.get("A").unwrap(),
            },
        ];
        (Pattern::from(pattern_vec), vertex_id_map)
    }

    /// The extend step looks like:
    ///         B
    ///       /   \
    ///      A     A
    /// The left A has label id 0 and index 0
    /// The right A also has label id 0 and index 0, the two A's are equivalent
    /// The target vertex is B with label id 1
    /// The two extend edges are both with edge id 1
    /// pattern_case1 + extend_step_case1 = pattern_case2
    fn build_extend_step_case1() -> ExtendStep {
        let extend_edge1 = ExtendEdge::new(0, 0, 1, Direction::Out);
        let extend_edge2 = extend_edge1.clone();
        ExtendStep::from((1, vec![extend_edge1, extend_edge2]))
    }

    fn get_modern_pattern_meta() -> PatternMeta {
        let modern_schema_file = match File::open("resource/modern_schema.json") {
            Ok(file) => file,
            Err(_) => File::open("catalogue/resource/modern_schema.json").unwrap(),
        };
        let modern_schema = Schema::from_json(modern_schema_file).unwrap();
        PatternMeta::from(modern_schema)
    }

    /// Pattern from modern schema file
    /// Person only Pattern
    fn build_modern_pattern_case1() -> Pattern {
        Pattern::from((0, 0))
    }

    /// Software only Pattern
    fn build_modern_pattern_case2() -> Pattern {
        Pattern::from((0, 1))
    }

    /// Person -> knows -> Person
    fn build_modern_pattern_case3() -> Pattern {
        let pattern_edge =
            PatternEdge { id: 0, label: 0, start_v_id: 0, end_v_id: 1, start_v_label: 0, end_v_label: 0 };
        let mut pattern = Pattern::from(vec![pattern_edge]);
        pattern.vertices.get_mut(&1).unwrap().index = 1;
        pattern
    }

    /// Person -> created -> Software
    fn build_modern_pattern_case4() -> Pattern {
        let pattern_edge =
            PatternEdge { id: 0, label: 1, start_v_id: 0, end_v_id: 1, start_v_label: 0, end_v_label: 1 };
        Pattern::from(vec![pattern_edge])
    }

    fn get_ldbc_pattern_meta() -> PatternMeta {
        let ldbc_schema_file = match File::open("resource/ldbc_schema.json") {
            Ok(file) => file,
            Err(_) => File::open("catalogue/resource/ldbc_schema.json").unwrap(),
        };
        let ldbc_schema = Schema::from_json(ldbc_schema_file).unwrap();
        PatternMeta::from(ldbc_schema)
    }

    /// Pattern from ldbc schema file
    /// Person -> knows -> Person
    fn build_ldbc_pattern_case1() -> Pattern {
        let pattern_edge =
            PatternEdge { id: 0, label: 12, start_v_id: 0, end_v_id: 1, start_v_label: 1, end_v_label: 1 };
        let mut pattern = Pattern::from(vec![pattern_edge]);
        pattern.vertices.get_mut(&1).unwrap().index = 1;
        pattern
    }

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
        let edge_0 = pattern_case1.edges.get(&0).unwrap();
        assert_eq!(edge_0.id, 0);
        assert_eq!(edge_0.label, 0);
        assert_eq!(edge_0.start_v_id, 0);
        assert_eq!(edge_0.end_v_id, 1);
        assert_eq!(edge_0.start_v_label, 0);
        assert_eq!(edge_0.end_v_label, 0);
        let edge_1 = pattern_case1.edges.get(&1).unwrap();
        assert_eq!(edge_1.id, 1);
        assert_eq!(edge_1.label, 0);
        assert_eq!(edge_1.start_v_id, 1);
        assert_eq!(edge_1.end_v_id, 0);
        assert_eq!(edge_1.start_v_label, 0);
        assert_eq!(edge_1.end_v_label, 0);
        let vertex_0 = pattern_case1.vertices.get(&0).unwrap();
        assert_eq!(vertex_0.id, 0);
        assert_eq!(vertex_0.label, 0);
        assert_eq!(vertex_0.connect_edges.len(), 2);
        let mut vertex_0_connect_edges_iter = vertex_0.connect_edges.iter();
        let (v0_e0, (v0_v0, v0_d0)) = vertex_0_connect_edges_iter.next().unwrap();
        assert_eq!(*v0_e0, 0);
        assert_eq!(*v0_v0, 1);
        assert_eq!(*v0_d0, Direction::Out);
        let (v0_e1, (v0_v1, v0_d1)) = vertex_0_connect_edges_iter.next().unwrap();
        assert_eq!(*v0_e1, 1);
        assert_eq!(*v0_v1, 1);
        assert_eq!(*v0_d1, Direction::In);
        assert_eq!(vertex_0.connect_vertices.len(), 1);
        let v0_v1_connected_edges = vertex_0.connect_vertices.get(&1).unwrap();
        assert_eq!(v0_v1_connected_edges.len(), 2);
        let mut v0_v1_connected_edges_iter = v0_v1_connected_edges.iter();
        assert_eq!(*v0_v1_connected_edges_iter.next().unwrap(), (0, Direction::Out));
        assert_eq!(*v0_v1_connected_edges_iter.next().unwrap(), (1, Direction::In));
        let vertex_1 = pattern_case1.vertices.get(&1).unwrap();
        assert_eq!(vertex_1.id, 1);
        assert_eq!(vertex_1.label, 0);
        assert_eq!(vertex_1.connect_edges.len(), 2);
        let mut vertex_1_connect_edges_iter = vertex_1.connect_edges.iter();
        let (v1_e0, (v1_v0, v1_d0)) = vertex_1_connect_edges_iter.next().unwrap();
        assert_eq!(*v1_e0, 0);
        assert_eq!(*v1_v0, 0);
        assert_eq!(*v1_d0, Direction::In);
        let (v1_e1, (v1_v1, v1_d1)) = vertex_1_connect_edges_iter.next().unwrap();
        assert_eq!(*v1_e1, 1);
        assert_eq!(*v1_v1, 0);
        assert_eq!(*v1_d1, Direction::Out);
        assert_eq!(vertex_1.connect_vertices.len(), 1);
        let v1_v0_connected_edges = vertex_1.connect_vertices.get(&0).unwrap();
        assert_eq!(v1_v0_connected_edges.len(), 2);
        let mut v1_v0_connected_edges_iter = v1_v0_connected_edges.iter();
        assert_eq!(*v1_v0_connected_edges_iter.next().unwrap(), (0, Direction::In));
        assert_eq!(*v1_v0_connected_edges_iter.next().unwrap(), (1, Direction::Out));
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
        for i in 0..pattern_after_extend.edges.len() as ID {
            let edge1 = pattern_after_extend.edges.get(&i).unwrap();
            let edge2 = pattern_case2.edges.get(&i).unwrap();
            assert_eq!(edge1.id, edge2.id);
            assert_eq!(edge1.label, edge2.label);
            assert_eq!(edge1.start_v_id, edge2.start_v_id);
            assert_eq!(edge1.start_v_label, edge2.start_v_label);
            assert_eq!(edge1.end_v_id, edge2.end_v_id);
            assert_eq!(edge1.end_v_label, edge2.end_v_label);
        }
        assert_eq!(pattern_after_extend.edges.len(), pattern_case2.edges.len());
        for i in 0..pattern_after_extend.vertices.len() as ID {
            let vertex1 = pattern_after_extend.vertices.get(&i).unwrap();
            let vertex2 = pattern_after_extend.vertices.get(&i).unwrap();
            assert_eq!(vertex1.id, vertex2.id);
            assert_eq!(vertex1.label, vertex2.label);
            assert_eq!(vertex1.index, vertex2.index);
            assert_eq!(vertex1.in_degree, vertex2.in_degree);
            assert_eq!(vertex1.out_degree, vertex2.out_degree);
            assert_eq!(vertex1.connect_edges.len(), vertex2.connect_edges.len());
            assert_eq!(vertex1.connect_vertices.len(), vertex2.connect_vertices.len());
            for (connect_edge1_id, (connect_vertex1_id, dir1)) in &vertex1.connect_edges {
                let (connect_vertex2_id, dir2) = vertex2
                    .connect_edges
                    .get(connect_edge1_id)
                    .unwrap();
                assert_eq!(*connect_vertex1_id, *connect_vertex2_id);
                assert_eq!(*dir1, *dir2);
            }
            for (connect_vertex1_id, edge_connections1) in &vertex1.connect_vertices {
                let edge_connections2 = vertex2
                    .connect_vertices
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
            assert_eq!(extend_edge.get_start_vertex_index(), 0);
            if extend_step.get_target_v_label() == 0 {
                if extend_edge.get_direction() == Direction::Out {
                    out_0_0_0 += 1;
                }
                if extend_edge.get_direction() == Direction::In {
                    incoming_0_0_0 += 1;
                }
            }
            if extend_step.get_target_v_label() == 1 && extend_edge.get_direction() == Direction::Out {
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
        assert_eq!(extend_edge.get_start_vertex_index(), 0);
        assert_eq!(extend_edge.get_edge_label(), 1);
        assert_eq!(extend_edge.get_direction(), Direction::In);
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
                            assert_eq!(extend_edge.get_start_vertex_index(), 0);
                            if extend_edge.get_direction() == Direction::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1
                            }
                            if extend_edge.get_direction() == Direction::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_0_1_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::Out
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
                            assert_eq!(extend_edge.get_start_vertex_index(), 1);
                            if extend_edge.get_direction() == Direction::In
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_1_0_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_1_0_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_0_1_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::Out
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
                            assert_eq!(extend_edge.get_start_vertex_index(), 0);
                            if extend_edge.get_direction() == Direction::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                found_out_0_0_0 = true;
                            } else if extend_edge.get_direction() == Direction::In
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
                            assert_eq!(extend_edge.get_start_vertex_index(), 1);
                            if extend_edge.get_direction() == Direction::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                found_out_0_1_0 = true;
                            } else if extend_edge.get_direction() == Direction::In
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
                            assert_eq!(extend_edge.get_start_vertex_index(), 0);
                            if extend_edge.get_direction() == Direction::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_0_1_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::In
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
                            assert_eq!(extend_edge.get_start_vertex_index(), 1);
                            if extend_edge.get_direction() == Direction::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_1_1_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::In
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
                            assert_eq!(extend_edge.get_start_vertex_index(), 0);
                            if extend_edge.get_direction() == Direction::Out
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
                            assert_eq!(extend_edge.get_start_vertex_index(), 1);
                            if extend_edge.get_direction() == Direction::Out
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
                            assert_eq!(extend_edge.get_start_vertex_index(), 0);
                            if extend_edge.get_direction() == Direction::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::In
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
                            assert_eq!(extend_edge.get_start_vertex_index(), 0);
                            if extend_edge.get_direction() == Direction::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == Direction::In
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
                            assert_eq!(extend_edge.get_start_vertex_index(), 0);
                            if extend_edge.get_direction() == Direction::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                found_out_0_0_0 = true;
                            } else if extend_edge.get_direction() == Direction::In
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
                            assert_eq!(extend_edge.get_start_vertex_index(), 0);
                            if extend_edge.get_direction() == Direction::In
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
    fn set_initial_index_case1() {
        let mut pattern = build_pattern_case1();
        pattern.set_initial_index();
        // Expected map from vertex id to initial index
        let mut expected_map = BTreeMap::new();
        expected_map.insert(0, 0);
        expected_map.insert(1, 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn set_initial_index_case2() {
        let mut pattern = build_pattern_case2();
        pattern.set_initial_index();
        // Expected map from vertex id to initial index
        let mut expected_map = BTreeMap::new();
        expected_map.insert(0, 0);
        expected_map.insert(1, 0);
        expected_map.insert(2, 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn set_initial_index_case3() {
        let mut pattern = build_pattern_case3();
        pattern.set_initial_index();
        // Expected map from vertex id to initial index
        let mut expected_map = BTreeMap::new();
        expected_map.insert(0, 1);
        expected_map.insert(1, 0);
        expected_map.insert(2, 1);
        expected_map.insert(3, 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn set_initial_index_case4() {
        let mut pattern = build_pattern_case4();
        pattern.set_initial_index();
        // Expected map from vertex id to initial index
        let mut expected_map = BTreeMap::new();
        expected_map.insert(0, 0);
        expected_map.insert(1, 0);
        expected_map.insert(2, 1);
        expected_map.insert(3, 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn set_initial_index_case5() {
        let mut pattern = build_pattern_case5();
        pattern.set_initial_index();
        // Expected map from vertex id to initial index
        let mut expected_map = BTreeMap::new();
        let id_vec_a: Vec<i32> = vec![100, 200, 300, 400];
        let id_vec_b: Vec<i32> = vec![10, 20, 30];
        let id_vec_c: Vec<i32> = vec![1, 2, 3];
        let id_vec_d: Vec<i32> = vec![1000];
        // A
        expected_map.insert(id_vec_a[0], 1);
        expected_map.insert(id_vec_a[1], 3);
        expected_map.insert(id_vec_a[2], 0);
        expected_map.insert(id_vec_a[3], 1);
        // B
        expected_map.insert(id_vec_b[0], 0);
        expected_map.insert(id_vec_b[1], 2);
        expected_map.insert(id_vec_b[2], 1);
        // C
        expected_map.insert(id_vec_c[0], 0);
        expected_map.insert(id_vec_c[1], 2);
        expected_map.insert(id_vec_c[2], 0);
        // D
        expected_map.insert(id_vec_d[0], 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn set_accurate_index_case1() {
        let mut pattern = build_pattern_case1();
        pattern.index_ranking();
        // Expected map from vertex id to initial index
        let mut expected_map = BTreeMap::new();
        expected_map.insert(0, 0);
        expected_map.insert(1, 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn set_accurate_index_case2() {
        let mut pattern = build_pattern_case2();
        pattern.index_ranking();
        // Expected map from vertex id to initial index
        let mut expected_map = BTreeMap::new();
        expected_map.insert(0, 0);
        expected_map.insert(1, 0);
        expected_map.insert(2, 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn set_accurate_index_case3() {
        let mut pattern = build_pattern_case3();
        pattern.index_ranking();
        // Expected map from vertex id to initial index
        let mut expected_map = BTreeMap::new();
        expected_map.insert(0, 1);
        expected_map.insert(1, 0);
        expected_map.insert(2, 1);
        expected_map.insert(3, 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    /// The pattern looks like:
    ///     B(2)  -> B(3)
    ///     |        |
    ///     A(0) <-> A(1)
    #[test]
    fn set_accurate_index_case4() {
        let mut pattern = build_pattern_case4();
        pattern.index_ranking();
        // Expected map from vertex id to initial index
        let mut expected_map = BTreeMap::new();
        expected_map.insert(0, 1);
        expected_map.insert(1, 0);
        expected_map.insert(2, 1);
        expected_map.insert(3, 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    /// The pattern looks like
    ///         A(0) -> B(0)    A(1) <-> A(2)
    ///         |               |
    /// C(0) -> B(1) <- A(3) -> B(2) <- C(1) <- D(0)
    ///         |
    ///         C(2)
    #[test]
    fn set_accurate_index_case5() {
        let mut pattern = build_pattern_case5();
        pattern.index_ranking();
        // Expected map from vertex id to initial index
        let mut expected_map = BTreeMap::new();
        let id_vec_a: Vec<i32> = vec![100, 200, 300, 400];
        let id_vec_b: Vec<i32> = vec![10, 20, 30];
        let id_vec_c: Vec<i32> = vec![1, 2, 3];
        let id_vec_d: Vec<i32> = vec![1000];
        // A
        expected_map.insert(id_vec_a[0], 1);
        expected_map.insert(id_vec_a[1], 3);
        expected_map.insert(id_vec_a[2], 0);
        expected_map.insert(id_vec_a[3], 2);
        // B
        expected_map.insert(id_vec_b[0], 0);
        expected_map.insert(id_vec_b[1], 2);
        expected_map.insert(id_vec_b[2], 1);
        // C
        expected_map.insert(id_vec_c[0], 0);
        expected_map.insert(id_vec_c[1], 2);
        expected_map.insert(id_vec_c[2], 0);
        // D
        expected_map.insert(id_vec_d[0], 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn index_ranking_case1() {
        let (mut pattern, vertex_id_map) = build_pattern_index_ranking_case1();
        pattern.index_ranking();
        let mut expected_index_map = BTreeMap::new();
        expected_index_map.insert(vertex_id_map.get("A0").unwrap(), 1);
        expected_index_map.insert(vertex_id_map.get("A1").unwrap(), 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_index_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn index_ranking_case2() {
        let (mut pattern, vertex_id_map) = build_pattern_index_ranking_case2();
        pattern.index_ranking();
        let mut expected_index_map = BTreeMap::new();
        expected_index_map.insert(vertex_id_map.get("A0").unwrap(), 0);
        expected_index_map.insert(vertex_id_map.get("A1").unwrap(), 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_index_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn index_ranking_case3() {
        let (mut pattern, vertex_id_map) = build_pattern_index_ranking_case3();
        pattern.index_ranking();
        let mut expected_index_map = BTreeMap::new();
        expected_index_map.insert(vertex_id_map.get("A0").unwrap(), 1);
        expected_index_map.insert(vertex_id_map.get("A1").unwrap(), 0);
        expected_index_map.insert(vertex_id_map.get("B0").unwrap(), 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_index_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn index_ranking_case4() {
        let (mut pattern, vertex_id_map) = build_pattern_index_ranking_case4();
        pattern.index_ranking();
        let mut expected_index_map = BTreeMap::new();
        expected_index_map.insert(vertex_id_map.get("A0").unwrap(), 2);
        expected_index_map.insert(vertex_id_map.get("A1").unwrap(), 0);
        expected_index_map.insert(vertex_id_map.get("A2").unwrap(), 1);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_index_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn index_ranking_case5() {
        let (mut pattern, vertex_id_map) = build_pattern_index_ranking_case5();
        pattern.index_ranking();
        let mut expected_index_map = BTreeMap::new();
        expected_index_map.insert(vertex_id_map.get("A0").unwrap(), 0);
        expected_index_map.insert(vertex_id_map.get("A1").unwrap(), 0);
        expected_index_map.insert(vertex_id_map.get("A2").unwrap(), 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_index_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn index_ranking_case6() {
        let (mut pattern, vertex_id_map) = build_pattern_index_ranking_case6();
        pattern.index_ranking();
        let mut expected_index_map = BTreeMap::new();
        expected_index_map.insert(vertex_id_map.get("A0").unwrap(), 0);
        expected_index_map.insert(vertex_id_map.get("A1").unwrap(), 1);
        expected_index_map.insert(vertex_id_map.get("B0").unwrap(), 0);
        // Verify Initial Indices
        let vertices = pattern.get_vertices();
        for (v_id, expected_index) in expected_index_map {
            assert_eq!(vertices.get(&v_id).unwrap().get_index(), expected_index);
        }
    }

    #[test]
    fn test_encode_decode_of_case1() {
        let mut pattern = build_pattern_case1();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        pattern.index_ranking();
        let code1: Vec<u8> = pattern.encode_to(&encoder);
        let mut pattern: Pattern = Pattern::decode_from(code1.clone(), &encoder);
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        pattern.index_ranking();
        let code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case2() {
        let mut pattern = build_pattern_case2();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        pattern.index_ranking();
        let code1: Vec<u8> = pattern.encode_to(&encoder);
        let mut pattern: Pattern = Pattern::decode_from(code1.clone(), &encoder);
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        pattern.index_ranking();
        let code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case3() {
        let mut pattern = build_pattern_case3();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        pattern.index_ranking();
        let code1: Vec<u8> = pattern.encode_to(&encoder);
        let mut pattern = build_pattern_case3();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        pattern.index_ranking();
        let code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case4() {
        let mut pattern = build_pattern_case4();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        pattern.index_ranking();
        let code1: Vec<u8> = pattern.encode_to(&encoder);
        let mut pattern = build_pattern_case4();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        pattern.index_ranking();
        let code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case5() {
        let mut pattern = build_pattern_case5();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        pattern.index_ranking();
        let code1: Vec<u8> = pattern.encode_to(&encoder);
        let mut pattern = build_pattern_case5();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        pattern.index_ranking();
        let code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }
}
