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
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::catalogue::pattern::*;
use crate::catalogue::{DynIter, PatternId, PatternLabelId};

#[derive(Debug, Clone)]
struct VertexGroupManager {
    vertex_group_map: BTreeMap<PatternId, usize>,
    vertex_groups: BTreeMap<(PatternLabelId, usize), Vec<PatternId>>,
    has_converged: bool,
}

impl VertexGroupManager {
    pub fn init_by_pattern(pattern: &Pattern) -> Self {
        let mut vertex_group_map: BTreeMap<PatternId, usize> = BTreeMap::new();
        let mut vertex_groups: BTreeMap<(PatternLabelId, usize), Vec<PatternId>> = BTreeMap::new();
        pattern.vertices_iter().for_each(|vertex| {
            let (v_id, v_label) = (vertex.get_id(), vertex.get_label());
            vertex_group_map.insert(v_id, 0);
            vertex_groups
                .entry((v_label, 0))
                .and_modify(|vertices| vertices.push(v_id))
                .or_insert(vec![v_id]);
        });
        VertexGroupManager { vertex_group_map, vertex_groups, has_converged: false }
    }

    /// Set Initial Vertex Index by comparing the labels and In/Out Degrees
    ///
    /// Return a bool indicating whether the ranks has changed for this initial rank iteration
    ///
    /// If nothing changed, we conclude that the ranks of all vertices have been set and are stable
    fn refine_vertex_groups(
        &mut self, pattern: &mut Pattern, vertex_adjacencies_map: &mut BTreeMap<PatternId, Vec<Adjacency>>,
    ) {
        // The updated version of vertex group map and vertex groups.
        // The updated data are temporarily stored here and finally moved to the VertexGroupManager.
        let mut updated_vertex_group_map: BTreeMap<PatternId, usize> = BTreeMap::new();
        let mut updated_vertex_groups: BTreeMap<(PatternLabelId, usize), Vec<PatternId>> = BTreeMap::new();
        let mut has_converged = true;
        for ((v_label, initial_rank), vertex_group) in self.vertex_groups.iter() {
            // Temporarily record the group for each vertex
            let mut vertex_group_tmp_vec: Vec<usize> = vec![*initial_rank; vertex_group.len()];
            // To find out the exact group of a vertex, compare it with all vertices with the same label
            for i in 0..vertex_group.len() {
                let current_v_id: PatternId = vertex_group[i];
                for j in (i + 1)..vertex_group.len() {
                    match self.cmp_vertices(pattern, current_v_id, vertex_group[j]) {
                        Ordering::Greater => vertex_group_tmp_vec[i] += 1,
                        Ordering::Less => vertex_group_tmp_vec[j] += 1,
                        Ordering::Equal => (),
                    }
                }

                let v_rank: usize = vertex_group_tmp_vec[i];
                if v_rank != *initial_rank {
                    has_converged = false;
                }

                updated_vertex_group_map.insert(current_v_id, v_rank);
                updated_vertex_groups
                    .entry((*v_label, v_rank))
                    .and_modify(|vertex_group| vertex_group.push(current_v_id))
                    .or_insert(vec![current_v_id]);
            }
        }

        // Update vertex group manager
        self.vertex_group_map = updated_vertex_group_map;
        self.vertex_groups = updated_vertex_groups;
        self.has_converged = has_converged;

        // Update the order of vertex adjacencies
        vertex_adjacencies_map
            .values_mut()
            .for_each(|adjacencies| {
                adjacencies.sort_by(|adj1, adj2| {
                    match pattern.cmp_adjacencies(adj1, adj2) {
                        Ordering::Less => return Ordering::Less,
                        Ordering::Greater => return Ordering::Greater,
                        Ordering::Equal => (),
                    }

                    // Compare the vertex groups
                    let adj1_v_group = self
                        .get_vertex_group(adj1.get_adj_vertex().get_id())
                        .unwrap();
                    let adj2_v_group = self
                        .get_vertex_group(adj2.get_adj_vertex().get_id())
                        .unwrap();
                    adj1_v_group.cmp(&adj2_v_group)
                });
            });
    }

    /// Compare the ranks of two PatternVertices
    ///
    /// Consider labels and out/in degrees only
    ///
    /// Called when setting initial ranks
    fn cmp_vertices(&self, pattern: &Pattern, v1_id: PatternId, v2_id: PatternId) -> Ordering {
        // Compare Label
        let v1_label = pattern.get_vertex(v1_id).unwrap().get_label();
        let v2_label = pattern.get_vertex(v2_id).unwrap().get_label();
        match v1_label.cmp(&v2_label) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }

        // Compare Out Degree
        let v1_out_degree = pattern.get_vertex_out_degree(v1_id);
        let v2_out_degree = pattern.get_vertex_out_degree(v2_id);
        match v1_out_degree.cmp(&v2_out_degree) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }

        // Compare In Degree
        let v1_in_degree = pattern.get_vertex_in_degree(v1_id);
        let v2_in_degree = pattern.get_vertex_in_degree(v2_id);
        match v1_in_degree.cmp(&v2_in_degree) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => (),
        }

        // Compare Adjacencies
        let mut v1_adjacencies_iter = pattern.adjacencies_iter(v1_id);
        let mut v2_adjacencies_iter = pattern.adjacencies_iter(v2_id);
        loop {
            let v1_adjacency = if let Some(value) = v1_adjacencies_iter.next() {
                value
            } else {
                break;
            };
            let v2_adjacency = if let Some(value) = v2_adjacencies_iter.next() {
                value
            } else {
                break;
            };
            // Compare direction and labels
            match pattern.cmp_adjacencies(v1_adjacency, v2_adjacency) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                Ordering::Equal => (),
            }

            // Compare the vertex groups
            let adj1_v_group = self
                .get_vertex_group(v1_adjacency.get_adj_vertex().get_id())
                .unwrap();
            let adj2_v_group = self
                .get_vertex_group(v2_adjacency.get_adj_vertex().get_id())
                .unwrap();
            match adj1_v_group.cmp(&adj2_v_group) {
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
                Ordering::Equal => (),
            }
        }

        // Return Equal if Still Cannot Distinguish
        Ordering::Equal
    }

    pub fn get_vertex_group(&self, vertex_id: PatternId) -> Option<&usize> {
        self.vertex_group_map.get(&vertex_id)
    }

    pub fn has_converged(&self) -> bool {
        self.has_converged
    }
}

#[derive(Debug, Clone)]
struct VertexRankManager {
    edge_rank_map: BTreeMap<PatternId, Option<usize>>,
    vertex_rank_map: BTreeMap<PatternId, Option<usize>>,
}

impl VertexRankManager {
    pub fn init_by_pattern(pattern: &Pattern) -> Self {
        let edge_rank_map: BTreeMap<PatternId, Option<usize>> = BTreeMap::new();
        let mut vertex_rank_map: BTreeMap<PatternId, Option<usize>> = BTreeMap::new();
        pattern.vertices_iter().for_each(|vertex| {
            vertex_rank_map.insert(vertex.get_id(), None);
        });
        VertexRankManager { edge_rank_map, vertex_rank_map }
    }

    pub fn get_vertex_rank(&self, vertex_id: PatternId) -> Option<usize> {
        *self.vertex_rank_map.get(&vertex_id).unwrap()
    }

    pub fn get_edge_rank(&self, edge_id: PatternId) -> Option<usize> {
        *self.edge_rank_map.get(&edge_id).unwrap()
    }

    /// Perform DFS Sorting to Pattern Edges Based on Labels and Ranks
    ///
    /// Return a tuple of 2 elements
    ///
    /// dfs_edge_sequence is a vector of edge ids in DFS order
    ///
    /// vertex_dfs_id_map maps from vertex ids to dfs id
    fn rank_vertices_and_edges(
        &mut self, pattern: &mut Pattern, start_v_id: PatternId,
        vertex_adjacencies_map: &mut BTreeMap<PatternId, Vec<Adjacency>>,
    ) {
        self.vertex_rank_map.insert(start_v_id, Some(0));
        let mut next_free_vertex_rank: usize = 1;
        let mut next_free_edge_rank: usize = 0;
        // Record which edges have been visited
        let mut visited_edges: BTreeSet<PatternId> = BTreeSet::new();
        // Initialize Stack for adjacencies
        let mut adjacency_stack: VecDeque<Adjacency> =
            self.init_adjacencies_stack(start_v_id, vertex_adjacencies_map);

        // Perform DFS on adjacencies
        while let Some(adjacency) = adjacency_stack.pop_back() {
            // Insert edge to dfs sequence if it has not been visited
            let adj_edge_id: PatternId = adjacency.get_edge_id();
            if visited_edges.contains(&adj_edge_id) {
                continue;
            }
            visited_edges.insert(adj_edge_id);
            self.edge_rank_map
                .insert(adj_edge_id, Some(next_free_edge_rank));
            next_free_edge_rank += 1;

            // Set dfs id to the vertex if it has not been set before
            let current_v_id: PatternId = adjacency.get_adj_vertex().get_id();
            if !self.vertex_rank_map.contains_key(&current_v_id) {
                self.vertex_rank_map
                    .insert(current_v_id, Some(next_free_vertex_rank));
                next_free_vertex_rank += 1;
            }

            // Push adjacencies of the current vertex into the stack
            self.extend_adjacencies_iter(current_v_id, pattern, vertex_adjacencies_map)
                .filter(|adj| !visited_edges.contains(&adj.get_edge_id()))
                .for_each(|adj| adjacency_stack.push_back(adj));
        }
    }

    fn init_adjacencies_stack(
        &self, start_v_id: PatternId, vertex_adjacencies_map: &BTreeMap<PatternId, Vec<Adjacency>>,
    ) -> VecDeque<Adjacency> {
        let mut adjacency_stack: VecDeque<Adjacency> = VecDeque::new();
        vertex_adjacencies_map
            .get(&start_v_id)
            .unwrap()
            .iter()
            .rev()
            .for_each(|adjacency| adjacency_stack.push_back(*adjacency));
        adjacency_stack
    }

    fn extend_adjacencies_iter(
        &self, current_v_id: PatternId, pattern: &Pattern,
        vertex_adjacencies_map: &mut BTreeMap<PatternId, Vec<Adjacency>>,
    ) -> DynIter<Adjacency> {
        let extend_adjacencies = vertex_adjacencies_map
            .get_mut(&current_v_id)
            .unwrap();
        extend_adjacencies.sort_by(|adj1, adj2| {
            match pattern.cmp_adjacencies(adj1, adj2) {
                Ordering::Less => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
                Ordering::Equal => {
                    // Adjacency will be given high priority if its adjacent vertex has no or smaller dfs id
                    let adj1_v_dfs_id =
                        if let Some(value) = self.get_vertex_rank(adj1.get_adj_vertex().get_id()) {
                            value
                        } else {
                            return Ordering::Less;
                        };

                    let adj2_v_dfs_id =
                        if let Some(value) = self.get_vertex_rank(adj2.get_adj_vertex().get_id()) {
                            value
                        } else {
                            return Ordering::Greater;
                        };

                    adj1_v_dfs_id.cmp(&adj2_v_dfs_id)
                }
            }
        });

        Box::new(extend_adjacencies.clone().into_iter().rev())
    }
}

#[derive(Debug, Clone)]
pub struct CanonicalLabelManager {
    vertex_adjacencies_map: BTreeMap<PatternId, Vec<Adjacency>>,
    vertex_group_manager: VertexGroupManager,
    rank_manager: VertexRankManager,
}

impl CanonicalLabelManager {
    pub fn init_by_pattern(pattern: &Pattern) -> Self {
        let mut vertex_adjacencies_map: BTreeMap<PatternId, Vec<Adjacency>> = BTreeMap::new();
        pattern
            .vertices_iter()
            .map(|vertex| vertex.get_id())
            .for_each(|v_id| {
                let vertex_adjacencies: Vec<Adjacency> = pattern
                    .adjacencies_iter(v_id)
                    .cloned()
                    .collect();
                vertex_adjacencies_map.insert(v_id, vertex_adjacencies);
            });
        CanonicalLabelManager {
            vertex_adjacencies_map,
            vertex_group_manager: VertexGroupManager::init_by_pattern(pattern),
            rank_manager: VertexRankManager::init_by_pattern(pattern),
        }
    }

    pub fn vertices_iter(&self) -> DynIter<PatternId> {
        Box::new(
            self.rank_manager
                .vertex_rank_map
                .iter()
                .map(|(v_id, _order)| *v_id),
        )
    }

    /// Vertex Grouping
    pub fn refine_vertex_groups(&mut self, pattern: &mut Pattern) {
        self.vertex_group_manager
            .refine_vertex_groups(pattern, &mut self.vertex_adjacencies_map);
    }

    pub fn has_vertex_groups_converged(&self) -> bool {
        self.vertex_group_manager.has_converged()
    }

    pub fn vertex_groups_iter(&self) -> DynIter<(&PatternId, &usize)> {
        Box::new(
            self.vertex_group_manager
                .vertex_group_map
                .iter(),
        )
    }

    pub fn get_vertex_group(&self, vertex_id: PatternId) -> Option<&usize> {
        self.vertex_group_manager
            .get_vertex_group(vertex_id)
    }

    /// Ranking Vertices and Edges
    pub fn vertex_ranks_iter(&self) -> DynIter<(&PatternId, &Option<usize>)> {
        Box::new(self.rank_manager.vertex_rank_map.iter())
    }

    pub fn edge_ranks_iter(&self) -> DynIter<(&PatternId, &Option<usize>)> {
        Box::new(self.rank_manager.edge_rank_map.iter())
    }

    pub fn get_vertex_rank(&self, vertex_id: PatternId) -> Option<usize> {
        self.rank_manager.get_vertex_rank(vertex_id)
    }

    pub fn get_edge_rank(&self, edge_id: PatternId) -> Option<usize> {
        self.rank_manager.get_edge_rank(edge_id)
    }

    pub fn rank_vertices_and_edges(&mut self, pattern: &mut Pattern, start_v_id: PatternId) {
        self.rank_manager
            .rank_vertices_and_edges(pattern, start_v_id, &mut self.vertex_adjacencies_map);
    }
}
