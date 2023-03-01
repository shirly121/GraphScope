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

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::convert::{TryFrom, TryInto};

use ir_common::generated::common::{self as common_pb, NameOrId, Variable};

use crate::catalogue::pattern::{Pattern, PatternEdge, PatternVertex};
use crate::catalogue::{PatternDirection, PatternId, PatternLabelId};
use crate::error::IrResult;

#[derive(Debug)]
pub struct JoinStep {
    probe_pattern: Pattern,
    shared_v_rank_map: BTreeMap<PatternId, PatternId>,
}

impl From<JoinDecomposition> for JoinStep {
    fn from(join_plan: JoinDecomposition) -> Self {
        let shared_v_rank_map = join_plan.get_shared_vertices_rank_map();
        let probe_pattern = join_plan.probe_pattern;
        JoinStep { probe_pattern, shared_v_rank_map }
    }
}

#[derive(Debug)]
pub struct DefiniteJoinStep {
    probe_pattern: Pattern,
    shared_vertices: BTreeSet<PatternId>,
}

impl From<JoinDecomposition> for DefiniteJoinStep {
    fn from(binary_join_plan: JoinDecomposition) -> Self {
        DefiniteJoinStep {
            probe_pattern: binary_join_plan.probe_pattern,
            shared_vertices: binary_join_plan.shared_vertices,
        }
    }
}

impl DefiniteJoinStep {
    pub fn from_target_pattern(
        target_pattern: &Pattern, join_step: &JoinStep, v_rank_map: &BTreeMap<PatternId, PatternId>,
        e_rank_map: &BTreeMap<PatternId, PatternId>,
    ) -> Self {
        let mut new_probe_pattern_edges = vec![];
        for probe_edge in join_step.probe_pattern.edges_iter() {
            let probe_edge_rank = join_step
                .probe_pattern
                .get_edge_rank(probe_edge.get_id())
                .expect("Fail to get edge rank with ID");
            let target_edge_rank = *e_rank_map
                .get(&probe_edge_rank)
                .expect("Fail to map probe edge to target edge");
            let target_edge = target_pattern
                .get_edge_from_rank(target_edge_rank)
                .expect("Fail to get edge with rank")
                .clone();
            new_probe_pattern_edges.push(target_edge);
        }
        let new_probe_pattern =
            Pattern::try_from(new_probe_pattern_edges).expect("Build pattern from edges fails");
        let mut shared_vertices = BTreeSet::new();
        // println!("v_rank_map: {:?}", v_rank_map);
        // println!("probe pattern: {:?}", &join_step.probe_pattern);
        // println!("shared v rank map: {:?}", &join_step.shared_v_rank_map);
        for (&probe_shared_v_rank, _) in join_step.shared_v_rank_map.iter() {
            let target_v_rank = *v_rank_map
                .get(&probe_shared_v_rank)
                .expect("Fail to map probe vertex to target vertex");
            let target_vertex = target_pattern
                .get_vertex_from_rank(target_v_rank)
                .expect("Fail to get verte with rank");
            shared_vertices.insert(target_vertex.get_id());
        }
        DefiniteJoinStep { probe_pattern: new_probe_pattern, shared_vertices }
    }

    pub fn get_probe_pattern(&self) -> &Pattern {
        &self.probe_pattern
    }

    pub fn get_join_keys(&self) -> Vec<Variable> {
        self.shared_vertices
            .iter()
            .map(|v_id| {
                let tag: NameOrId = (*v_id as i32).into();
                common_pb::Variable { tag: tag.try_into().ok(), property: None }
            })
            .collect()
    }
}

/// Similar to ExtendStep, BinaryJoinStep always comes hand in hand with a pattern instance.
///
/// ## Usage:
/// Build Pattern A + BinaryJoinStep (With Probe Pattern Inside) = Target Pattern B
///
/// ## Notes:
/// - The names 'build' and 'probe' comes from the two phases of hash join.
///
/// - Build pattern A always has larger size than probe pattern B.
/// The reason is that when we find the BinaryJoinStep during dynamic programming,
/// the probe pattern has already found the best plan for pattern matching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinDecomposition {
    build_pattern: Pattern,
    build_v_rank_map: BTreeMap<PatternId, PatternId>,
    build_e_rank_map: BTreeMap<PatternId, PatternId>,
    probe_pattern: Pattern,
    probe_v_rank_map: BTreeMap<PatternId, PatternId>,
    probe_e_rank_map: BTreeMap<PatternId, PatternId>,
    shared_vertices: BTreeSet<PatternId>,
}

impl JoinDecomposition {
    fn init(build_pattern: Pattern, probe_pattern: Pattern) -> Self {
        let shared_vertices: BTreeSet<PatternId> = build_pattern
            .vertices_iter()
            .map(|vertex| vertex.get_id())
            .filter(|&v_id| probe_pattern.get_vertex(v_id).is_some())
            .collect();
        JoinDecomposition::new(build_pattern, probe_pattern, shared_vertices)
    }

    fn new(build_pattern: Pattern, probe_pattern: Pattern, shared_vertices: BTreeSet<PatternId>) -> Self {
        JoinDecomposition {
            build_pattern,
            probe_pattern,
            shared_vertices,
            build_v_rank_map: BTreeMap::new(),
            build_e_rank_map: BTreeMap::new(),
            probe_v_rank_map: BTreeMap::new(),
            probe_e_rank_map: BTreeMap::new(),
        }
    }

    fn set_rank_map(&mut self, target_pattern: &Pattern) {
        self.build_v_rank_map = get_v_rank_map_between_patterns(&self.build_pattern, target_pattern);
        self.build_e_rank_map = get_e_rank_map_between_patterns(&self.build_pattern, target_pattern);
        self.probe_v_rank_map = get_v_rank_map_between_patterns(&self.probe_pattern, target_pattern);
        self.probe_e_rank_map = get_e_rank_map_between_patterns(&self.probe_pattern, target_pattern);
    }

    pub fn get_build_pattern(&self) -> &Pattern {
        &self.build_pattern
    }

    pub fn get_probe_pattern(&self) -> &Pattern {
        &self.probe_pattern
    }

    pub fn get_shared_vertices(&self) -> &BTreeSet<PatternId> {
        &self.shared_vertices
    }

    pub fn get_build_v_rank_map(&self) -> &BTreeMap<PatternId, PatternId> {
        &self.build_v_rank_map
    }

    pub fn get_build_e_rank_map(&self) -> &BTreeMap<PatternId, PatternId> {
        &self.build_e_rank_map
    }

    pub fn get_probe_v_rank_map(&self) -> &BTreeMap<PatternId, PatternId> {
        &self.probe_v_rank_map
    }

    pub fn get_probe_e_rank_map(&self) -> &BTreeMap<PatternId, PatternId> {
        &self.probe_e_rank_map
    }

    /// Specify which vertices in build/probe patterns serve as the criteria for joining
    /// - The first element represents the ID of the shared vertex in probe pattern
    /// - The second element represents the ID of the shared vertex in build pattern
    pub fn get_shared_vertices_rank_map(&self) -> BTreeMap<usize, usize> {
        let mut shared_vertices_rank_map: BTreeMap<usize, usize> = BTreeMap::new();
        self.shared_vertices
            .iter()
            .for_each(|&shared_v_id| {
                let v_rank_build = self
                    .build_pattern
                    .get_vertex_rank(shared_v_id)
                    .expect("Failed to get vertex rank");
                let v_rank_probe = self
                    .probe_pattern
                    .get_vertex_rank(shared_v_id)
                    .expect("Failed to get vertex rank");
                shared_vertices_rank_map.insert(v_rank_probe, v_rank_build);
            });
        shared_vertices_rank_map
    }
}

impl JoinDecomposition {
    /// Generate join keys for logical plan generation
    pub fn generate_join_keys(&self) -> Vec<Variable> {
        self.get_shared_vertices_rank_map()
            .iter()
            .map(|(&v_rank_probe, &_v_rank_build)| {
                let v_id: PatternId = self
                    .get_probe_pattern()
                    .get_vertex_from_rank(v_rank_probe)
                    .expect("Failed to get vertex from rank")
                    .get_id();
                let tag: NameOrId = (v_id as i32).into();
                common_pb::Variable { tag: tag.try_into().ok(), property: None }
            })
            .collect()
    }
}

impl JoinDecomposition {
    /// ## Usage:
    /// Build Pattern joins Probe Pattern on shared vertices = Target Pattern
    pub fn join(&self) -> IrResult<Pattern> {
        // Initialize target pattern from build pattern
        let build_pattern = self.get_build_pattern();
        let probe_pattern: &Pattern = self.get_probe_pattern();
        let mut target_pattern: Pattern = build_pattern.clone();
        let shared_vertices_id_map: BTreeMap<PatternId, PatternId> = self.init_shared_vertices_id_map();
        let mut vertex_id_map_probe_to_build: BTreeMap<PatternId, PatternId> =
            shared_vertices_id_map.clone();
        // Record the mapping of edge ID from probe pattern to build pattern
        // Keys of the map are edges that have been inserted into target pattern
        let mut visited_edges_in_probe_pattern: BTreeSet<PatternId> = BTreeSet::new();
        // Collect all edges that should be inserted into target pattern.
        let mut edges_to_insert: Vec<PatternEdge> = Vec::new();
        let mut next_insert_vertex_id: PatternId = target_pattern.get_max_vertex_id() + 1;
        let mut next_insert_edge_id: PatternId = target_pattern.get_max_edge_id() + 1;
        // For every shared vertex, iterate all its adjacencies to insert into target pattern
        // Step-1: Push the first shared vertex to the queue as the starting vertex of BFS
        let mut vertex_queue: VecDeque<PatternId> = shared_vertices_id_map
            .keys()
            .map(|v_id_probe| *v_id_probe)
            .collect();
        while let Some(v_id) = vertex_queue.pop_back() {
            let mut visited_edges: BTreeSet<PatternId> = BTreeSet::new();
            probe_pattern
                .adjacencies_iter(v_id)
                .filter(|adj| {
                    !visited_edges_in_probe_pattern.contains(&adj.get_edge_id())
                        && target_pattern
                            .get_edge(adj.get_edge_id())
                            .is_none()
                })
                .for_each(|adj| {
                    let adj_v_id: PatternId = adj.get_adj_vertex().get_id();
                    let adj_edge_id: PatternId = adj.get_edge_id();
                    let adj_edge = probe_pattern.get_edge(adj_edge_id).unwrap();
                    // In default, the IDs of start and end vertices are set for outgoing edges
                    // Swap if the edge is incoming
                    let mut start_v_id: PatternId = *vertex_id_map_probe_to_build.get(&v_id).unwrap();
                    let mut end_v_id: PatternId =
                        if let Some(v_id_build) = vertex_id_map_probe_to_build.get(&adj_v_id) {
                            *v_id_build
                        } else {
                            let next_v_id: PatternId = next_insert_vertex_id;
                            next_insert_vertex_id += 1;
                            vertex_id_map_probe_to_build.insert(adj_v_id, next_v_id);
                            next_v_id
                        };
                    match adj.get_direction() {
                        PatternDirection::Out => (),
                        PatternDirection::In => {
                            (start_v_id, end_v_id) = (end_v_id, start_v_id);
                        }
                    }

                    // Insert the edge into target pattern
                    let next_edge_id: PatternId = next_insert_edge_id;
                    next_insert_edge_id += 1;
                    let edge_to_insert: PatternEdge = PatternEdge::new(
                        next_edge_id,
                        adj_edge.get_label(),
                        PatternVertex::new(start_v_id, adj_edge.get_start_vertex().get_label()),
                        PatternVertex::new(end_v_id, adj_edge.get_end_vertex().get_label()),
                    );
                    edges_to_insert.push(edge_to_insert);
                    // Construct the mapping of edge id from probe pattern to build patten
                    visited_edges.insert(adj_edge_id);
                    // Push the newly introduced vertices to the vertex queue
                    vertex_queue.push_back(adj_v_id);
                });

            visited_edges_in_probe_pattern.append(&mut visited_edges);
        }
        // Insert edges to the joined pattern
        target_pattern = target_pattern
            .extend_by_edges(edges_to_insert.iter())
            .expect("Failed to extend pattern by edges");
        Ok(target_pattern)
    }

    /// The join process will only use the vertex/edge id in the given build/probe patterns without creating new ids.
    pub fn join_with_raw_id(&self) -> IrResult<Pattern> {
        // Initialize target pattern from build pattern
        let build_pattern = self.get_build_pattern();
        let probe_pattern: &Pattern = self.get_probe_pattern();
        let mut target_pattern: Pattern = build_pattern.clone();
        // Record the mapping of edge ID from probe pattern to build pattern
        // Keys of the map are edges that have been inserted into target pattern
        let mut visited_edges: BTreeSet<PatternId> = BTreeSet::new();
        // For every shared vertex, iterate all its adjacencies to insert into target pattern
        // Step-1: Push the first shared vertex to the queue as the starting vertex of BFS
        let mut vertex_queue: VecDeque<PatternId> = self
            .get_shared_vertices()
            .iter()
            .map(|&shared_v_id| shared_v_id)
            .collect();
        // Collect all edges that should be inserted into target pattern.
        let mut edges_to_insert: Vec<PatternEdge> = vec![];
        while let Some(v_id) = vertex_queue.pop_back() {
            probe_pattern
                .adjacencies_iter(v_id)
                .filter(|&adj| visited_edges.insert(adj.get_edge_id()))
                .filter(|&adj| {
                    target_pattern
                        .get_edge(adj.get_edge_id())
                        .is_none()
                })
                .for_each(|adj| {
                    let adj_v_id: PatternId = adj.get_adj_vertex().get_id();
                    let adj_edge_id: PatternId = adj.get_edge_id();
                    let edge_to_insert: PatternEdge = probe_pattern
                        .get_edge(adj_edge_id)
                        .expect("Failed to get edge from id")
                        .clone();
                    edges_to_insert.push(edge_to_insert);
                    // Push the newly introduced vertices to the vertex queue
                    vertex_queue.push_back(adj_v_id);
                });
        }
        // Insert edges to the joined pattern
        target_pattern = target_pattern
            .extend_by_edges(edges_to_insert.iter())
            .expect("Failed to extend pattern by edges");
        Ok(target_pattern)
    }

    fn init_shared_vertices_id_map(&self) -> BTreeMap<PatternId, PatternId> {
        let mut shared_vertices_id_map: BTreeMap<PatternId, PatternId> = BTreeMap::new();
        self.get_shared_vertices_rank_map()
            .iter()
            .for_each(|(v_rank_probe, v_rank_build)| {
                let v_id_probe: PatternId = self
                    .get_probe_pattern()
                    .get_vertex_from_rank(*v_rank_probe)
                    .unwrap()
                    .get_id();
                let v_id_build: PatternId = self
                    .get_build_pattern()
                    .get_vertex_from_rank(*v_rank_build)
                    .unwrap()
                    .get_id();
                shared_vertices_id_map.insert(v_id_probe, v_id_build);
            });

        shared_vertices_id_map
    }
}

impl JoinDecomposition {
    /// Determine whether a decomposition plan is valid or not
    /// This function is used to filter some useless decompoistion plans, and the criteria are as follow:
    /// - **There must be at least one disjoint vertex in both build and probe patterns.**
    /// (Only one disjoint vertex is equivalent to Extend Step)
    /// - **Build Pattern must be larger in size than probe pattern.**
    /// The reason is that when we perform bottom-up DP algorithm, we can always find the best plan of the smaller probe pattern when the larger build pattern is considered.
    fn is_valid(&self, target_pattern: &Pattern) -> bool {
        let build_pattern_vertices_num = self.build_pattern.get_vertices_num();
        let probe_pattern_vertices_num = self.probe_pattern.get_vertices_num();
        let shared_vertices_num = self.shared_vertices.len();
        let is_plan_valid: bool = shared_vertices_num > 0
            && build_pattern_vertices_num > shared_vertices_num + 1
            && probe_pattern_vertices_num > shared_vertices_num + 1;
        if !is_plan_valid {
            return false;
        }
        for &shared_v_id in self.get_shared_vertices().iter() {
            let num_connected_build_vertices = target_pattern
                .adjacencies_iter(shared_v_id)
                .map(|adj| adj.get_adj_vertex().get_id())
                .filter(|adj_v_id| !self.get_shared_vertices().contains(adj_v_id))
                .filter(|&adj_v_id| {
                    self.get_build_pattern()
                        .get_vertex(adj_v_id)
                        .is_some()
                })
                .map(|adj_v_id| adj_v_id)
                .count();
            let num_connected_probe_vertices = target_pattern
                .adjacencies_iter(shared_v_id)
                .map(|adj| adj.get_adj_vertex().get_id())
                .filter(|adj_v_id| !self.get_shared_vertices().contains(adj_v_id))
                .filter(|&adj_v_id| {
                    self.get_probe_pattern()
                        .get_vertex(adj_v_id)
                        .is_some()
                })
                .map(|adj_v_id| adj_v_id)
                .count();
            if num_connected_build_vertices == 0 || num_connected_probe_vertices == 0 {
                return false;
            }
        }

        // Ensure that the two subpatterns can join to be the target pattern
        let joined_pattern: Pattern = self.join().expect("Failed to get two patterns");
        if joined_pattern.get_vertices_num() != target_pattern.get_vertices_num()
            || joined_pattern.get_edges_num() != target_pattern.get_edges_num()
        {
            return false;
        }
        let target_pattern_code: Vec<u8> = target_pattern.encode_to();
        let joined_pattern_code: Vec<u8> = joined_pattern.encode_to();
        if joined_pattern_code != target_pattern_code {
            return false;
        }

        return true;
    }

    /// Check if the decomposition plan is redundant
    fn is_redundant(
        &self, _target_pattern: &Pattern, pattern_code_set: &mut BTreeSet<BTreeSet<Vec<u8>>>,
    ) -> bool {
        let mut pattern_codes: BTreeSet<Vec<u8>> = BTreeSet::new();
        pattern_codes.insert(self.get_build_pattern().encode_to());
        pattern_codes.insert(self.get_probe_pattern().encode_to());
        !pattern_code_set.insert(pattern_codes)
    }
}

struct JoinDecompositionPlan {
    plan: JoinDecomposition,
    always_shared_vertices: BTreeSet<PatternId>,
}

impl Pattern {
    /// ## Usage:
    /// Given a large pattern, use BFS to decompose it into a vector of join plans (Build Pattern + Binary Join Step).
    ///
    /// ## Note:
    /// Probe pattern contains two types of vertices: **disjoint** and **shared**
    ///
    /// Shared vertices are those used as the criteria of join operation, therefore existing in both patterns, and disjoint vertices can only exist in one of the patterns.
    pub fn binary_join_decomposition(&self) -> IrResult<Vec<JoinDecomposition>> {
        // Pattern with less than 5 vertices has no binary join plan
        if self.get_vertices_num() < 5 {
            return Ok(vec![]);
        }

        // Initialize the bfs queue with join plans whose probe patterns have size = 1
        let mut decomposition_plans: Vec<JoinDecomposition> = Vec::new();
        let mut candidate_plan_queue: VecDeque<JoinDecompositionPlan> = self
            .init_candidate_decomposition_plan_queue()
            .expect("Failed to initialize candidate join decomposition plan");
        let mut pattern_code_set: BTreeSet<BTreeSet<Vec<u8>>> = BTreeSet::new();

        while let Some(candidate_plan) = candidate_plan_queue.pop_front() {
            // Extend to other decomposition plans
            self.extend_decomposition_plans(&candidate_plan)
                .into_iter()
                .filter(|plan_to_extend| {
                    !plan_to_extend
                        .plan
                        .is_redundant(self, &mut pattern_code_set)
                })
                .for_each(|plan_to_extend| {
                    candidate_plan_queue.push_back(plan_to_extend);
                });
            // candidate_plan_queue.append(&mut self.extend_decomposition_plans(&candidate_plan));

            if candidate_plan.plan.is_valid(self) {
                let mut build_pattern: Pattern = candidate_plan.plan.build_pattern;
                let mut probe_pattern: Pattern = candidate_plan.plan.probe_pattern;
                if build_pattern.get_vertices_num() < probe_pattern.get_vertices_num() {
                    (build_pattern, probe_pattern) = (probe_pattern, build_pattern);
                }

                decomposition_plans.push(JoinDecomposition::init(build_pattern, probe_pattern));
            }
        }

        // Append vertex predicate info to each binary join plan
        decomposition_plans
            .iter_mut()
            .for_each(|join_plan| {
                self.vertices_iter()
                    .map(|vertex| vertex.get_id())
                    .for_each(|v_id| {
                        if let Some(predicate) = self.get_vertex_predicate(v_id) {
                            join_plan
                                .build_pattern
                                .set_vertex_predicate(v_id, predicate.clone());
                            join_plan
                                .probe_pattern
                                .set_vertex_predicate(v_id, predicate.clone());
                        }
                    });
            });
        for binary_join_plan in decomposition_plans.iter_mut() {
            binary_join_plan.set_rank_map(self);
        }
        Ok(decomposition_plans)
    }

    fn init_candidate_decomposition_plan_queue(&self) -> IrResult<VecDeque<JoinDecompositionPlan>> {
        let mut candidate_decomposition_plan_queue: VecDeque<JoinDecompositionPlan> = VecDeque::new();
        // Initialize the bfs queue with join plans whose probe patterns have size = 1
        let mut vertex_ids: Vec<PatternId> = self
            .vertices_iter()
            .map(|vertex| vertex.get_id())
            .collect();
        vertex_ids.sort_by(|&v1_id, &v2_id| {
            let v1_degree = self.get_vertex_degree(v1_id);
            let v2_degree = self.get_vertex_degree(v2_id);
            v1_degree.cmp(&v2_degree)
        });
        for v_id in vertex_ids {
            let v_label: PatternLabelId = self
                .get_vertex(v_id)
                .expect("Faield to get vertex from id")
                .get_label();
            let build_pattern: Pattern = self.clone();
            let probe_pattern = Pattern::try_from(PatternVertex::new(v_id, v_label))
                .expect("Faield to build pattern from one single vertex");
            let plan = JoinDecompositionPlan {
                plan: JoinDecomposition::init(build_pattern, probe_pattern),
                always_shared_vertices: BTreeSet::new(),
            };
            candidate_decomposition_plan_queue.push_back(plan);
            break;
        }

        Ok(candidate_decomposition_plan_queue)
    }

    /// Extend to other candidate decomposition plans and shore in bfs queue
    ///
    /// Return an iterator of candidate decomposition plans
    ///
    /// Idea:
    /// to extend an existing decomposition plan, at least one disjoint vertex should be added to the probe pattern.
    /// The new disjoint vertex can derived be of two cases:
    /// Case-1: From a shared vertex
    /// Case-2: From a neighbor of a shared vertex
    fn extend_decomposition_plans(&self, last_plan: &JoinDecompositionPlan) -> Vec<JoinDecompositionPlan> {
        let mut candidate_plans_to_extend: Vec<JoinDecompositionPlan> = vec![];
        let shared_vertices: &BTreeSet<PatternId> = last_plan.plan.get_shared_vertices();
        for &shared_v_id in shared_vertices.iter() {
            // Case-1: Disjoint vertex from shared vertex
            self.extend_shared_vertex_as_disjoint(last_plan, shared_v_id)
                .into_iter()
                .for_each(|plan_to_extend| {
                    candidate_plans_to_extend.push(plan_to_extend);
                });

            // Case-2: Disjoint vertex from neighbor of shared vertex
            self.extend_one_neighbor_of_shared_vertex_as_shared(last_plan, shared_v_id)
                .into_iter()
                .for_each(|plan_to_extend| {
                    candidate_plans_to_extend.push(plan_to_extend);
                });
        }

        candidate_plans_to_extend
    }

    fn extend_shared_vertex_as_disjoint(
        &self, last_plan: &JoinDecompositionPlan, shared_v_id: PatternId,
    ) -> Vec<JoinDecompositionPlan> {
        // Vertices that are always shared cannot be disjoint
        if last_plan
            .always_shared_vertices
            .contains(&shared_v_id)
        {
            return vec![];
        }

        let last_build_pattern: &Pattern = last_plan.plan.get_build_pattern();
        let last_probe_pattern: &Pattern = last_plan.plan.get_probe_pattern();
        let last_shared_vertices: &BTreeSet<PatternId> = last_plan.plan.get_shared_vertices();
        let mut next_build_pattern: Pattern = last_build_pattern.clone();
        let mut next_probe_pattern: Pattern = last_probe_pattern.clone();
        // Not removing vertex as disjoint if there is only one vertex in build pattern
        if next_build_pattern.get_vertices_num() == 1 {
            return vec![];
        }

        // Remove the new disjoint vertex from build pattern
        next_build_pattern.remove_vertex_local(shared_v_id);
        if next_build_pattern.is_connected() {
            // Collects edge to insert into probe pattern
            let mut recorded_edges_to_insert: BTreeSet<PatternId> = BTreeSet::new();
            let mut edges_to_insert: Vec<PatternEdge> = vec![];
            // Neighbors of the new disjoint vertex in build pattern become shared vertices
            let neighbor_vertices: BTreeSet<PatternId> = last_build_pattern
                .adjacencies_iter(shared_v_id)
                .filter(|adj| !last_shared_vertices.contains(&adj.get_adj_vertex().get_id()))
                .map(|adj| {
                    let pattern_edge: PatternEdge = last_build_pattern
                        .get_edge(adj.get_edge_id())
                        .expect("Failed to get edge from id")
                        .clone();
                    recorded_edges_to_insert.insert(pattern_edge.get_id());
                    edges_to_insert.push(pattern_edge);
                    adj.get_adj_vertex().get_id()
                })
                .collect();

            for neighbor_v_id in neighbor_vertices {
                last_build_pattern
                    .adjacencies_iter(neighbor_v_id)
                    .filter(|&adj| {
                        let adj_v_id: PatternId = adj.get_adj_vertex().get_id();
                        next_probe_pattern
                            .get_vertex(adj_v_id)
                            .is_some()
                    })
                    .filter(|&adj| {
                        let adj_e_id: PatternId = adj.get_edge_id();
                        recorded_edges_to_insert.insert(adj_e_id)
                    })
                    .for_each(|adj| {
                        let pattern_edge: PatternEdge = last_build_pattern
                            .get_edge(adj.get_edge_id())
                            .expect("Failed to get pattern edge from id")
                            .clone();
                        edges_to_insert.push(pattern_edge);
                    });
            }
            // Extend the probe pattern with new edges
            next_probe_pattern = next_probe_pattern
                .extend_by_edges(edges_to_insert.iter())
                .expect("Failed to extend pattern by edges");
            vec![JoinDecompositionPlan {
                plan: JoinDecomposition::init(next_build_pattern, next_probe_pattern),
                always_shared_vertices: last_plan.always_shared_vertices.clone(),
            }]
        } else {
            // Union connected components if the new build pattern is not connected
            let num_connected_comps = next_build_pattern.get_connected_component_num();
            // Not deal with more than 2 connected components
            if num_connected_comps > 2 {
                println!("Error: Unsupported num of connected components {}", num_connected_comps);
                return vec![];
            }

            // Exteng edges in one of the connected coomponents
            let connected_components: Vec<Pattern> = next_build_pattern.get_connected_components();
            let extended_connected_comps: Vec<Pattern> = connected_components
                .into_iter()
                .map(|connected_component| {
                    // insert the shared vertex and adjacent edges to the pattern
                    let edges_to_insert: Vec<&PatternEdge> = self
                        .adjacencies_iter(shared_v_id)
                        .filter(|&adj| {
                            let adj_v_id = adj.get_adj_vertex().get_id();
                            connected_component
                                .get_vertex(adj_v_id)
                                .is_some()
                        })
                        .map(|adj| {
                            // Collect edges to insert to probe pattern
                            let edge_id: PatternId = adj.get_edge_id();
                            self.get_edge(edge_id)
                                .expect("Edge Not Found in Pattern")
                        })
                        .collect();
                    connected_component
                        .extend_by_edges(edges_to_insert.into_iter())
                        .expect("Failed to extend pattern by edges")
                })
                .collect();
            let next_probe_patterns: Vec<Pattern> = extended_connected_comps
                .iter()
                .map(|extended_connected_comp| {
                    let binary_join_plan = JoinDecomposition::init(
                        last_probe_pattern.clone(),
                        extended_connected_comp.clone(),
                    );
                    binary_join_plan
                        .join_with_raw_id()
                        .expect("Failed to join two patterns")
                })
                .collect();
            // Two Bianry Join Plans of Connected Components
            vec![
                JoinDecompositionPlan {
                    plan: JoinDecomposition::init(
                        extended_connected_comps[0].clone(),
                        next_probe_patterns[1].clone(),
                    ),
                    always_shared_vertices: last_plan.always_shared_vertices.clone(),
                },
                JoinDecompositionPlan {
                    plan: JoinDecomposition::init(
                        extended_connected_comps[1].clone(),
                        next_probe_patterns[0].clone(),
                    ),
                    always_shared_vertices: last_plan.always_shared_vertices.clone(),
                },
            ]
        }
    }

    fn extend_one_neighbor_of_shared_vertex_as_shared(
        &self, last_plan: &JoinDecompositionPlan, shared_v_id: PatternId,
    ) -> Vec<JoinDecompositionPlan> {
        let last_build_pattern: &Pattern = last_plan.plan.get_build_pattern();
        let last_probe_pattern: &Pattern = last_plan.plan.get_probe_pattern();
        let last_shared_vertices: &BTreeSet<PatternId> = last_plan.plan.get_shared_vertices();
        // Neighbors of the new disjoint vertex in build pattern become shared vertices
        let neighbor_vertices: BTreeSet<PatternId> = last_build_pattern
            .adjacencies_iter(shared_v_id)
            .map(|adj| adj.get_adj_vertex().get_id())
            .filter(|adj_v_id| !last_shared_vertices.contains(adj_v_id))
            .collect();
        // Collect plans to extend to
        let mut plans_to_extend: Vec<JoinDecompositionPlan> = vec![];
        neighbor_vertices
            .iter()
            .for_each(|&new_shared_v_id| {
                // Collect edges to be inserted to probe pattern
                let edges_to_insert: Vec<&PatternEdge> = self
                    .adjacencies_iter(new_shared_v_id)
                    .filter(|&adj| {
                        let adj_v_id: PatternId = adj.get_adj_vertex().get_id();
                        last_shared_vertices.contains(&adj_v_id)
                    })
                    .map(|adj| {
                        self.get_edge(adj.get_edge_id())
                            .expect("Failed to get edge from id")
                    })
                    .collect();
                // Generate the plan to extend to
                let next_build_pattern: Pattern = last_build_pattern.clone();
                let next_probe_pattern: Pattern = last_probe_pattern
                    .extend_by_edges(edges_to_insert.into_iter())
                    .expect("Failed to extend pattern by edges");
                let mut always_shared_vertices = last_plan.always_shared_vertices.clone();
                always_shared_vertices.insert(shared_v_id);
                let num_valid_edges = self
                    .adjacencies_iter(shared_v_id)
                    .map(|adj| adj.get_adj_vertex().get_id())
                    .filter(|&adj_v_id| {
                        next_probe_pattern
                            .get_vertex(adj_v_id)
                            .is_none()
                    })
                    .count();
                if num_valid_edges > 0 {
                    plans_to_extend.push(JoinDecompositionPlan {
                        plan: JoinDecomposition::init(next_build_pattern, next_probe_pattern),
                        always_shared_vertices,
                    });
                }
            });
        plans_to_extend
    }
}

fn get_v_rank_map_between_patterns(
    base_pattern: &Pattern, target_pattern: &Pattern,
) -> BTreeMap<PatternId, PatternId> {
    let mut v_rank_map = BTreeMap::new();
    for base_v in base_pattern.vertices_iter() {
        let base_v_rank = base_pattern
            .get_vertex_rank(base_v.get_id())
            .expect("Fail to get vertex rank with ID from base pattern");
        let target_v_rank = target_pattern
            .get_vertex_rank(base_v.get_id())
            .expect("Fail to get vertex rank with ID from target pattern");
        v_rank_map.insert(base_v_rank, target_v_rank);
    }
    v_rank_map
}

fn get_e_rank_map_between_patterns(
    base_pattern: &Pattern, target_pattern: &Pattern,
) -> BTreeMap<PatternId, PatternId> {
    let mut e_rank_map = BTreeMap::new();
    for base_e in base_pattern.edges_iter() {
        let base_e_rank = base_pattern
            .get_edge_rank(base_e.get_id())
            .expect("Fail to get edge rank with ID from base pattern");
        let target_e_rank = target_pattern
            .get_edge_rank(base_e.get_id())
            .expect("Fail to get edge rank with ID from target pattern");
        e_rank_map.insert(base_e_rank, target_e_rank);
    }
    e_rank_map
}

impl Pattern {
    pub fn de_join(&self, join_step: &DefiniteJoinStep) -> Option<Pattern> {
        let edges_to_remove: Vec<PatternId> = join_step
            .probe_pattern
            .edges_iter()
            .filter(|edge| {
                let start_v_id = edge.get_start_vertex().get_id();
                let end_v_id = edge.get_end_vertex().get_id();
                !(join_step.shared_vertices.contains(&start_v_id)
                    && join_step.shared_vertices.contains(&end_v_id))
            })
            .map(|edge| edge.get_id())
            .collect();
        self.clone()
            .remove_edges(edges_to_remove.into_iter())
    }

    // ToDo: Add predicate info the the new build pattern
    pub fn new_build_pattern(
        &self, build_pattern: &Pattern, build_e_rank_map: &BTreeMap<PatternId, PatternId>,
    ) -> IrResult<Self> {
        let mut new_build_pattern_edges = vec![];
        for build_pattern_edge in build_pattern.edges_iter() {
            let edge_rank = build_pattern
                .get_edge_rank(build_pattern_edge.get_id())
                .expect("Fail to get edge rank with ID");
            let edge_rank_in_target_pattern = *build_e_rank_map
                .get(&edge_rank)
                .expect("Fail to map build pattern's edge to target pattern edge");
            let new_edge = self
                .get_edge_from_rank(edge_rank_in_target_pattern)
                .expect("Fail to get edge from rank")
                .clone();
            new_build_pattern_edges.push(new_edge)
        }
        Pattern::try_from(new_build_pattern_edges)
    }
}
