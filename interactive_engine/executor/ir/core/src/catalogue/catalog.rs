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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use bincode::Result as BincodeResult;
use bincode::{deserialize_from, serialize_into};
use petgraph::graph::{EdgeIndex, EdgeReference, Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use serde::de::Visitor;
use serde::{Deserialize, Serialize};

use crate::catalogue::join_step::BinaryJoinPlan;
use crate::catalogue::extend_step::{get_subsets, ExtendEdge, ExtendStep};
use crate::catalogue::pattern::{Adjacency, Pattern, PatternEdge, PatternVertex};
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::{DynIter, PatternDirection, PatternId, PatternLabelId};

/// In Catalog Graph, Vertex Represents a Pattern
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternWeight {
    pattern: Pattern,
    /// Estimate how many such pattern in a graph
    count: usize,
    /// Store previous 1 step estimated best approach to get the pattern
    best_approach: Option<Approach>,
    ///
    out_extend_map: HashMap<Vec<u8>, Approach>,
}

impl PatternWeight {
    pub fn get_pattern(&self) -> &Pattern {
        &self.pattern
    }

    pub fn get_count(&self) -> usize {
        self.count
    }

    pub fn get_best_approach(&self) -> Option<Approach> {
        self.best_approach
    }

    pub fn get_extend_approach(&self, extend_code: &Vec<u8>) -> Option<Approach> {
        self.out_extend_map.get(extend_code).cloned()
    }

    pub fn out_extend_iter(&self) -> DynIter<Approach> {
        let mut out_extend_codes: Vec<&Vec<u8>> = self
            .out_extend_map
            .iter()
            .map(|(code, _)| code)
            .collect();
        out_extend_codes.sort_by(|&code1, &code2| code1.len().cmp(&code2.len()));
        Box::new(
            out_extend_codes
                .into_iter()
                .map(move |code| *self.out_extend_map.get(code).unwrap()),
        )
    }

    pub fn set_count(&mut self, count: usize) {
        self.count = count
    }

    pub fn set_best_approach(&mut self, best_approach: Approach) {
        self.best_approach = Some(best_approach)
    }

    pub fn add_out_extend(&mut self, extend_code: Vec<u8>, approach: Approach) {
        self.out_extend_map
            .insert(extend_code, approach);
    }
}

/// Edge Weight for approach case is join
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinWeight {
    /// Probe Pattern Node Index
    probe_pattern_node_index: NodeIndex,
    join_plan: BinaryJoinPlan,
}

impl JoinWeight {
    pub fn get_probe_pattern_node_index(&self) -> NodeIndex {
        self.probe_pattern_node_index
    }

    pub fn get_join_plan(&self) -> &BinaryJoinPlan {
        &self.join_plan
    }
}

/// Edge Weight for approach case is extend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendWeight {
    extend_step: ExtendStep,
    /// Target vertex's order in the target pattern
    target_vertex_rank: PatternId,
    /// count infos
    adjacency_count: usize,
    intersect_count: usize,
}

impl ExtendWeight {
    pub fn get_extend_step(&self) -> &ExtendStep {
        &self.extend_step
    }

    pub fn get_target_vertex_rank(&self) -> usize {
        self.target_vertex_rank
    }

    pub fn get_adjacency_count(&self) -> usize {
        self.adjacency_count
    }

    pub fn get_intersect_count(&self) -> usize {
        self.intersect_count
    }

    pub fn set_extend_step(&mut self, extend_step: ExtendStep) {
        self.extend_step = extend_step
    }

    pub fn set_adjacency_count(&mut self, count: usize) {
        self.adjacency_count = count
    }

    pub fn set_intersect_count(&mut self, count: usize) {
        self.intersect_count = count
    }
}

/// In Catalog Graph, Edge Represents an approach from a pattern to a another pattern
/// The approach is either:
/// pattern <join> pattern -> pattern
/// pattern <extend> extend_step -> pattern
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApproachWeight {
    /// Case that the approach is join
    BinaryJoinStep(JoinWeight),
    /// Case that the approach is extend
    ExtendStep(ExtendWeight),
}

/// Methods of accesing some fields of EdgeWeight
impl ApproachWeight {
    pub fn is_extend(&self) -> bool {
        if let ApproachWeight::ExtendStep(_) = self {
            true
        } else {
            false
        }
    }

    pub fn is_join(&self) -> bool {
        if let ApproachWeight::BinaryJoinStep(_) = self {
            true
        } else {
            false
        }
    }

    pub fn get_extend_weight(&self) -> Option<&ExtendWeight> {
        if let ApproachWeight::ExtendStep(extend_weight) = self {
            Some(extend_weight)
        } else {
            None
        }
    }

    pub fn get_extend_weight_mut(&mut self) -> Option<&mut ExtendWeight> {
        if let ApproachWeight::ExtendStep(extend_weight) = self {
            Some(extend_weight)
        } else {
            None
        }
    }

    pub fn get_join_weight(&self) -> Option<&JoinWeight> {
        if let ApproachWeight::BinaryJoinStep(join_weight) = self {
            Some(join_weight)
        } else {
            None
        }
    }

    pub fn get_join_weight_mut(&mut self) -> Option<&mut JoinWeight> {
        if let ApproachWeight::BinaryJoinStep(join_weight) = self {
            Some(join_weight)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Approach {
    approach_index: EdgeIndex,
    src_pattern_index: NodeIndex,
    target_pattern_index: NodeIndex,
}

impl Approach {
    pub fn new(
        src_pattern_index: NodeIndex, target_pattern_index: NodeIndex, approach_index: EdgeIndex,
    ) -> Self {
        Approach { approach_index, src_pattern_index, target_pattern_index }
    }
}

impl<'a> From<EdgeReference<'a, ApproachWeight>> for Approach {
    fn from(edge_ref: EdgeReference<ApproachWeight>) -> Self {
        Approach {
            approach_index: edge_ref.id(),
            src_pattern_index: edge_ref.source(),
            target_pattern_index: edge_ref.target(),
        }
    }
}

impl Approach {
    pub fn get_approach_index(&self) -> EdgeIndex {
        self.approach_index
    }

    pub fn get_src_pattern_index(&self) -> NodeIndex {
        self.src_pattern_index
    }

    pub fn get_target_pattern_index(&self) -> NodeIndex {
        self.target_pattern_index
    }
}

#[derive(Debug, Clone)]
pub enum PatMatPlanSpace {
    ExtendWithIntersection,
    BinaryJoin,
    Hybrid,
}

impl Default for PatMatPlanSpace {
    fn default() -> Self {
        PatMatPlanSpace::Hybrid
    }
}

#[derive(Debug, Clone, Default)]
pub struct Catalogue {
    /// Catalog Graph
    store: Graph<PatternWeight, ApproachWeight>,
    /// Key: pattern code, Value: Node(Vertex) Index
    /// Usage: use a pattern code to uniquely identify a pattern in catalog graph
    pattern_locate_map: HashMap<Vec<u8>, NodeIndex>,
    /// Those patterns with size 1 are the entries of catalogue
    /// - Stores entries with their NodeIndex and PatternLabelId
    entries: Vec<NodeIndex>,
    /// Map from pattern code to its estimated cardinality
    pattern_count_map: HashMap<Vec<u8>, usize>,
    /// Pattern Match Plan Space
    plan_space: PatMatPlanSpace,
}

impl Catalogue {
    /// Build a catalogue from a pattern meta with some limits
    /// It can be used to build the basic parts of catalog graph
    pub fn build_from_meta(
        pattern_meta: &PatternMeta, pattern_size_limit: usize, same_label_vertex_limit: usize,
    ) -> Catalogue {
        let mut catalog = Catalogue::default();
        // Use BFS to generate the catalog graph
        let mut queue = VecDeque::new();
        // The one-vertex patterns are the starting points
        for vertex_label in pattern_meta.vertex_label_ids_iter() {
            let new_pattern = Pattern::from(PatternVertex::new(0, vertex_label));
            let (_, new_pattern_index) = catalog.add_pattern(new_pattern.clone());
            queue.push_back((new_pattern, new_pattern_index));
        }
        while let Some((relaxed_pattern, relaxed_pattern_index)) = queue.pop_front() {
            // Filter out those patterns beyond the pattern size limitation
            if relaxed_pattern.get_vertices_num() >= pattern_size_limit {
                continue;
            }
            // Find possible extend steps of the relaxed pattern
            let extend_steps = relaxed_pattern.get_extend_steps(pattern_meta, same_label_vertex_limit);
            for extend_step in extend_steps.iter() {
                // relaxed pattern + extend step to get new pattern
                let new_pattern = relaxed_pattern.extend(extend_step).unwrap();
                let (existed, new_pattern_index) = catalog.add_pattern(new_pattern.clone());
                let adj_vertex_rank = new_pattern
                    .get_vertex_rank(new_pattern.get_max_vertex_id())
                    .unwrap();
                catalog.add_extend_step(
                    extend_step.clone(),
                    relaxed_pattern_index,
                    new_pattern_index,
                    adj_vertex_rank,
                );
                if !existed {
                    queue.push_back((new_pattern, new_pattern_index));
                }
            }
        }
        catalog
    }

    /// Build a catalog from a pattern dedicated for its optimization
    pub fn build_from_pattern(pattern: &Pattern, plan_space: PatMatPlanSpace) -> Catalogue {
        // Initialize Catalog
        let mut catalog = Catalogue::default();
        catalog.set_plan_space(plan_space);
        // Update catalog with a given pattern to add necessary optimization info
        catalog.update_catalog_by_pattern(pattern);
        catalog
    }

    /// Usage: Update the current catalog with the given pattern.
    /// Aims to add neccesary optimization info of the pattern
    ///
    /// Use BFS to traverse all possible approaches to match the given pattern
    pub fn update_catalog_by_pattern(&mut self, pattern: &Pattern) {
        match self.plan_space {
            PatMatPlanSpace::ExtendWithIntersection => {
                self.update_extend_steps_by_pattern(pattern);
                println!("Extend Pattern Num: {}", self.get_patterns_num());
                println!("ExtendStep Num: {}", self.get_approaches_num());
            }
            PatMatPlanSpace::BinaryJoin => {
                self.update_join_steps_by_pattern(pattern);
            }
            PatMatPlanSpace::Hybrid => {
                self.update_extend_steps_by_pattern(pattern);
                println!("Extend Pattern Num: {}", self.get_patterns_num());
                println!("ExtendStep Num: {}", self.get_approaches_num());
                self.update_join_steps_by_pattern(pattern);
                println!("Extend Pattern Num: {}", self.get_patterns_num());
                println!("ExtendStep Num: {}", self.get_approaches_num());
            }
            _ => {
                panic!("Unsupported pattern match plan space");
            }
        }
    }

    /// Update the current catalog by given pattern
    /// Aims to add neccesary optimization info of the pattern
    pub fn update_extend_steps_by_pattern(&mut self, pattern: &Pattern) {
        // Use BFS to get all possible extend approaches to get the pattern
        let mut queue = VecDeque::new();
        // record sub patterns that are relaxed (by vertex id set)
        let mut relaxed_patterns = BTreeSet::new();
        // one-vertex pattern is the starting point of the BFS
        for vertex in pattern.vertices_iter() {
            let new_pattern = Pattern::from(*vertex);
            let (_, new_pattern_index) = self.add_pattern(new_pattern.clone());
            queue.push_back((new_pattern, new_pattern_index));
        }
        while let Some((relaxed_pattern, relaxed_pattern_index)) = queue.pop_front() {
            // check whether the current pattern is relaxed or not
            let relaxed_pattern_vertices: BTreeSet<PatternId> = relaxed_pattern
                .vertices_iter()
                .map(|v| v.get_id())
                .collect();
            if !relaxed_patterns.contains(&relaxed_pattern_vertices) {
                // Store already added target vertex of extend step
                for adj_vertex_id in get_adj_vertex_id_candies(pattern, &relaxed_pattern_vertices) {
                    // the adj_vertex is regarded as target vertex
                    // back link to the relaxed pattern to find all edges to be added
                    let final_back_links =
                        get_adjacencies_to_src_vertices(pattern, &relaxed_pattern_vertices, adj_vertex_id);
                    let back_links_collection = get_subsets(final_back_links.clone(), |_, _| false);
                    for back_links in back_links_collection {
                        let extend_step = new_extend_step_from_adjacencies(
                            &relaxed_pattern,
                            pattern,
                            &back_links,
                            adj_vertex_id,
                        );
                        let new_pattern = relaxed_pattern
                            .extend_by_edges(new_pattern_edges_from_adjacencies(pattern, &back_links))
                            .unwrap();
                        let (_, new_pattern_index) = self.add_pattern(new_pattern.clone());
                        let adj_vertex_rank = new_pattern
                            .get_vertex_rank(adj_vertex_id)
                            .unwrap();
                        self.add_extend_step(
                            extend_step,
                            relaxed_pattern_index,
                            new_pattern_index,
                            adj_vertex_rank,
                        );
                        if back_links == final_back_links {
                            queue.push_back((new_pattern, new_pattern_index));
                        }
                    }
                }
                relaxed_patterns.insert(relaxed_pattern_vertices);
            }
        }
    }

    /// Usage: Given a pattern, find out all (build pattern, binary join steo) pairs that join to get the given pattern, and store them in catalogue.
    fn update_join_steps_by_pattern(&mut self, pattern: &Pattern) {
        let pattern_code: Vec<u8> = pattern.encode_to();
        let pattern_node_index: NodeIndex = 
            if let Some(&node_index) = self.pattern_locate_map.get(&pattern_code) {
                node_index
            } else {
                let node_index = self
                    .store
                    .add_node(PatternWeight {
                        pattern: pattern.clone(),
                        count: 0,
                        best_approach: None,
                        out_extend_map: HashMap::new(),
                    });
                self.pattern_locate_map
                    .insert(pattern_code, node_index);
                node_index
            };
        // Iterate through all binary join plans
        pattern
            .binary_join_decomposition()
            .expect("Failed to decompose binary join plans")
            .into_iter()
            .for_each(|binary_join_plan| {
                // Build nodes for both build and probe patterns if no existing patterns can be found
                let build_pattern = binary_join_plan.get_build_pattern();
                let build_pattern_node_index = {
                    let build_pattern_code: Vec<u8> = build_pattern.encode_to();
                    self.get_pattern_index(&build_pattern_code)
                        .expect("Pattern not hit in catalogue")
                };
                let probe_pattern = binary_join_plan.get_probe_pattern().clone();
                let probe_pattern_node_index = {
                    let probe_pattern_code = probe_pattern.encode_to();
                    self.get_pattern_index(&probe_pattern_code)
                        .expect("Pattern not hit in catalogue")
                };
                // Iteratively update the two sub-patterns
                self.update_join_steps_by_pattern(&build_pattern);
                self.update_join_steps_by_pattern(&probe_pattern);
                // Build binary join step edges in Catalogue
                self.store.add_edge(
                    build_pattern_node_index,
                    pattern_node_index,
                    ApproachWeight::BinaryJoinStep(JoinWeight {
                        probe_pattern_node_index,
                        join_plan: binary_join_plan,
                    }),
                );
            });
    }
}

fn get_adj_vertex_id_candies(
    pattern: &Pattern, pre_pattern_vertices: &BTreeSet<PatternId>,
) -> BTreeSet<PatternId> {
    pre_pattern_vertices
        .iter()
        .flat_map(move |&vertex_id| {
            pattern
                .adjacencies_iter(vertex_id)
                .map(|adj| adj.get_adj_vertex().get_id())
        })
        .filter(move |vertex_id| !pre_pattern_vertices.contains(vertex_id))
        .collect()
}

fn get_adjacencies_to_src_vertices<'a>(
    pattern: &'a Pattern, vertices_set: &'a BTreeSet<PatternId>, adj_vertex_id: PatternId,
) -> Vec<&'a Adjacency> {
    pattern
        .adjacencies_iter(adj_vertex_id)
        .filter(|adj| vertices_set.contains(&adj.get_adj_vertex().get_id()))
        .collect()
}

fn new_extend_step_from_adjacencies(
    pre_pattern: &Pattern, pattern: &Pattern, adjacencies: &Vec<&Adjacency>, adj_vertex_id: PatternId,
) -> ExtendStep {
    let extend_edges: Vec<ExtendEdge> = adjacencies
        .iter()
        .map(|adj| {
            ExtendEdge::new(
                pre_pattern
                    .get_vertex_rank(adj.get_adj_vertex().get_id())
                    .unwrap(),
                adj.get_edge_label(),
                adj.get_direction().reverse(),
            )
        })
        .collect();
    let target_v_label = pattern
        .get_vertex(adj_vertex_id)
        .unwrap()
        .get_label();
    // generate new extend step
    ExtendStep::new(target_v_label, extend_edges)
}

fn new_pattern_edges_from_adjacencies<'a>(
    pattern: &'a Pattern, adjacencies: &'a Vec<&'a Adjacency>,
) -> DynIter<'a, &'a PatternEdge> {
    Box::new(
        adjacencies
            .iter()
            .map(move |adj| pattern.get_edge(adj.get_edge_id()).unwrap()),
    )
}

impl Catalogue {
    fn add_pattern(&mut self, pattern: Pattern) -> (bool, NodeIndex) {
        let pattern_code = pattern.encode_to();
        // check whether the catalog graph has the newly generate pattern
        if let Some(&pattern_index) = self.pattern_locate_map.get(&pattern_code) {
            (true, pattern_index)
        } else {
            let pattern_vertices_num = pattern.get_vertices_num();
            let pattern_index = self.store.add_node(PatternWeight {
                pattern,
                count: 0,
                best_approach: None,
                out_extend_map: HashMap::new(),
            });
            self.pattern_locate_map
                .insert(pattern_code, pattern_index);
            if pattern_vertices_num == 1 {
                self.entries.push(pattern_index);
            }
            (false, pattern_index)
        }
    }

    fn add_extend_step(
        &mut self, extend_step: ExtendStep, pre_pattern_index: NodeIndex, target_pattern_index: NodeIndex,
        target_vertex_rank: PatternId,
    ) -> (bool, Approach) {
        let extend_step_code = extend_step.encode_to();
        if let Some(approach) = self
            .get_pattern_weight(pre_pattern_index)
            .and_then(|pattern_weight| {
                pattern_weight
                    .out_extend_map
                    .get(&extend_step_code)
            })
        {
            (true, *approach)
        } else {
            let approach_index = self.store.add_edge(
                pre_pattern_index,
                target_pattern_index,
                ApproachWeight::ExtendStep(ExtendWeight {
                    extend_step,
                    target_vertex_rank,
                    adjacency_count: 0,
                    intersect_count: 0,
                }),
            );
            let approach = Approach::new(pre_pattern_index, target_pattern_index, approach_index);
            self.get_pattern_weight_mut(pre_pattern_index)
                .unwrap()
                .add_out_extend(extend_step_code, approach);
            (false, approach)
        }
    }
}

/// Methods for accessing some fields of Catalogue
impl Catalogue {
    pub fn get_patterns_num(&self) -> usize {
        self.store.node_count()
    }

    pub fn get_approaches_num(&self) -> usize {
        self.store.edge_count()
    }

    pub fn get_pattern_index(&self, pattern_code: &Vec<u8>) -> Option<NodeIndex> {
        self.pattern_locate_map
            .get(pattern_code)
            .cloned()
    }

    pub fn get_pattern_weight(&self, pattern_index: NodeIndex) -> Option<&PatternWeight> {
        self.store.node_weight(pattern_index)
    }

    pub fn get_pattern_weight_mut(&mut self, pattern_index: NodeIndex) -> Option<&mut PatternWeight> {
        self.store.node_weight_mut(pattern_index)
    }

    pub fn get_approach_weight(&self, approach_index: EdgeIndex) -> Option<&ApproachWeight> {
        self.store.edge_weight(approach_index)
    }

    pub fn get_approach_weight_mut(&mut self, approach_index: EdgeIndex) -> Option<&mut ApproachWeight> {
        self.store.edge_weight_mut(approach_index)
    }

    pub fn get_extend_weight(&self, approach_index: EdgeIndex) -> Option<&ExtendWeight> {
        self.get_approach_weight(approach_index)
            .and_then(|approach_weight| approach_weight.get_extend_weight())
    }

    pub fn get_extend_weight_mut(&mut self, approach_index: EdgeIndex) -> Option<&mut ExtendWeight> {
        self.get_approach_weight_mut(approach_index)
            .and_then(|approach_weight| approach_weight.get_extend_weight_mut())
    }

    pub fn get_join_weight(&self, approach_index: EdgeIndex) -> Option<&JoinWeight> {
        self.get_approach_weight(approach_index)
            .and_then(|approach_weight| approach_weight.get_join_weight())
    }

    pub fn get_join_weight_mut(&mut self, approach_index: EdgeIndex) -> Option<&mut JoinWeight> {
        self.get_approach_weight_mut(approach_index)
            .and_then(|approach_weight| approach_weight.get_join_weight_mut())
    }

    pub fn entries_iter(&self) -> DynIter<NodeIndex> {
        Box::new(self.entries.iter().cloned())
    }

    pub fn pattern_indices_iter(&self) -> DynIter<NodeIndex> {
        Box::new(self.store.node_indices())
    }

    /// Get a pattern's all out connection
    pub fn pattern_out_approaches_iter(&self, pattern_index: NodeIndex) -> DynIter<Approach> {
        Box::new(
            self.store
                .edges_directed(pattern_index, Direction::Outgoing)
                .map(move |edge| Approach::from(edge)),
        )
    }

    /// Get a pattern's all incoming connection
    pub fn pattern_in_approaches_iter(&self, pattern_index: NodeIndex) -> DynIter<Approach> {
        Box::new(
            self.store
                .edges_directed(pattern_index, Direction::Incoming)
                .map(move |edge| Approach::from(edge)),
        )
    }

    pub fn get_plan_space(&self) -> &PatMatPlanSpace {
        &self.plan_space
    }

    pub fn set_plan_space(&mut self, plan_space: PatMatPlanSpace) {
        self.plan_space = plan_space;
    }

    pub fn set_pattern_count(&mut self, pattern_index: NodeIndex, count: usize) {
        if let Some(pattern_weight) = self.get_pattern_weight_mut(pattern_index) {
            pattern_weight.set_count(count)
        }
    }

    pub fn set_pattern_best_approach(&mut self, pattern_index: NodeIndex, best_approach: Approach) {
        if let Some(pattern_weight) = self.get_pattern_weight_mut(pattern_index) {
            pattern_weight.set_best_approach(best_approach)
        }
    }

    pub fn set_extend_count_infos(&mut self, pattern_index: NodeIndex) {
        if let Some(extend_approaches) = self
            .get_pattern_weight(pattern_index)
            .map(|pattern_weight| {
                pattern_weight
                    .out_extend_iter()
                    .collect::<Vec<Approach>>()
            })
        {
            let mut adjacency_count_map = HashMap::new();
            for extend_approach in extend_approaches {
                let extend_step = self
                    .get_extend_weight(extend_approach.get_approach_index())
                    .unwrap()
                    .get_extend_step();
                let target_pattern_count = self
                    .get_pattern_weight(extend_approach.get_target_pattern_index())
                    .unwrap()
                    .get_count();
                if extend_step.get_extend_edges_num() == 1 {
                    adjacency_count_map
                        .insert(extend_step.iter().next().unwrap().clone(), target_pattern_count);
                    let extend_weight_mut = self
                        .get_extend_weight_mut(extend_approach.get_approach_index())
                        .unwrap();
                    extend_weight_mut.set_adjacency_count(target_pattern_count);
                    extend_weight_mut.set_intersect_count(0);
                } else {
                    let target_vertex_label = extend_step.get_target_vertex_label();
                    let extend_edges = extend_step.get_extend_edges();
                    let adjacency_count = extend_edges
                        .iter()
                        .map(|extend_edge| adjacency_count_map.get(&extend_edge).unwrap())
                        .sum::<usize>();
                    let mut min_intersect_count = usize::MAX;
                    let mut extend_step_with_min_count = None;
                    for i in 0..extend_edges.len() {
                        let mut sub_extend_edges = extend_edges.clone();
                        let rm_extend_edge = sub_extend_edges.remove(i);
                        let sub_extend_step = ExtendStep::new(target_vertex_label, sub_extend_edges);
                        let sub_extend_step_code = sub_extend_step.encode_to();
                        let sub_extend_approach = self
                            .get_pattern_weight(pattern_index)
                            .and_then(|pattern_weight| {
                                pattern_weight.get_extend_approach(&sub_extend_step_code)
                            })
                            .unwrap();
                        let sub_extend_weight = self
                            .get_extend_weight(sub_extend_approach.get_approach_index())
                            .unwrap();
                        let sub_intersect_count = sub_extend_weight.get_intersect_count();
                        let sub_target_pattern_count = self
                            .get_pattern_weight(sub_extend_approach.get_target_pattern_index())
                            .unwrap()
                            .get_count();
                        if i == 0 || sub_intersect_count + sub_target_pattern_count < min_intersect_count {
                            min_intersect_count = sub_intersect_count + sub_target_pattern_count;
                            extend_step_with_min_count = Some(
                                sub_extend_weight
                                    .get_extend_step()
                                    .clone()
                                    .add_extend_edge(rm_extend_edge),
                            );
                        }
                    }
                    let extend_weight_mut = self
                        .get_extend_weight_mut(extend_approach.get_approach_index())
                        .unwrap();
                    extend_weight_mut.set_adjacency_count(adjacency_count);
                    extend_weight_mut.set_intersect_count(min_intersect_count);
                    extend_weight_mut.set_extend_step(extend_step_with_min_count.unwrap());
                }
            }
        }
    }

    pub fn estimate_pattern_count(&mut self, pattern: &Pattern) -> usize {
        let pattern_code = pattern.encode_to();
        // If the pattern is stored in the catalog, directly return its count
        if let Some(pattern_index) = self.get_pattern_index(&pattern_code) {
            self.get_pattern_weight(pattern_index)
                .unwrap()
                .get_count()
        }
        // Check whether the Pattern Count info is historically cached
        else if let Some(&patten_count) = self.pattern_count_map.get(&pattern_code) {
            patten_count
        } else {
            // pattern combinations stores
            // (sub pattern0, sub pattern1, intersect pattern, removed edge0, removed edge1)
            let mut pattern_combinations = vec![];
            // Enumarate all (edge0, edge1) pairs to find possible pattern combination
            let edges: Vec<PatternEdge> = pattern.edges_iter().cloned().collect();
            for i in 0..edges.len() {
                for j in (i + 1)..edges.len() {
                    let edge_0 = edges.get(i).unwrap();
                    let edge_1 = edges.get(j).unwrap();
                    // legal sub pattern has to be fully connected after remove an edge
                    if let (Some(sub_pattern_0), Some(sub_pattern_1)) = (
                        pattern.clone().remove_edge(edge_0.get_id()),
                        pattern.clone().remove_edge(edge_1.get_id()),
                    ) {
                        if let Some(intersect_pattern) = sub_pattern_0
                            .clone()
                            .remove_edge(edge_1.get_id())
                        {
                            pattern_combinations.push((
                                sub_pattern_0,
                                sub_pattern_1,
                                intersect_pattern,
                                edge_0.clone(),
                                edge_1.clone(),
                            ));
                        }
                    }
                }
            }
            // Iterate over all possible pattern combinations to get average estimate pattern count
            // estimate pattern count = count(sub pattern 0) * count(sub pattern 1) / count(intersect pattern)
            let mut estimate_count_sum = 0;
            for (sub_pattern_0, sub_pattern_1, intersect_pattern, edge_0, edge_1) in
                pattern_combinations.iter()
            {
                // Get these sub patterns' counts recursively
                let sub_pattern_0_count = self.estimate_pattern_count(sub_pattern_0);
                let sub_pattern_1_count = self.estimate_pattern_count(sub_pattern_1);
                let mut intersect_pattern_count = self.estimate_pattern_count(intersect_pattern);
                // Deal with the situation that the intersect pattern is disconnected
                // the intersect pattern is disconnected if
                // 1. its vertex number > 1
                // 2. edge0 and edge1 shares a common vertex
                if intersect_pattern.get_vertices_num() > 1 {
                    if let Some(common_vertex) = get_common_vertex_of_edges(edge_0, edge_1) {
                        let common_vertex_count =
                            self.estimate_pattern_count(&Pattern::from(common_vertex));
                        // intersect pattern's count should multiply the common vertex's count
                        intersect_pattern_count *= common_vertex_count;
                    }
                }
                // Deal with the situation that intersect pattern count is 0
                let estimate_count = if intersect_pattern_count == 0 {
                    sub_pattern_0_count * sub_pattern_1_count
                } else {
                    sub_pattern_0_count * sub_pattern_1_count / intersect_pattern_count
                };
                estimate_count_sum += estimate_count;
            }
            // If pattern combinations's len is 0, which means
            // 1. pattern's edge number <= 1
            // 2. such pattern's count info should be stored in the catalog
            // 3. therefore, its count is estimated as 0
            let pattern_count = if pattern_combinations.len() == 0 {
                0
            } else {
                estimate_count_sum / pattern_combinations.len()
            };
            self.pattern_count_map
                .insert(pattern_code, pattern_count);
            pattern_count
        }
    }

    pub fn print_count_info(&self) {
        for node_index in self.store.node_indices() {
            println!(
                "{:?}: {:?}",
                node_index,
                self.store
                    .node_weight(node_index)
                    .unwrap()
                    .get_count()
            );
        }
    }
}

fn get_common_vertex_of_edges(edge_0: &PatternEdge, edge_1: &PatternEdge) -> Option<PatternVertex> {
    let start_vertex_0 = edge_0.get_start_vertex();
    let end_vertex_0 = edge_0.get_end_vertex();
    let start_vertex_1 = edge_1.get_start_vertex();
    let end_vertex_1 = edge_1.get_end_vertex();
    if start_vertex_0 == start_vertex_1 || start_vertex_0 == end_vertex_1 {
        Some(start_vertex_0)
    } else if end_vertex_0 == start_vertex_1 || end_vertex_0 == end_vertex_1 {
        Some(end_vertex_0)
    } else {
        None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CataTopo {
    pattern_map: BTreeMap<NodeIndex, PatternWeight>,
    approach_map: BTreeMap<Approach, ApproachWeight>,
}

impl From<&Catalogue> for CataTopo {
    fn from(catalog: &Catalogue) -> Self {
        let mut pattern_map = BTreeMap::new();
        let mut approach_map = BTreeMap::new();
        for approach_index in catalog.store.edge_indices() {
            let (src_pattern_index, target_pattern_index) = catalog
                .store
                .edge_endpoints(approach_index)
                .unwrap();
            let approach = Approach::new(src_pattern_index, target_pattern_index, approach_index);
            let approach_weight = catalog
                .get_approach_weight(approach_index)
                .unwrap()
                .clone();
            approach_map.insert(approach, approach_weight);
            pattern_map.entry(src_pattern_index).or_insert(
                catalog
                    .get_pattern_weight(src_pattern_index)
                    .unwrap()
                    .clone(),
            );
            pattern_map
                .entry(target_pattern_index)
                .or_insert(
                    catalog
                        .get_pattern_weight(target_pattern_index)
                        .unwrap()
                        .clone(),
                );
        }
        CataTopo { pattern_map, approach_map }
    }
}

impl From<CataTopo> for Catalogue {
    fn from(cata_topo: CataTopo) -> Self {
        let mut catalog = Catalogue::default();
        let mut old_new_pattern_index_map = HashMap::new();
        for (pattern_index, pattern_weight) in cata_topo.pattern_map.into_iter() {
            let pattern = pattern_weight.get_pattern();
            let pattern_code = pattern.encode_to();
            let pattern_vertices_num = pattern.get_vertices_num();
            let new_pattern_index = catalog.store.add_node(pattern_weight);
            catalog
                .pattern_locate_map
                .insert(pattern_code, new_pattern_index);
            if pattern_vertices_num == 1 {
                catalog.entries.push(new_pattern_index);
            }
            old_new_pattern_index_map.insert(pattern_index, new_pattern_index);
        }
        for (approach, approach_weight) in cata_topo.approach_map.into_iter() {
            let src_pattern_index = *old_new_pattern_index_map
                .get(&approach.get_src_pattern_index())
                .unwrap();
            let target_pattern_index = *old_new_pattern_index_map
                .get(&approach.get_target_pattern_index())
                .unwrap();
            catalog
                .store
                .add_edge(src_pattern_index, target_pattern_index, approach_weight);
        }
        catalog
    }
}

impl Serialize for Catalogue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let cata_topo = CataTopo::from(self);
        cata_topo.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Catalogue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct CatalogueVistor;
        impl<'de> Visitor<'de> for CatalogueVistor {
            type Value = Catalogue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a valid Catalogue")
            }

            fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let cata_topo = CataTopo::deserialize(deserializer)?;
                Ok(Catalogue::from(cata_topo))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let cata_topo: CataTopo = seq
                    .next_element()?
                    .ok_or(serde::de::Error::custom("Invalid CataTopo Bincode"))?;
                Ok(Catalogue::from(cata_topo))
            }
        }
        deserializer.deserialize_newtype_struct("Catalogue", CatalogueVistor)
    }
}

impl Catalogue {
    pub fn export<P: AsRef<Path>>(&self, path: P) -> BincodeResult<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        serialize_into(&mut writer, self)
    }

    pub fn import<P: AsRef<Path>>(path: P) -> BincodeResult<Self> {
        let mut reader = BufReader::new(File::open(path)?);
        deserialize_from(&mut reader)
    }
}

#[derive(Debug, Clone)]
pub struct TableRow {
    src_pattern: Pattern,
    target_pattern: Pattern,
    extend_step: ExtendStep,
    extend_counts: Vec<usize>,
    pattern_count: usize,
}

impl TableRow {
    pub fn new(
        src_pattern: Pattern, target_pattern: Pattern, extend_step: ExtendStep, extend_counts: Vec<usize>,
        pattern_count: usize,
    ) -> TableRow {
        TableRow { src_pattern, target_pattern, extend_step, extend_counts, pattern_count }
    }

    pub fn get_src_pattern(&self) -> &Pattern {
        &self.src_pattern
    }

    pub fn get_target_pattern(&self) -> &Pattern {
        &self.target_pattern
    }

    pub fn get_extend_step(&self) -> &ExtendStep {
        &self.extend_step
    }

    pub fn get_extend_counts(&self) -> Vec<usize> {
        self.extend_counts.clone()
    }

    pub fn get_pattern_count(&self) -> usize {
        self.pattern_count
    }

    pub fn set_extend_counts(&mut self, extend_counts: Vec<usize>) {
        self.extend_counts = extend_counts
    }

    pub fn set_pattern_count(&mut self, pattern_count: usize) {
        self.pattern_count = pattern_count
    }
}

#[derive(Debug, Clone, Default)]
pub struct TableLogue {
    rows: Vec<TableRow>,
}

impl TableLogue {
    pub fn iter(&self) -> DynIter<&TableRow> {
        Box::new(self.rows.iter())
    }

    pub fn iter_mut(&mut self) -> DynIter<&mut TableRow> {
        Box::new(self.rows.iter_mut())
    }

    pub fn add_row(&mut self, row: TableRow) {
        self.rows.push(row)
    }

    pub fn get_patterns_num(&self) -> usize {
        let mut pattern_code_set = HashSet::new();
        for (src_pattern, target_pattern) in self
            .rows
            .iter()
            .map(|row| (&row.src_pattern, &row.target_pattern))
        {
            pattern_code_set.insert(src_pattern.encode_to());
            pattern_code_set.insert(target_pattern.encode_to());
        }
        pattern_code_set.len()
    }

    pub fn get_extends_num(&self) -> usize {
        self.rows.len()
    }
}

impl TableLogue {
    pub fn naive_init(
        vertex_labels: Vec<PatternLabelId>, edge_labels: Vec<PatternLabelId>, pattern_size_limit: usize,
    ) -> TableLogue {
        let mut table_logue = TableLogue::default();
        let mut queue = VecDeque::new();
        let mut pattern_code_set = HashSet::new();
        let adjacent_edges: Vec<(PatternLabelId, PatternDirection)> = edge_labels
            .iter()
            .map(|edge_id| (*edge_id, PatternDirection::Out))
            .chain(
                edge_labels
                    .iter()
                    .map(|edge_id| (*edge_id, PatternDirection::In)),
            )
            .collect();
        for &vertex_label in vertex_labels.iter() {
            let new_pattern = Pattern::from(PatternVertex::new(0, vertex_label));
            if pattern_code_set.insert(new_pattern.encode_to()) {
                queue.push_back(new_pattern);
            }
        }
        while let Some(relaxed_pattern) = queue.pop_front() {
            if relaxed_pattern.get_vertices_num() >= pattern_size_limit {
                continue;
            }
            let mut extend_steps = vec![];
            let adjacent_edges_arranges =
                get_vec_arranges(&adjacent_edges, relaxed_pattern.get_vertices_num());
            for &vertex_label in vertex_labels.iter() {
                for adjacent_edges in adjacent_edges_arranges.iter() {
                    let extend_edges: Vec<ExtendEdge> = adjacent_edges
                        .iter()
                        .enumerate()
                        .map(|(i, &(edge_label, edge_dir))| ExtendEdge::new(i, edge_label, edge_dir))
                        .collect();
                    for extend_edges in get_subsets(extend_edges, |_, _| false) {
                        let extend_step = ExtendStep::new(vertex_label, extend_edges);
                        extend_steps.push(extend_step);
                    }
                }
            }
            for extend_step in extend_steps {
                let new_pattern = relaxed_pattern.extend(&extend_step).unwrap();
                let table_row = TableRow::new(
                    relaxed_pattern.clone(),
                    new_pattern.clone(),
                    extend_step.clone(),
                    vec![0; extend_step.get_extend_edges_num()],
                    1,
                );
                table_logue.add_row(table_row);
                if pattern_code_set.insert(new_pattern.encode_to()) {
                    queue.push_back(new_pattern)
                }
            }
        }
        table_logue
    }

    pub fn from_meta(pattern_meta: &PatternMeta, pattern_size_limit: usize) -> TableLogue {
        let mut table_logue = TableLogue::default();
        let mut queue = VecDeque::new();
        let mut pattern_code_set = HashSet::new();
        for vertex_label in pattern_meta.vertex_label_ids_iter() {
            let new_pattern = Pattern::from(PatternVertex::new(0, vertex_label));
            if pattern_code_set.insert(new_pattern.encode_to()) {
                queue.push_back(new_pattern);
            }
        }
        while let Some(relaxed_pattern) = queue.pop_front() {
            if relaxed_pattern.get_vertices_num() >= pattern_size_limit {
                continue;
            }
            let extend_steps = relaxed_pattern.get_extend_steps(pattern_meta, pattern_size_limit);
            for extend_step in extend_steps.iter() {
                let new_pattern = relaxed_pattern.extend(extend_step).unwrap();
                let table_row = TableRow::new(
                    relaxed_pattern.clone(),
                    new_pattern.clone(),
                    extend_step.clone(),
                    vec![0; extend_step.get_extend_edges_num()],
                    1,
                );
                table_logue.add_row(table_row);
                if pattern_code_set.insert(new_pattern.encode_to()) {
                    queue.push_back(new_pattern)
                }
            }
        }
        table_logue
    }
}

fn get_vec_arranges<T: Clone>(vector: &Vec<T>, len: usize) -> Vec<Vec<T>> {
    let mut vec_arranges = vec![];
    let mut stack = vec![];
    for element in vector.iter() {
        stack.push(vec![element.clone()]);
    }
    while let Some(vec_arrange) = stack.pop() {
        if vec_arrange.len() == len {
            vec_arranges.push(vec_arrange);
        } else {
            for element in vector.iter() {
                let mut new_vec_arrange = vec_arrange.clone();
                new_vec_arrange.push(element.clone());
                stack.push(new_vec_arrange);
            }
        }
    }
    vec_arranges
}

#[cfg(test)]
mod test {
    use crate::{catalogue::pattern_meta::PatternMeta, plan::meta::Schema, JsonIO};
    use std::fs::File;

    use super::{get_vec_arranges, Catalogue, TableLogue};

    #[test]
    fn test_table_logue_struct() {
        let ldbc_schema_file = match File::open("resource/ldbc_schema_broad.json") {
            Ok(file) => file,
            Err(_) => match File::open("core/resource/ldbc_schema_broad.json") {
                Ok(file) => file,
                Err(_) => File::open("../core/resource/ldbc_schema_broad.json").unwrap(),
            },
        };
        let pattern_meta = PatternMeta::from(Schema::from_json(ldbc_schema_file).unwrap());
        let table_logue = TableLogue::from_meta(&pattern_meta, 4);
        let g_logue = Catalogue::build_from_meta(&pattern_meta, 4, 4);
        assert_eq!(table_logue.get_patterns_num() + 5, g_logue.get_patterns_num());
        assert_eq!(table_logue.get_extends_num(), g_logue.store.edge_count());
    }

    #[test]
    fn test_get_vec_arranges() {
        let vector = vec![1, 2, 3];
        println!("{:?}", get_vec_arranges(&vector, 3));
    }
}
