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
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use bincode::Result as BincodeResult;
use bincode::{deserialize_from, serialize_into};
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use petgraph::graph::{EdgeIndex, EdgeReference, Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use serde::de::Visitor;
use serde::{Deserialize, Serialize};

use crate::catalogue::extend_step::{get_subsets, DefiniteExtendStep, ExtendEdge, ExtendStep};
use crate::catalogue::pattern::{Adjacency, Pattern, PatternVertex};
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::{query_params, DynIter, PatternDirection, PatternId, PatternLabelId};
use crate::error::{IrError, IrResult};

static ALPHA: f64 = 0.5;
static BETA: f64 = 0.5;
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
    /// Join with which pattern
    pattern_index: NodeIndex,
}

impl JoinWeight {
    pub fn get_pattern_index(&self) -> NodeIndex {
        self.pattern_index
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
    Join(JoinWeight),
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
        if let ApproachWeight::Join(_) = self {
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
        if let ApproachWeight::Join(join_weight) = self {
            Some(join_weight)
        } else {
            None
        }
    }

    pub fn get_join_weight_mut(&mut self) -> Option<&mut JoinWeight> {
        if let ApproachWeight::Join(join_weight) = self {
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

#[derive(Debug, Clone, Default)]
pub struct Catalogue {
    /// Catalog Graph
    store: Graph<PatternWeight, ApproachWeight>,
    /// Key: pattern code, Value: Node(Vertex) Index
    /// Usage: use a pattern code to uniquely identify a pattern in catalog graph
    pattern_locate_map: HashMap<Vec<u8>, NodeIndex>,
    /// Store the extend steps already found between the src pattern and target pattern
    /// - Used to avoid add equivalent extend steps between two patterns
    // extend_step_comparator_map: HashMap<(Vec<u8>, Vec<u8>), BTreeMap<ExtendStepComparator, Approach>>,
    /// Those patterns with size 1 are the entries of catalogue
    /// - Stores entries with their NodeIndex and PatternLabelId
    entries: Vec<NodeIndex>,
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
            let new_pattern_code = new_pattern.encode_to();
            let new_pattern_index = catalog.store.add_node(PatternWeight {
                pattern: new_pattern.clone(),
                count: 0,
                best_approach: None,
                out_extend_map: HashMap::new(),
            });
            catalog
                .pattern_locate_map
                .insert(new_pattern_code.clone(), new_pattern_index);
            catalog.entries.push(new_pattern_index);
            queue.push_back((new_pattern, new_pattern_code, new_pattern_index));
        }
        while let Some((relaxed_pattern, relaxed_pattern_code, relaxed_pattern_index)) = queue.pop_front() {
            // Filter out those patterns beyond the pattern size limitation
            if relaxed_pattern.get_vertices_num() >= pattern_size_limit {
                continue;
            }
            // Find possible extend steps of the relaxed pattern
            let extend_steps = relaxed_pattern.get_extend_steps(pattern_meta, same_label_vertex_limit);
            for extend_step in extend_steps.iter() {
                // relaxed pattern + extend step to get new pattern
                // let extend_step_comparator: ExtendStepComparator =
                //     new_extend_step_comparator(extend_step, &relaxed_pattern);
                let new_pattern = relaxed_pattern.extend(extend_step).unwrap();
                let new_pattern_code = new_pattern.encode_to();
                // check whether the new pattern existed in the catalog graph
                let new_pattern_index = if let Some(&pattern_index) = catalog
                    .pattern_locate_map
                    .get(&new_pattern_code)
                {
                    pattern_index
                } else {
                    catalog.store.add_node(PatternWeight {
                        pattern: new_pattern.clone(),
                        count: 0,
                        best_approach: None,
                        out_extend_map: HashMap::new(),
                    })
                };
                // Get all extend steps(comparators) between the relaxed pattern and new pattern
                // let extend_step_comparators = catalog
                //     .extend_step_comparator_map
                //     .entry((relaxed_pattern_code.clone(), new_pattern_code.clone()))
                //     .or_default();
                // If there exists a equivalent extend step added to the catalog, continue
                let approach_index = catalog.store.add_edge(
                    relaxed_pattern_index,
                    new_pattern_index,
                    ApproachWeight::ExtendStep(ExtendWeight {
                        extend_step: extend_step.clone(),
                        target_vertex_rank: new_pattern
                            .get_vertex_rank(new_pattern.get_max_vertex_id())
                            .unwrap(),
                        adjacency_count: 0,
                        intersect_count: 0,
                    }),
                );
                if !catalog
                    .pattern_locate_map
                    .contains_key(&new_pattern_code)
                {
                    catalog
                        .pattern_locate_map
                        .insert(new_pattern_code.clone(), new_pattern_index);
                    queue.push_back((new_pattern, new_pattern_code, new_pattern_index));
                }
                let approach = Approach::new(relaxed_pattern_index, new_pattern_index, approach_index);
                catalog
                    .get_pattern_weight_mut(relaxed_pattern_index)
                    .unwrap()
                    .add_out_extend(extend_step.encode_to(), approach);
            }
        }
        catalog
    }

    /// Build a catalog from a pattern dedicated for its optimization
    pub fn build_from_pattern(pattern: &Pattern) -> Catalogue {
        // Empty catalog
        let mut catalog = Catalogue::default();
        // Update the empty catalog by the pattern to add necessary optimization info
        catalog.update_catalog_by_pattern(pattern);
        catalog
    }

    /// Update the current catalog by given pattern
    /// Aims to add neccesary optimization info of the pattern
    pub fn update_catalog_by_pattern(&mut self, pattern: &Pattern) {
        // Use BFS to get all possible extend approaches to get the pattern
        let mut queue = VecDeque::new();
        // record sub patterns that are relaxed (by vertex id set)
        let mut relaxed_patterns = BTreeSet::new();
        // one-vertex pattern is the starting point of the BFS
        for vertex in pattern.vertices_iter() {
            let new_pattern = Pattern::from(*vertex);
            let new_pattern_code = new_pattern.encode_to();
            let new_pattern_index =
                if let Some(pattern_index) = self.pattern_locate_map.get(&new_pattern_code) {
                    *pattern_index
                } else {
                    let pattern_index = self.store.add_node(PatternWeight {
                        pattern: new_pattern.clone(),
                        count: 0,
                        best_approach: None,
                        out_extend_map: HashMap::new(),
                    });
                    self.pattern_locate_map
                        .insert(new_pattern_code.clone(), pattern_index);
                    self.entries.push(pattern_index);
                    pattern_index
                };
            queue.push_back((new_pattern, new_pattern_code, new_pattern_index));
        }
        while let Some((relaxed_pattern, relaxed_pattern_code, relaxed_pattern_index)) = queue.pop_front() {
            // check whether the current pattern is relaxed or not
            let relaxed_pattern_vertices: BTreeSet<PatternId> = relaxed_pattern
                .vertices_iter()
                .map(|v| v.get_id())
                .collect();
            if relaxed_patterns.contains(&relaxed_pattern_vertices) {
                continue;
            } else {
                relaxed_patterns.insert(relaxed_pattern_vertices.clone());
            }
            // Store already added target vertex of extend step
            let mut added_vertices_ids = BTreeSet::new();
            for vertex_id in relaxed_pattern
                .vertices_iter()
                .map(|v| v.get_id())
            {
                for adj_vertex_id in pattern
                    .adjacencies_iter(vertex_id)
                    .map(|adj| adj.get_adj_vertex().get_id())
                {
                    // If ralaxed pattern contains adj vertex or it is already added as target vertex
                    // ignore the vertex
                    if relaxed_pattern_vertices.contains(&adj_vertex_id)
                        || added_vertices_ids.contains(&adj_vertex_id)
                    {
                        continue;
                    }
                    // the adj_vertex is regarded as target vertex
                    // back link to the relaxed pattern to         find all edges to be added
                    let final_back_links = pattern
                        .adjacencies_iter(adj_vertex_id)
                        .filter(|adj| relaxed_pattern_vertices.contains(&adj.get_adj_vertex().get_id()))
                        .collect::<Vec<&Adjacency>>();
                    let back_links_collection = get_subsets(final_back_links.clone(), |_, _| false);
                    for back_links in back_links_collection {
                        // Get extend edges to build extend step
                        let extend_edges: Vec<ExtendEdge> = back_links
                            .iter()
                            .map(|adj| {
                                ExtendEdge::new(
                                    relaxed_pattern
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
                        let extend_step = ExtendStep::new(target_v_label, extend_edges);
                        // let extend_step_comparator: ExtendStepComparator =
                        //     new_extend_step_comparator(&extend_step, &relaxed_pattern);
                        // generate the new pattern with add_edges(extend edges)
                        let new_pattern = relaxed_pattern
                            .extend_by_edges(
                                back_links
                                    .iter()
                                    .map(|adj| pattern.get_edge(adj.get_edge_id()).unwrap()),
                            )
                            .unwrap();
                        let new_pattern_code = new_pattern.encode_to();
                        // check whether the catalog graph has the newly generate pattern
                        let new_pattern_index =
                            if let Some(&pattern_index) = self.pattern_locate_map.get(&new_pattern_code) {
                                pattern_index
                            } else {
                                self.store.add_node(PatternWeight {
                                    pattern: new_pattern.clone(),
                                    count: 0,
                                    best_approach: None,
                                    out_extend_map: HashMap::new(),
                                })
                            };
                        // check whether the extend step exists in the catalog graph or not
                        // Get all extend steps(comparators) between the relaxed pattern and new pattern
                        // let extend_step_comparators = self
                        //     .extend_step_comparator_map
                        //     .entry((relaxed_pattern_code.clone(), new_pattern_code.clone()))
                        //     .or_default();
                        //
                        let approach_index = self.store.add_edge(
                            relaxed_pattern_index,
                            new_pattern_index,
                            ApproachWeight::ExtendStep(ExtendWeight {
                                extend_step: extend_step.clone(),
                                target_vertex_rank: new_pattern
                                    .get_vertex_rank(adj_vertex_id)
                                    .unwrap(),
                                adjacency_count: 0,
                                intersect_count: 0,
                            }),
                        );
                        if !self
                            .pattern_locate_map
                            .contains_key(&new_pattern_code)
                        {
                            self.pattern_locate_map
                                .insert(new_pattern_code.clone(), new_pattern_index);
                        }
                        let approach =
                            Approach::new(relaxed_pattern_index, new_pattern_index, approach_index);
                        self.get_pattern_weight_mut(relaxed_pattern_index)
                            .unwrap()
                            .add_out_extend(extend_step.encode_to(), approach.clone());
                        if back_links == final_back_links {
                            added_vertices_ids.insert(adj_vertex_id);
                            queue.push_back((new_pattern, new_pattern_code, new_pattern_index));
                        }
                    }
                }
            }
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
                        if sub_intersect_count + sub_target_pattern_count < min_intersect_count {
                            min_intersect_count = sub_intersect_count + target_pattern_count;
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
}

/// Methods for Pattern to generate pb Logical plan of pattern matching
impl Pattern {
    /// Generate a naive extend based pattern match plan
    pub fn generate_simple_extend_match_plan(&self) -> IrResult<pb::LogicalPlan> {
        let mut trace_pattern = self.clone();
        let mut definite_extend_steps = vec![];
        while trace_pattern.get_vertices_num() > 1 {
            let mut all_vertex_ids: Vec<PatternId> = trace_pattern
                .vertices_iter()
                .map(|v| v.get_id())
                .collect();
            // Sort the vertices by order/incoming order/ out going order
            // Vertex with larger degree will be extended later
            all_vertex_ids.sort_by(|&v1_id, &v2_id| {
                let v1_has_predicate = trace_pattern
                    .get_vertex_predicate(v1_id)
                    .is_some();
                let v2_has_predicate = trace_pattern
                    .get_vertex_predicate(v2_id)
                    .is_some();
                if v1_has_predicate && !v2_has_predicate {
                    Ordering::Greater
                } else if !v1_has_predicate && v2_has_predicate {
                    Ordering::Less
                } else {
                    let v1_edges_predicate_num = trace_pattern
                        .adjacencies_iter(v1_id)
                        .filter(|adj| {
                            trace_pattern
                                .get_edge_predicate(adj.get_edge_id())
                                .is_some()
                        })
                        .count();
                    let v2_edges_predicate_num = trace_pattern
                        .adjacencies_iter(v2_id)
                        .filter(|adj| {
                            trace_pattern
                                .get_edge_predicate(adj.get_edge_id())
                                .is_some()
                        })
                        .count();
                    if v1_edges_predicate_num > v2_edges_predicate_num {
                        Ordering::Greater
                    } else if v2_edges_predicate_num > v1_edges_predicate_num {
                        Ordering::Less
                    } else {
                        let degree_order = trace_pattern
                            .get_vertex_degree(v1_id)
                            .cmp(&trace_pattern.get_vertex_degree(v2_id));
                        if let Ordering::Equal = degree_order {
                            trace_pattern
                                .get_vertex_out_degree(v1_id)
                                .cmp(&trace_pattern.get_vertex_out_degree(v2_id))
                        } else {
                            degree_order
                        }
                    }
                }
            });
            let select_vertex_id = *all_vertex_ids.first().unwrap();
            let definite_extend_step =
                DefiniteExtendStep::from_target_pattern(&trace_pattern, select_vertex_id).unwrap();
            definite_extend_steps.push(definite_extend_step);
            trace_pattern.remove_vertex(select_vertex_id);
        }
        definite_extend_steps.push(trace_pattern.try_into()?);
        build_logical_plan(self, definite_extend_steps)
    }

    /// Generate an optimized extend based pattern match plan
    /// Current implementation is Top-Down Greedy method
    pub fn generate_optimized_match_plan_greedily(&self, catalog: &Catalogue) -> IrResult<pb::LogicalPlan> {
        let pattern_code = self.encode_to();
        // locate the pattern node in the catalog graph
        if let Some(node_index) = catalog.get_pattern_index(&pattern_code) {
            let mut trace_pattern = self.clone();
            let mut trace_pattern_index = node_index;
            let mut trace_pattern_weight = catalog
                .get_pattern_weight(trace_pattern_index)
                .unwrap();
            let mut definite_extend_steps = vec![];
            while trace_pattern.get_vertices_num() > 1 {
                let mut all_extend_approaches: Vec<Approach> = catalog
                    .pattern_in_approaches_iter(trace_pattern_index)
                    .filter(|approach| {
                        catalog
                            .get_approach_weight(approach.get_approach_index())
                            .unwrap()
                            .is_extend()
                    })
                    .collect();
                // use the extend step with the lowerst estimated cost
                sort_extend_approaches(
                    &mut all_extend_approaches,
                    catalog,
                    &trace_pattern,
                    trace_pattern_weight,
                );
                let selected_approach = all_extend_approaches[0];
                let (pre_pattern, definite_extend_step, _) =
                    pattern_roll_back(trace_pattern, trace_pattern_index, selected_approach, catalog);
                definite_extend_steps.push(definite_extend_step);
                trace_pattern = pre_pattern;
                trace_pattern_index = selected_approach.get_src_pattern_index();
                trace_pattern_weight = catalog
                    .get_pattern_weight(trace_pattern_index)
                    .unwrap();
            }
            // transform the one-vertex pattern into definite extend step
            definite_extend_steps.push(trace_pattern.try_into()?);
            build_logical_plan(self, definite_extend_steps)
        } else {
            Err(IrError::Unsupported("Cannot Locate Pattern in the Catalogue".to_string()))
        }
    }

    pub fn generate_optimized_match_plan_recursively(
        &self, catalog: &mut Catalogue,
    ) -> IrResult<pb::LogicalPlan> {
        let pattern_code = self.encode_to();
        if let Some(pattern_index) = catalog.get_pattern_index(&pattern_code) {
            // let mut memory_map = HashMap::new();
            let (mut definite_extend_steps, _) =
                get_definite_extend_steps_recursively(catalog, pattern_index, self.clone());
            definite_extend_steps.reverse();
            build_logical_plan(self, definite_extend_steps)
        } else {
            Err(IrError::Unsupported("Cannot Locate Pattern in the Catalogue".to_string()))
        }
    }
}

pub fn get_definite_extend_steps_recursively(
    catalog: &mut Catalogue, pattern_index: NodeIndex, pattern: Pattern,
) -> (Vec<DefiniteExtendStep>, usize) {
    let pattern_weight = catalog
        .get_pattern_weight(pattern_index)
        .unwrap();
    if pattern.get_vertices_num() == 1 {
        let src_definite_extend_step = DefiniteExtendStep::try_from(pattern).unwrap();
        let cost = pattern_weight.get_count();
        return (vec![src_definite_extend_step], cost);
    } else if let Some(best_approach) = pattern_weight.get_best_approach() {
        let (pre_pattern, definite_extend_step, this_step_cost) =
            pattern_roll_back(pattern, pattern_index, best_approach, catalog);
        let pre_pattern_index = best_approach.get_src_pattern_index();
        let (mut definite_extend_steps, mut cost) =
            get_definite_extend_steps_recursively(catalog, pre_pattern_index, pre_pattern);
        definite_extend_steps.push(definite_extend_step);
        cost += this_step_cost;
        return (definite_extend_steps, cost);
    } else {
        let mut definite_extend_steps_with_min_cost = vec![];
        let mut min_cost = usize::MAX;
        let approaches: Vec<Approach> = catalog
            .pattern_in_approaches_iter(pattern_index)
            .collect();
        let mut best_approach = approaches[0];
        for approach in approaches {
            let (pre_pattern, definite_extend_step, this_step_cost) =
                pattern_roll_back(pattern.clone(), pattern_index, approach, catalog);
            let pre_pattern_index = approach.get_src_pattern_index();
            let (mut definite_extend_steps, mut cost) =
                get_definite_extend_steps_recursively(catalog, pre_pattern_index, pre_pattern);
            definite_extend_steps.push(definite_extend_step);
            cost += this_step_cost;
            if cost < min_cost {
                definite_extend_steps_with_min_cost = definite_extend_steps;
                min_cost = cost;
                best_approach = approach;
            }
        }
        catalog.set_pattern_best_approach(pattern_index, best_approach);
        return (definite_extend_steps_with_min_cost, min_cost);
    }
}

fn pattern_roll_back(
    pattern: Pattern, pattern_index: NodeIndex, approach: Approach, catalog: &Catalogue,
) -> (Pattern, DefiniteExtendStep, usize) {
    let pattern_weight = catalog
        .get_pattern_weight(pattern_index)
        .unwrap();
    let extend_weight = catalog
        .get_extend_weight(approach.get_approach_index())
        .unwrap();
    let pre_pattern_index = approach.get_src_pattern_index();
    let pre_pattern_weight = catalog
        .get_pattern_weight(pre_pattern_index)
        .unwrap();
    let this_step_cost = extend_cost_estimate(pre_pattern_weight, pattern_weight, extend_weight);
    let target_vertex_id = pattern
        .get_vertex_from_rank(extend_weight.get_target_vertex_rank())
        .unwrap()
        .get_id();
    let extend_step = extend_weight.get_extend_step();
    let edge_id_map = pattern
        .adjacencies_iter(target_vertex_id)
        .map(|adjacency| {
            (
                (
                    adjacency.get_adj_vertex().get_id(),
                    adjacency.get_edge_label(),
                    adjacency.get_direction().reverse(),
                ),
                adjacency.get_edge_id(),
            )
        })
        .collect();
    let mut pre_pattern = pattern;
    pre_pattern.remove_vertex(target_vertex_id);
    let definite_extend_step =
        DefiniteExtendStep::from_src_pattern(&pre_pattern, &extend_step, target_vertex_id, edge_id_map)
            .unwrap();
    (pre_pattern, definite_extend_step, this_step_cost)
}

fn sort_extend_approaches(
    approaches: &mut Vec<Approach>, catalog: &Catalogue, pattern: &Pattern, pattern_weight: &PatternWeight,
) {
    approaches.sort_by(|approach1, approach2| {
        let extend_weight1 = catalog
            .get_extend_weight(approach1.get_approach_index())
            .unwrap();
        let extend_weight2 = catalog
            .get_extend_weight(approach2.get_approach_index())
            .unwrap();
        let target_vertex1_has_predicate = pattern
            .get_vertex_predicate(
                pattern
                    .get_vertex_from_rank(extend_weight1.get_target_vertex_rank())
                    .unwrap()
                    .get_id(),
            )
            .is_some();
        let target_vertex2_has_predicate = pattern
            .get_vertex_predicate(
                pattern
                    .get_vertex_from_rank(extend_weight2.get_target_vertex_rank())
                    .unwrap()
                    .get_id(),
            )
            .is_some();
        if target_vertex1_has_predicate && !target_vertex2_has_predicate {
            return Ordering::Greater;
        } else if !target_vertex1_has_predicate && target_vertex2_has_predicate {
            return Ordering::Less;
        }
        let pre_pattern_weight1 = catalog
            .get_pattern_weight(approach1.get_src_pattern_index())
            .unwrap();
        let pre_pattern_weight2 = catalog
            .get_pattern_weight(approach2.get_src_pattern_index())
            .unwrap();
        extend_cost_estimate(pre_pattern_weight1, pattern_weight, extend_weight1)
            .cmp(&extend_cost_estimate(pre_pattern_weight2, pattern_weight, extend_weight2))
    });
}

/// Build logical plan for extend based pattern match plan
///             source
///           /   |    \
///           \   |    /
///            intersect
fn build_logical_plan(
    origin_pattern: &Pattern, mut definite_extend_steps: Vec<DefiniteExtendStep>,
) -> IrResult<pb::LogicalPlan> {
    let mut match_plan = pb::LogicalPlan::default();
    let mut child_offset = 1;
    let source = {
        let source_extend = match definite_extend_steps.pop() {
            Some(src_extend) => src_extend,
            None => {
                return Err(IrError::InvalidPattern(
                    "Build logical plan error: from empty extend steps!".to_string(),
                ))
            }
        };
        let source_vertex_label = source_extend.get_target_vertex_label();
        let source_vertex_id = source_extend.get_target_vertex_id();
        let source_vertex_predicate = origin_pattern
            .get_vertex_predicate(source_vertex_id)
            .cloned();
        pb::Scan {
            scan_opt: 0,
            alias: Some((source_vertex_id as i32).into()),
            params: Some(query_params(vec![source_vertex_label.into()], vec![], source_vertex_predicate)),
            idx_predicate: None,
        }
    };
    // pre_node will have some children, need a subplan
    let mut pre_node = pb::logical_plan::Node { opr: Some(source.into()), children: vec![] };
    for definite_extend_step in definite_extend_steps.into_iter().rev() {
        let edge_expands = definite_extend_step.generate_expand_operators(origin_pattern);
        let edge_expands_num = edge_expands.len();
        let edge_expands_ids: Vec<i32> = (0..edge_expands_num as i32)
            .map(|i| i + child_offset)
            .collect();
        for &i in edge_expands_ids.iter() {
            pre_node.children.push(i);
        }
        match_plan.nodes.push(pre_node);
        // if edge expand num > 1, we need a Intersect Operator
        if edge_expands_num > 1 {
            for edge_expand in edge_expands {
                let node = pb::logical_plan::Node {
                    opr: Some(edge_expand.into()),
                    children: vec![child_offset + edge_expands_num as i32],
                };
                match_plan.nodes.push(node);
            }
            let intersect = definite_extend_step.generate_intersect_operator(edge_expands_ids);
            pre_node = pb::logical_plan::Node { opr: Some(intersect.into()), children: vec![] };
            child_offset += (edge_expands_num + 1) as i32;
        } else if edge_expands_num == 1 {
            let edge_expand = edge_expands.into_iter().last().unwrap();
            pre_node = pb::logical_plan::Node { opr: Some(edge_expand.into()), children: vec![] };
            child_offset += 1;
        } else {
            return Err(IrError::InvalidPattern(
                "Build logical plan error: extend step is not source but has 0 edges".to_string(),
            ));
        }
        if let Some(filter) = definite_extend_step.generate_vertex_filter_operator(origin_pattern) {
            let filter_id = child_offset;
            pre_node.children.push(filter_id);
            match_plan.nodes.push(pre_node);
            pre_node = pb::logical_plan::Node { opr: Some(filter.into()), children: vec![] };
            child_offset += 1;
        }
    }
    pre_node.children.push(child_offset);
    match_plan.nodes.push(pre_node);

    let sink = {
        pb::Sink {
            tags: origin_pattern
                .vertices_with_tag_iter()
                .map(|v_tuple| common_pb::NameOrIdKey { key: Some((v_tuple.get_id() as i32).into()) })
                .collect(),
            sink_target: Some(pb::sink::SinkTarget {
                inner: Some(pb::sink::sink_target::Inner::SinkDefault(pb::SinkDefault {
                    id_name_mappings: vec![],
                })),
            }),
        }
    };
    match_plan
        .nodes
        .push(pb::logical_plan::Node { opr: Some(sink.into()), children: vec![] });
    Ok(match_plan)
}

// #[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
// struct ExtendEdgeComparator {
//     src_vertex_label: PatternLabelId,
//     src_vertex_group: PatternId,
//     edge_label: PatternLabelId,
//     dir: PatternDirection,
// }

// fn new_extend_edge_comparator(extend_edge: &ExtendEdge, pattern: &Pattern) -> ExtendEdgeComparator {
//     let src_vertex = pattern
//         .get_vertex_from_rank(extend_edge.get_src_vertex_rank())
//         .unwrap();
//     let src_vertex_label = src_vertex.get_label();
//     let src_vertex_group = pattern
//         .get_vertex_group(src_vertex.get_id())
//         .unwrap();
//     ExtendEdgeComparator {
//         src_vertex_label,
//         src_vertex_group,
//         edge_label: extend_edge.get_edge_label(),
//         dir: extend_edge.get_direction(),
//     }
// }

// /// Used to identify whether two extend step is equivalent or not
// ///
// /// It is a vector of each extend edge's (src vertex label, src vertex group, edge label, direction)
// type ExtendStepComparator = Vec<ExtendEdgeComparator>;

// /// Create a ExtendStepProxy by an ExtendStep and the Pattern it attaches to
// fn new_extend_step_comparator(extend_step: &ExtendStep, pattern: &Pattern) -> ExtendStepComparator {
//     let mut comparator: ExtendStepComparator = extend_step
//         .iter()
//         .map(|extend_edge| new_extend_edge_comparator(extend_edge, pattern))
//         .collect();
//     comparator.sort();
//     comparator
// }

/// Cost estimation functions
fn extend_cost_estimate(
    pre_pattern_weight: &PatternWeight, pattern_weight: &PatternWeight, extend_weight: &ExtendWeight,
) -> usize {
    pre_pattern_weight.count
        + pattern_weight.count
        + ((extend_weight.get_adjacency_count() as f64) * ALPHA) as usize
        + ((extend_weight.get_intersect_count() as f64) * BETA) as usize
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
        for (pattern_index, pattern_weight) in cata_topo.pattern_map.into_iter() {
            let pattern = pattern_weight.get_pattern();
            let pattern_code = pattern.encode_to();
            catalog
                .pattern_locate_map
                .insert(pattern_code, pattern_index);
            if pattern.get_vertices_num() == 1 {
                catalog.entries.push(pattern_index);
            }
            catalog.store.add_node(pattern_weight);
        }
        for (approach, approach_weight) in cata_topo.approach_map.into_iter() {
            let src_pattern_index = approach.get_src_pattern_index();
            let target_pattern_index = approach.get_target_pattern_index();
            let approach_index =
                catalog
                    .store
                    .add_edge(src_pattern_index, target_pattern_index, approach_weight);
            if let Some(extend_weight) = catalog.get_extend_weight(approach_index) {
                let src_pattern = catalog
                    .get_pattern_weight(src_pattern_index)
                    .unwrap()
                    .get_pattern();
                let target_pattern = catalog
                    .get_pattern_weight(target_pattern_index)
                    .unwrap()
                    .get_pattern();
                let src_pattern_code = src_pattern.encode_to();
                let target_pattern_code = target_pattern.encode_to();
                let extend_step = extend_weight.get_extend_step();
                // let extend_step_comparator = new_extend_step_comparator(extend_step, src_pattern);
                // catalog
                //     .extend_step_comparator_map
                //     .entry((src_pattern_code, target_pattern_code))
                //     .or_default()
                //     .insert(
                //         extend_step_comparator,
                //         Approach::new(src_pattern_index, target_pattern_index, approach_index),
                //     );
            }
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
