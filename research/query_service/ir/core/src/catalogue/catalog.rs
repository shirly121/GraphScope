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
use std::collections::{BTreeSet, BinaryHeap, HashMap, VecDeque};
use std::convert::TryInto;

use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use petgraph::graph::{EdgeIndex, Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;

use super::pattern::PatternVertex;
use crate::catalogue::codec::{Cipher, Encoder};
use crate::catalogue::extend_step::{DefiniteExtendStep, ExtendEdge, ExtendStep};
use crate::catalogue::pattern::{Adjacency, Pattern};
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::{query_params, DynIter, PatternDirection, PatternId, PatternLabelId};
use crate::error::{IrError, IrResult};

static ALPHA: f64 = 0.5;
static BETA: f64 = 0.5;
/// In Catalog Graph, Vertex Represents a Pattern
#[derive(Debug, Clone)]
pub struct PatternWeight {
    /// Code uniquely identify a pattern
    code: Vec<u8>,
    /// Estimate how many such pattern in a graph
    count: usize,
    /// Store previous 1 step estimated best approach to get the pattern
    best_approach: Option<EdgeIndex>,
}

impl PatternWeight {
    pub fn get_pattern_code(&self) -> Vec<u8> {
        self.code.clone()
    }

    pub fn get_pattern(&self, encoder: &Encoder) -> Pattern {
        Cipher::decode_from(&self.code, encoder).unwrap()
    }
}

/// Edge Weight for approach case is join
#[derive(Debug, Clone)]
pub struct JoinWeight {
    /// Join with which pattern
    pattern_index: NodeIndex,
}

/// Edge Weight for approach case is extend
#[derive(Debug, Clone)]
pub struct ExtendWeight {
    /// Code of the extend step
    code: Vec<u8>,
    /// Target vertex's order in the target pattern
    target_vertex_rank: usize,
    /// Whether the extend step is single extend or need intersect
    is_single_extend: bool,
    /// Total count of all extend edges
    count_sum: usize,
    /// The order of extend edge (from smaller count to larger count)
    counts: BinaryHeap<(usize, ExtendEdge)>,
}

impl ExtendWeight {
    pub fn get_extend_step_code(&self) -> Vec<u8> {
        self.code.clone()
    }

    pub fn get_extend_step(&self, encoder: &Encoder) -> ExtendStep {
        Cipher::decode_from(&self.code, encoder).unwrap()
    }

    pub fn get_target_vertex_rank(&self) -> usize {
        self.target_vertex_rank
    }
}

/// In Catalog Graph, Edge Represents an approach from a pattern to a another pattern
/// The approach is either:
/// pattern <join> pattern -> pattern
/// pattern <extend> extend_step -> pattern
#[derive(Debug, Clone)]
pub enum ApproachWeight {
    /// Case that the approach is join
    Pattern(JoinWeight),
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
        if let ApproachWeight::ExtendStep(_) = self {
            false
        } else {
            true
        }
    }

    pub fn get_extend_weight(&self) -> Option<&ExtendWeight> {
        if let ApproachWeight::ExtendStep(w) = self {
            Some(&w)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct Catalogue {
    /// Catalog Graph
    store: Graph<PatternWeight, ApproachWeight>,
    /// Key: pattern code, Value: Node(Vertex) Index
    /// Usage: use a pattern code to uniquely identify a pattern in catalog graph
    pattern_locate_map: HashMap<Vec<u8>, NodeIndex>,
    /// The global encoder for the catalog
    encoder: Encoder,
    // Store the extend steps already found between the src pattern and target pattern
    // - Used to avoid add equivalent extend steps between two patterns
    extend_step_comparator_map: HashMap<(Vec<u8>, Vec<u8>), BTreeSet<ExtendStepComparator>>,

    entry_map: HashMap<PatternLabelId, NodeIndex>,
}

impl Catalogue {
    /// Build a catalogue from a pattern meta with some limits
    /// It can be used to build the basic parts of catalog graph
    pub fn build_from_meta(
        pattern_meta: &PatternMeta, pattern_size_limit: usize, same_label_vertex_limit: usize,
    ) -> Catalogue {
        let mut catalog = Catalogue {
            store: Graph::new(),
            pattern_locate_map: HashMap::new(),
            encoder: Encoder::init_by_pattern_meta(pattern_meta, same_label_vertex_limit),
            extend_step_comparator_map: HashMap::new(),
            entry_map: HashMap::new(),
        };
        // Use BFS to generate the catalog graph
        let mut queue = VecDeque::new();
        // The one-vertex patterns are the starting points
        for vertex_label in pattern_meta.vertex_label_ids_iter() {
            let new_pattern = Pattern::from(PatternVertex::new(0, vertex_label));
            let new_pattern_code: Vec<u8> = Cipher::encode_to(&new_pattern, &catalog.encoder);
            let new_pattern_index = catalog.store.add_node(PatternWeight {
                code: new_pattern_code.clone(),
                count: 0,
                best_approach: None,
            });
            catalog
                .pattern_locate_map
                .insert(new_pattern_code.clone(), new_pattern_index);
            catalog
                .entry_map
                .insert(vertex_label, new_pattern_index);
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
                let extend_step_code: Vec<u8> = Cipher::encode_to(extend_step, &catalog.encoder);
                let extend_step_comparator: ExtendStepComparator =
                    create_extend_step_comparator(extend_step, &relaxed_pattern);
                let new_pattern = relaxed_pattern.extend(extend_step).unwrap();
                let new_pattern_code: Vec<u8> = Cipher::encode_to(&new_pattern, &catalog.encoder);
                // check whether the new pattern existed in the catalog graph
                let new_pattern_index = if let Some(&pattern_index) = catalog
                    .pattern_locate_map
                    .get(&new_pattern_code)
                {
                    pattern_index
                } else {
                    catalog.store.add_node(PatternWeight {
                        code: new_pattern_code.clone(),
                        count: 0,
                        best_approach: None,
                    })
                };
                // Get all extend steps(comparators) between the relaxed pattern and new pattern
                let extend_step_comparators = catalog
                    .extend_step_comparator_map
                    .entry((relaxed_pattern_code.clone(), new_pattern_code.clone()))
                    .or_default();
                // If there exists a equivalent extend step added to the catalog, continue
                if !extend_step_comparators.contains(&extend_step_comparator) {
                    extend_step_comparators.insert(extend_step_comparator.clone());
                    // Enocode extend step and add as an edge to the catalog graph
                    catalog.store.add_edge(
                        relaxed_pattern_index,
                        new_pattern_index,
                        ApproachWeight::ExtendStep(ExtendWeight {
                            code: extend_step_code.clone(),
                            target_vertex_rank: new_pattern
                                .get_vertex_rank(new_pattern.get_max_vertex_id())
                                .unwrap(),
                            is_single_extend: extend_step.get_extend_edges_num() == 0,
                            count_sum: 0,
                            counts: BinaryHeap::new(),
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
                }
            }
        }
        catalog
    }

    /// Build a catalog from a pattern dedicated for its optimization
    pub fn build_from_pattern(pattern: &Pattern) -> Catalogue {
        // Empty catalog
        let mut catalog = Catalogue {
            store: Graph::new(),
            pattern_locate_map: HashMap::new(),
            encoder: Encoder::init_by_pattern(pattern, 4),
            extend_step_comparator_map: HashMap::new(),
            entry_map: HashMap::new(),
        };
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
            let new_pattern_code: Vec<u8> = Cipher::encode_to(&new_pattern, &self.encoder);
            let new_pattern_index =
                if let Some(pattern_index) = self.pattern_locate_map.get(&new_pattern_code) {
                    *pattern_index
                } else {
                    let pattern_index = self.store.add_node(PatternWeight {
                        code: new_pattern_code.clone(),
                        count: 0,
                        best_approach: None,
                    });
                    self.pattern_locate_map
                        .insert(new_pattern_code.clone(), pattern_index);
                    self.entry_map
                        .insert(vertex.get_label(), pattern_index);
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
                    // back link to the relaxed pattern to find all edges to be added
                    let back_links: Vec<&Adjacency> = pattern
                        .adjacencies_iter(adj_vertex_id)
                        .filter(|adj| relaxed_pattern_vertices.contains(&adj.get_adj_vertex().get_id()))
                        .collect();
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
                    let extend_step_code: Vec<u8> = Cipher::encode_to(&extend_step, &self.encoder);
                    let extend_step_comparator: ExtendStepComparator =
                        create_extend_step_comparator(&extend_step, &relaxed_pattern);
                    // generate the new pattern with add_edges(extend edges)
                    let new_pattern = relaxed_pattern
                        .extend_by_edges(
                            back_links
                                .iter()
                                .map(|adj| pattern.get_edge(adj.get_edge_id()).unwrap()),
                        )
                        .unwrap();
                    let new_pattern_code: Vec<u8> = Cipher::encode_to(&new_pattern, &self.encoder);
                    // check whether the catalog graph has the newly generate pattern
                    let new_pattern_index =
                        if let Some(&pattern_index) = self.pattern_locate_map.get(&new_pattern_code) {
                            pattern_index
                        } else {
                            self.store.add_node(PatternWeight {
                                code: new_pattern_code.clone(),
                                count: 0,
                                best_approach: None,
                            })
                        };
                    // check whether the extend step exists in the catalog graph or not
                    // Get all extend steps(comparators) between the relaxed pattern and new pattern
                    let extend_step_comparators = self
                        .extend_step_comparator_map
                        .entry((relaxed_pattern_code.clone(), new_pattern_code.clone()))
                        .or_default();
                    //
                    if !extend_step_comparators.contains(&extend_step_comparator) {
                        extend_step_comparators.insert(extend_step_comparator.clone());
                        self.store.add_edge(
                            relaxed_pattern_index,
                            new_pattern_index,
                            ApproachWeight::ExtendStep(ExtendWeight {
                                code: extend_step_code,
                                target_vertex_rank: new_pattern
                                    .get_vertex_rank(adj_vertex_id)
                                    .unwrap(),
                                is_single_extend: extend_step.get_extend_edges_num() == 1,
                                count_sum: 0,
                                counts: BinaryHeap::new(),
                            }),
                        );
                        if !self
                            .pattern_locate_map
                            .contains_key(&new_pattern_code)
                        {
                            self.pattern_locate_map
                                .insert(new_pattern_code.clone(), new_pattern_index);
                        }
                    }
                    added_vertices_ids.insert(adj_vertex_id);
                    queue.push_back((new_pattern, new_pattern_code, new_pattern_index));
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

    pub fn get_encoder(&self) -> &Encoder {
        &self.encoder
    }

    pub fn entries_iter(&self) -> DynIter<(PatternLabelId, NodeIndex)> {
        Box::new(
            self.entry_map
                .iter()
                .map(|(label, node_index)| (*label, *node_index)),
        )
    }

    /// Get a pattern's all out connection
    pub fn pattern_out_approach_iter(
        &self, pattern_index: NodeIndex,
    ) -> DynIter<(NodeIndex, &PatternWeight, EdgeIndex, &ApproachWeight)> {
        Box::new(
            self.store
                .edges_directed(pattern_index, Direction::Outgoing)
                .map(move |edge| {
                    (
                        edge.target(),
                        self.store.node_weight(edge.target()).unwrap(),
                        edge.id(),
                        edge.weight(),
                    )
                }),
        )
    }

    /// Get a pattern's all incoming connection
    pub fn pattern_in_approach_iter(
        &self, pattern_index: NodeIndex,
    ) -> DynIter<(NodeIndex, &PatternWeight, EdgeIndex, &ApproachWeight)> {
        Box::new(
            self.store
                .edges_directed(pattern_index, Direction::Incoming)
                .map(move |edge| {
                    (
                        edge.source(),
                        self.store.node_weight(edge.source()).unwrap(),
                        edge.id(),
                        edge.weight(),
                    )
                }),
        )
    }

    pub fn set_pattern_count(&mut self, pattern_index: NodeIndex, count: usize) {
        if let Some(pattern_weight) = self.store.node_weight_mut(pattern_index) {
            pattern_weight.count = count
        }
    }

    pub fn set_extend_count(
        &mut self, extend_index: EdgeIndex, extend_nums_counts: Vec<(ExtendEdge, usize)>,
    ) {
        if let Some(approach_weight) = self.store.edge_weight_mut(extend_index) {
            if let ApproachWeight::ExtendStep(extend_weight) = approach_weight {
                extend_weight.count_sum = 0;
                extend_weight.counts.clear();
                for (extend_edge, count) in extend_nums_counts {
                    extend_weight.count_sum += count;
                    extend_weight.counts.push((count, extend_edge));
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
            });
            let select_vertex_id = *all_vertex_ids.first().unwrap();
            let definite_extend_step =
                DefiniteExtendStep::new_from_target_vertex_id(&trace_pattern, select_vertex_id).unwrap();
            definite_extend_steps.push(definite_extend_step);
            trace_pattern.remove_vertex(select_vertex_id);
        }
        definite_extend_steps.push(trace_pattern.try_into()?);
        build_logical_plan(self, definite_extend_steps)
    }

    /// Generate an optimized extend based pattern match plan
    /// Current implementation is Top-Down Greedy method
    pub fn generate_optimized_match_plan(&self, catalog: &Catalogue) -> IrResult<pb::LogicalPlan> {
        let pattern_code: Vec<u8> = Cipher::encode_to(self, &catalog.encoder);
        // locate the pattern node in the catalog graph
        if let Some(node_index) = catalog.get_pattern_index(&pattern_code) {
            let mut trace_pattern = self.clone();
            let mut trace_pattern_index = node_index;
            let mut trace_pattern_weight = catalog
                .get_pattern_weight(trace_pattern_index)
                .unwrap();
            let mut definite_extend_steps = vec![];
            while trace_pattern.get_vertices_num() > 1 {
                let mut all_extends: Vec<(NodeIndex, &PatternWeight, &ApproachWeight)> = catalog
                    .pattern_in_approach_iter(trace_pattern_index)
                    .filter(|(_, _, _, edge_weight)| edge_weight.is_extend())
                    .map(|(src_pattern_index, src_pattern_weight, _, approach_weight)| {
                        (src_pattern_index, src_pattern_weight, approach_weight)
                    })
                    .collect();
                // use the extend step with the lowerst estimated cost
                all_extends.sort_by(
                    |&(_, pre_pattern_weight1, edge_weight1), &(_, pre_pattern_weight2, edge_weight2)| {
                        if let (
                            ApproachWeight::ExtendStep(extend_weight1),
                            ApproachWeight::ExtendStep(extend_weight2),
                        ) = (edge_weight1, edge_weight2)
                        {
                            let target_vertex1_has_predicate = trace_pattern
                                .get_vertex_predicate(
                                    trace_pattern
                                        .get_vertex_from_rank(extend_weight1.target_vertex_rank)
                                        .unwrap()
                                        .get_id(),
                                )
                                .is_some();
                            let target_vertex2_has_predicate = trace_pattern
                                .get_vertex_predicate(
                                    trace_pattern
                                        .get_vertex_from_rank(extend_weight2.target_vertex_rank)
                                        .unwrap()
                                        .get_id(),
                                )
                                .is_some();
                            if target_vertex1_has_predicate && !target_vertex2_has_predicate {
                                return Ordering::Greater;
                            } else if !target_vertex1_has_predicate && target_vertex2_has_predicate {
                                return Ordering::Less;
                            }
                        }
                        total_cost_estimate(
                            ALPHA,
                            BETA,
                            pre_pattern_weight1,
                            trace_pattern_weight,
                            edge_weight1,
                        )
                        .cmp(&total_cost_estimate(
                            ALPHA,
                            BETA,
                            pre_pattern_weight2,
                            trace_pattern_weight,
                            edge_weight2,
                        ))
                    },
                );
                let (pre_pattern_index, pre_pattern_weight, edge_weight) = all_extends[0];
                let target_vertex_id = trace_pattern
                    .get_vertex_from_rank(
                        edge_weight
                            .get_extend_weight()
                            .unwrap()
                            .target_vertex_rank,
                    )
                    .unwrap()
                    .get_id();

                let definite_extend_step =
                    DefiniteExtendStep::new_from_target_vertex_id(&trace_pattern, target_vertex_id)
                        .unwrap();
                definite_extend_steps.push(definite_extend_step);
                trace_pattern.remove_vertex(target_vertex_id);
                trace_pattern_index = pre_pattern_index;
                trace_pattern_weight = pre_pattern_weight;
            }
            // transform the one-vertex pattern into definite extend step
            definite_extend_steps.push(trace_pattern.try_into()?);
            build_logical_plan(self, definite_extend_steps)
        } else {
            Err(IrError::Unsupported("Cannot Locate Pattern in the Catalogue".to_string()))
        }
    }
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
        pb::Scan {
            scan_opt: 0,
            alias: Some((source_extend.get_target_vertex_id() as i32).into()),
            params: Some(query_params(vec![source_vertex_label.into()], vec![], None)),
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

/// Used to identify whether two extend step is equivalent or not
///
/// It is a vector of each extend edge's (src vertex label, src vertex group, edge label, direction)
type ExtendStepComparator = Vec<(PatternLabelId, PatternId, PatternLabelId, PatternDirection)>;

/// Create a ExtendStepProxy by an ExtendStep and the Pattern it attaches to
fn create_extend_step_comparator(extend_step: &ExtendStep, pattern: &Pattern) -> ExtendStepComparator {
    let mut comparator: ExtendStepComparator = extend_step
        .iter()
        .map(|extend_edge| {
            let src_vertex = pattern
                .get_vertex_from_rank(extend_edge.get_src_vertex_rank())
                .unwrap();
            let src_vertex_label = src_vertex.get_label();
            let src_vertex_group = pattern
                .get_vertex_group(src_vertex.get_id())
                .unwrap();
            (src_vertex_label, src_vertex_group, extend_edge.get_edge_label(), extend_edge.get_direction())
        })
        .collect();
    comparator.sort();
    comparator
}

/// Cost estimation functions
fn total_cost_estimate(
    alpha: f64, beta: f64, pre_pattern_weight: &PatternWeight, pattern_weight: &PatternWeight,
    edge_weight: &ApproachWeight,
) -> usize {
    if let ApproachWeight::ExtendStep(extend_weight) = edge_weight {
        f_cost_estimate(alpha, pre_pattern_weight, extend_weight)
            + i_cost_estimate(beta, pre_pattern_weight, extend_weight)
            + e_cost_estimate(pattern_weight)
            + d_cost_estimate(pre_pattern_weight)
    } else {
        usize::MAX
    }
}

fn f_cost_estimate(alpha: f64, pre_pattern_weight: &PatternWeight, extend_weight: &ExtendWeight) -> usize {
    (alpha * (pre_pattern_weight.count as f64) * (extend_weight.count_sum as f64)) as usize
}

fn i_cost_estimate(beta: f64, pre_pattern_weight: &PatternWeight, extend_weight: &ExtendWeight) -> usize {
    if extend_weight.is_single_extend {
        0
    } else {
        (beta * (pre_pattern_weight.count as f64) * (extend_weight.count_sum as f64)) as usize
    }
}

fn e_cost_estimate(pattern_weight: &PatternWeight) -> usize {
    pattern_weight.count
}

fn d_cost_estimate(pre_pattern_weight: &PatternWeight) -> usize {
    pre_pattern_weight.count
}
