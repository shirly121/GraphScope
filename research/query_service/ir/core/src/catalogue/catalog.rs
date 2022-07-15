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

use crate::catalogue::codec::{Cipher, Encoder};
use crate::catalogue::extend_step::{DefiniteExtendStep, ExtendEdge, ExtendStep};
use crate::catalogue::pattern::{Pattern, PatternEdge, PatternVertex};
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::{query_params, DynIter, PatternDirection, PatternId, PatternRankId};
use crate::error::{IrError, IrResult};

static ALPHA: f64 = 0.5;
static BETA: f64 = 0.5;
/// In Catalog Graph, Vertex Represents a Pattern
#[derive(Debug, Clone)]
struct VertexWeight {
    /// Code uniquely identify a pattern
    code: Vec<u8>,
    /// Estimate how many such pattern in a graph
    count: usize,
    /// Store previous 1 step estimated best approach to get the pattern
    best_approach: Option<EdgeIndex>,
}

/// Edge Weight for approach case is join
#[derive(Debug, Clone)]
struct EdgeWeightForJoin {
    /// Join with which pattern
    pattern_index: NodeIndex,
}

/// Edge Weight for approach case is extend
#[derive(Debug, Clone)]
struct EdgeWeightForExtendStep {
    /// Code of the extend step
    code: Vec<u8>,
    /// Whether the extend step is single extend or need intersect
    is_single_extend: bool,
    /// Total count of all extend edges
    count_sum: usize,
    /// The order of extend edge (from smaller count to larger count)
    counts: BinaryHeap<(usize, PatternId, PatternRankId)>,
}

/// In Catalog Graph, Edge Represents an approach from a pattern to a another pattern
/// The approach is either:
/// pattern <join> pattern -> pattern
/// pattern <extend> extend_step -> pattern
#[derive(Debug, Clone)]
enum EdgeWeight {
    /// Case that the approach is join
    Pattern(EdgeWeightForJoin),
    /// Case that the approach is extend
    ExtendStep(EdgeWeightForExtendStep),
}

/// Methods of accesing some fields of EdgeWeight
impl EdgeWeight {
    fn is_extend(&self) -> bool {
        if let EdgeWeight::ExtendStep(_) = self {
            true
        } else {
            false
        }
    }

    fn is_join(&self) -> bool {
        if let EdgeWeight::ExtendStep(_) = self {
            false
        } else {
            true
        }
    }

    fn get_extend_step_weight(&self) -> Option<&EdgeWeightForExtendStep> {
        if let EdgeWeight::ExtendStep(w) = self {
            Some(&w)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct Catalogue {
    /// Catalog Graph
    store: Graph<VertexWeight, EdgeWeight>,
    /// Key: pattern code, Value: Node(Vertex) Index
    /// Usage: use a pattern code to uniquely identify a pattern in catalog graph
    pattern_v_locate_map: HashMap<Vec<u8>, NodeIndex>,
    /// The global encoder for the catalog
    encoder: Encoder,
}

impl Catalogue {
    /// Build a catalogue from a pattern meta with some limits
    /// It can be used to build the basic parts of catalog graph
    pub fn build_from_meta(
        pattern_meta: &PatternMeta, pattern_size_limit: usize, same_label_vertex_limit: usize,
    ) -> Catalogue {
        let mut catalog = Catalogue {
            store: Graph::new(),
            pattern_v_locate_map: HashMap::new(),
            encoder: Encoder::init_by_pattern_meta(pattern_meta, same_label_vertex_limit),
        };
        // Use BFS to generate the catalog graph
        let mut queue = VecDeque::new();
        // The one-vertex patterns are the starting points
        for vertex_label in pattern_meta.vertex_label_ids_iter() {
            let new_pattern = Pattern::from(PatternVertex::new(0, vertex_label));
            let new_pattern_code: Vec<u8> = Cipher::encode_to(&new_pattern, &catalog.encoder);
            let new_pattern_index = catalog.store.add_node(VertexWeight {
                code: new_pattern_code.clone(),
                count: 0,
                best_approach: None,
            });
            catalog
                .pattern_v_locate_map
                .insert(new_pattern_code.clone(), new_pattern_index);
            queue.push_back((new_pattern, new_pattern_index));
        }
        while queue.len() > 0 {
            let (relaxed_pattern, relaxed_pattern_index) = queue.pop_front().unwrap();
            // Filter out those patterns beyond the pattern size limitation
            if relaxed_pattern.get_vertex_num() >= pattern_size_limit {
                continue;
            }
            // Find possible extend steps of the relaxed pattern
            let extend_steps = relaxed_pattern.get_extend_steps(pattern_meta, same_label_vertex_limit);
            for extend_step in extend_steps.iter() {
                // relaxed pattern + extend step to get new pattern
                let extend_step_code: Vec<u8> = Cipher::encode_to(extend_step, &catalog.encoder);
                let new_pattern = relaxed_pattern.extend(extend_step).unwrap();
                let new_pattern_code: Vec<u8> = Cipher::encode_to(&new_pattern, &catalog.encoder);
                // check whether the new pattern existed in the catalog graph
                let (new_pattern_index, existed) = if let Some(pattern_index) = catalog
                    .pattern_v_locate_map
                    .get(&new_pattern_code)
                {
                    (*pattern_index, true)
                } else {
                    // not exist, add pattern as a vertex to the catalog graph
                    (
                        catalog.store.add_node(VertexWeight {
                            code: new_pattern_code.clone(),
                            count: 0,
                            best_approach: None,
                        }),
                        false,
                    )
                };
                // Enocode extend step and add as an edge to the catalog graph
                catalog.store.add_edge(
                    relaxed_pattern_index,
                    new_pattern_index,
                    EdgeWeight::ExtendStep(EdgeWeightForExtendStep {
                        code: extend_step_code.clone(),
                        is_single_extend: extend_step.get_extend_edges_num() == 0,
                        count_sum: 0,
                        counts: BinaryHeap::new(),
                    }),
                );
                if !existed {
                    // record the pattern with code in the locate map
                    catalog
                        .pattern_v_locate_map
                        .insert(new_pattern_code.clone(), new_pattern_index);
                    // add the pattern to the queue for further extend
                    queue.push_back((new_pattern, new_pattern_index));
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
            pattern_v_locate_map: HashMap::new(),
            encoder: Encoder::init_by_pattern(pattern, 4),
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
            let new_pattern = Pattern::from(vertex.clone());
            let new_pattern_code: Vec<u8> = Cipher::encode_to(&new_pattern, &self.encoder);
            let new_pattern_index =
                if let Some(pattern_index) = self.pattern_v_locate_map.get(&new_pattern_code) {
                    *pattern_index
                } else {
                    let pattern_index = self.store.add_node(VertexWeight {
                        code: new_pattern_code.clone(),
                        count: 0,
                        best_approach: None,
                    });
                    self.pattern_v_locate_map
                        .insert(new_pattern_code, pattern_index);
                    pattern_index
                };
            queue.push_back((new_pattern, new_pattern_index));
        }
        while let Some((relaxed_pattern, relaxed_pattern_index)) = queue.pop_front() {
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
                    .map(|adj| adj.get_adj_vertex_id())
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
                    let add_edges: Vec<PatternEdge> = pattern
                        .adjacencies_iter(adj_vertex_id)
                        .filter(|adj| relaxed_pattern_vertices.contains(&adj.get_adj_vertex_id()))
                        .map(|adj| {
                            pattern
                                .get_edge_from_id(adj.get_edge_id())
                                .unwrap()
                                .clone()
                        })
                        .collect();
                    // generate the new pattern with add_edges(extend edges)
                    let new_pattern = relaxed_pattern
                        .clone()
                        .extend_by_edges(add_edges.iter())
                        .unwrap();
                    let new_pattern_code: Vec<u8> = Cipher::encode_to(&new_pattern, &self.encoder);
                    // check whether the catalog graph has the newly generate pattern
                    let (new_pattern_index, existed) =
                        if let Some(pattern_index) = self.pattern_v_locate_map.get(&new_pattern_code) {
                            (*pattern_index, true)
                        } else {
                            (
                                self.store.add_node(VertexWeight {
                                    code: new_pattern_code.clone(),
                                    count: 0,
                                    best_approach: None,
                                }),
                                false,
                            )
                        };
                    // transform add_edges to Vec<ExtendEdge> to encode the add as an edge to the catalog graph
                    let extend_edges: Vec<ExtendEdge> = add_edges
                        .iter()
                        .map(|add_edge| {
                            let add_edge_label = add_edge.get_label();
                            let (src_v_label, src_v_rank, dir) =
                                if add_edge.get_end_vertex_id() == adj_vertex_id {
                                    let src_vertex = relaxed_pattern
                                        .get_vertex_from_id(add_edge.get_start_vertex_id())
                                        .unwrap();
                                    (src_vertex.get_label(), src_vertex.get_rank(), PatternDirection::Out)
                                } else {
                                    let src_vertex = relaxed_pattern
                                        .get_vertex_from_id(add_edge.get_end_vertex_id())
                                        .unwrap();
                                    (src_vertex.get_label(), src_vertex.get_rank(), PatternDirection::In)
                                };
                            ExtendEdge::new(src_v_label, src_v_rank, add_edge_label, dir)
                        })
                        .collect();
                    let target_v_label = pattern
                        .get_vertex_from_id(adj_vertex_id)
                        .unwrap()
                        .get_label();
                    // generate new extend step
                    let extend_step = ExtendStep::new(target_v_label, extend_edges);
                    let extend_step_code: Vec<u8> = Cipher::encode_to(&extend_step, &self.encoder);
                    if !existed {
                        self.pattern_v_locate_map
                            .insert(new_pattern_code, new_pattern_index);
                    }
                    // check whether the extend step exists in the catalog graph or not
                    let mut found_extend_step = false;
                    for connection_weight in self
                        .store
                        .edges_connecting(relaxed_pattern_index, new_pattern_index)
                        .map(|edge| edge.weight())
                    {
                        if let EdgeWeight::ExtendStep(pre_extend_step_weight) = connection_weight {
                            if pre_extend_step_weight.code == extend_step_code {
                                found_extend_step = true;
                                break;
                            }
                        }
                    }
                    // the extend step doesn't exist, add it the the catalog graph
                    if !found_extend_step {
                        self.store.add_edge(
                            relaxed_pattern_index,
                            new_pattern_index,
                            EdgeWeight::ExtendStep(EdgeWeightForExtendStep {
                                code: extend_step_code,
                                is_single_extend: extend_step.get_extend_edges_num() == 1,
                                count_sum: 0,
                                counts: BinaryHeap::new(),
                            }),
                        );
                    }
                    queue.push_back((new_pattern, new_pattern_index));
                    added_vertices_ids.insert(adj_vertex_id);
                }
            }
        }
    }
}

/// Methods for accessing some fields of Catalogue
impl Catalogue {
    fn get_pattern_index(&self, pattern_code: &Vec<u8>) -> Option<NodeIndex> {
        self.pattern_v_locate_map
            .get(pattern_code)
            .cloned()
    }

    fn get_pattern_weight(&self, pattern_index: NodeIndex) -> Option<&VertexWeight> {
        self.store.node_weight(pattern_index)
    }

    /// Get a pattern's all out connection
    fn pattern_out_connection_iter(
        &self, pattern_index: NodeIndex,
    ) -> DynIter<(NodeIndex, &VertexWeight, &EdgeWeight)> {
        Box::new(
            self.store
                .edges_directed(pattern_index, Direction::Outgoing)
                .map(move |edge| {
                    (edge.target(), self.store.node_weight(edge.target()).unwrap(), edge.weight())
                }),
        )
    }

    /// Get a pattern's all incoming connection
    fn pattern_in_connection_iter(
        &self, pattern_index: NodeIndex,
    ) -> DynIter<(NodeIndex, &VertexWeight, &EdgeWeight)> {
        Box::new(
            self.store
                .edges_directed(pattern_index, Direction::Incoming)
                .map(move |edge| {
                    (edge.source(), self.store.node_weight(edge.source()).unwrap(), edge.weight())
                }),
        )
    }
}

/// Methods for Pattern to generate pb Logical plan of pattern matching
impl Pattern {
    /// Generate a naive extend based pattern match plan
    pub fn generate_simple_extend_match_plan(&self) -> IrResult<pb::LogicalPlan> {
        let mut trace_pattern = self.clone();
        let mut definite_extend_steps = vec![];
        while trace_pattern.get_vertex_num() > 1 {
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
            let definite_extend_step = trace_pattern
                .generate_definite_extend_step_by_v_id(select_vertex_id)
                .unwrap();
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
            while trace_pattern.get_vertex_num() > 1 {
                let mut all_extends: Vec<(NodeIndex, &VertexWeight, &EdgeWeight)> = catalog
                    .pattern_in_connection_iter(trace_pattern_index)
                    .filter(|(_, _, edge_weight)| edge_weight.is_extend())
                    .collect();
                // use the extend step with the lowerst estimated cost
                all_extends.sort_by(
                    |&(_, pre_pattern_weight1, edge_weight1), &(_, pre_pattern_weight2, edge_weight2)| {
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
                let mut found_best_extend = false;
                // make the vertex with predicate to be extended earlier
                for &(pre_pattern_index, pre_pattern_weight, edge_weight) in all_extends.iter() {
                    let extend_step: ExtendStep = Cipher::decode_from(
                        &edge_weight
                            .get_extend_step_weight()
                            .unwrap()
                            .code,
                        &catalog.encoder,
                    )?;
                    let target_vertex_id = trace_pattern
                        .locate_vertex(&extend_step, &pre_pattern_weight.code, &catalog.encoder)
                        .unwrap();
                    if !trace_pattern.vertex_has_predicate(target_vertex_id) {
                        let definite_extend_step = trace_pattern
                            .generate_definite_extend_step_by_v_id(target_vertex_id)
                            .unwrap();
                        definite_extend_steps.push(definite_extend_step);
                        trace_pattern.remove_vertex(target_vertex_id);
                        trace_pattern_index = pre_pattern_index;
                        trace_pattern_weight = pre_pattern_weight;
                        found_best_extend = true;
                        break;
                    }
                }
                // Situation that all vertices have predicate, then pick the one with the lowest cost
                if !found_best_extend {
                    let (pre_pattern_index, pre_pattern_weight, edge_weight) = all_extends[0];
                    let extend_step: ExtendStep = Cipher::decode_from(
                        &edge_weight
                            .get_extend_step_weight()
                            .unwrap()
                            .code,
                        &catalog.encoder,
                    )?;
                    let target_vertex_id = trace_pattern
                        .locate_vertex(&extend_step, &pre_pattern_weight.code, &catalog.encoder)
                        .unwrap();
                    let definite_extend_step = trace_pattern
                        .generate_definite_extend_step_by_v_id(target_vertex_id)
                        .unwrap();
                    definite_extend_steps.push(definite_extend_step);
                    trace_pattern.remove_vertex(target_vertex_id);
                    trace_pattern_index = pre_pattern_index;
                    trace_pattern_weight = pre_pattern_weight;
                }
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
        let source_vertex_label = source_extend.get_target_v_label();
        pb::Scan {
            scan_opt: 0,
            alias: Some((source_extend.get_target_v_id() as i32).into()),
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
                .map(|v_id| common_pb::NameOrIdKey { key: Some((v_id as i32).into()) })
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

/// Cost estimation functions
fn total_cost_estimate(
    alpha: f64, beta: f64, pre_pattern_weight: &VertexWeight, pattern_weight: &VertexWeight,
    edge_weight: &EdgeWeight,
) -> usize {
    if let EdgeWeight::ExtendStep(extend_weight) = edge_weight {
        f_cost_estimate(alpha, pre_pattern_weight, extend_weight)
            + i_cost_estimate(beta, pre_pattern_weight, extend_weight)
            + e_cost_estimate(pattern_weight)
            + d_cost_estimate(pre_pattern_weight)
    } else {
        usize::MAX
    }
}

fn f_cost_estimate(
    alpha: f64, pre_pattern_weight: &VertexWeight, extend_weight: &EdgeWeightForExtendStep,
) -> usize {
    (alpha * (pre_pattern_weight.count as f64) * (extend_weight.count_sum as f64)) as usize
}

fn i_cost_estimate(
    beta: f64, pre_pattern_weight: &VertexWeight, extend_weight: &EdgeWeightForExtendStep,
) -> usize {
    if extend_weight.is_single_extend {
        0
    } else {
        (beta * (pre_pattern_weight.count as f64) * (extend_weight.count_sum as f64)) as usize
    }
}

fn e_cost_estimate(pattern_weight: &VertexWeight) -> usize {
    pattern_weight.count
}

fn d_cost_estimate(pre_pattern_weight: &VertexWeight) -> usize {
    pre_pattern_weight.count
}

#[cfg(test)]
mod test {
    use crate::catalogue::catalog::Catalogue;
    use crate::catalogue::test_cases::pattern_cases::*;
    use crate::catalogue::test_cases::pattern_meta_cases::*;

    #[test]
    fn test_catalog_for_modern_graph() {
        let modern_graph_meta = get_modern_pattern_meta();
        let catalog = Catalogue::build_from_meta(&modern_graph_meta, 2, 3);
        assert_eq!(4, catalog.store.node_count());
        assert_eq!(4, catalog.store.edge_count());
    }

    #[test]
    fn test_catalog_for_ldbc_graph() {
        let ldbc_graph_meta = get_ldbc_pattern_meta();
        let catalog = Catalogue::build_from_meta(&ldbc_graph_meta, 2, 3);
        assert_eq!(34, catalog.store.node_count());
        assert_eq!(42, catalog.store.edge_count());
    }

    #[test]
    fn test_catalog_for_modern_pattern_case1() {
        let modern_pattern = build_modern_pattern_case1();
        let catalog = Catalogue::build_from_pattern(&modern_pattern);
        assert_eq!(1, catalog.store.node_count());
        assert_eq!(0, catalog.store.edge_count());
    }

    #[test]
    fn test_catalog_for_modern_pattern_case2() {
        let modern_pattern = build_modern_pattern_case2();
        let catalog = Catalogue::build_from_pattern(&modern_pattern);
        assert_eq!(1, catalog.store.node_count());
        assert_eq!(0, catalog.store.edge_count());
    }

    #[test]
    fn test_catalog_for_modern_pattern_case3() {
        let modern_pattern = build_modern_pattern_case3();
        let catalog = Catalogue::build_from_pattern(&modern_pattern);
        assert_eq!(2, catalog.store.node_count());
        assert_eq!(2, catalog.store.edge_count());
    }

    #[test]
    fn test_catalog_for_modern_pattern_case4() {
        let modern_pattern = build_modern_pattern_case4();
        let catalog = Catalogue::build_from_pattern(&modern_pattern);
        assert_eq!(3, catalog.store.node_count());
        assert_eq!(2, catalog.store.edge_count());
    }

    #[test]
    fn test_catalog_for_ldbc_pattern_from_pb_case1() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case1().unwrap();
        let catalog = Catalogue::build_from_pattern(&ldbc_pattern);
        assert_eq!(3, catalog.store.node_count());
        assert_eq!(5, catalog.store.edge_count());
    }

    #[test]
    fn test_catalog_for_ldbc_pattern_from_pb_case2() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case2().unwrap();
        let catalog = Catalogue::build_from_pattern(&ldbc_pattern);
        assert_eq!(5, catalog.store.node_count());
        assert_eq!(7, catalog.store.edge_count());
    }

    #[test]
    fn test_catalog_for_ldbc_pattern_from_pb_case3() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case3().unwrap();
        let catalog = Catalogue::build_from_pattern(&ldbc_pattern);
        assert_eq!(4, catalog.store.node_count());
        assert_eq!(9, catalog.store.edge_count());
    }

    #[test]
    fn test_catalog_for_ldbc_pattern_from_pb_case4() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case4().unwrap();
        let catalog = Catalogue::build_from_pattern(&ldbc_pattern);
        assert_eq!(11, catalog.store.node_count());
        assert_eq!(17, catalog.store.edge_count());
    }
}
