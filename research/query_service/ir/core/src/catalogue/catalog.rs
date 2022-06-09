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

use petgraph::graph::{EdgeIndex, Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use std::collections::{BTreeSet, BinaryHeap, HashMap, VecDeque};

use crate::catalogue::codec::{Cipher, Encoder};
use crate::catalogue::extend_step::{DefiniteExtendStep, ExtendEdge, ExtendStep};
use crate::catalogue::pattern::{Pattern, PatternEdge};
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::{query_params, DynIter, PatternDirection, PatternId, PatternRankId};
use crate::error::IrError;
use crate::plan::patmat::MatchingStrategy;

use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;

static ALPHA: f64 = 0.5;
static BETA: f64 = 0.5;
#[derive(Debug)]
struct VertexWeight {
    code: Vec<u8>,
    count: usize,
    best_approach: Option<EdgeIndex>,
}

#[derive(Debug, Clone)]
struct EdgeWeightForJoin {
    pattern_index: NodeIndex,
}

#[derive(Debug, Clone)]
struct EdgeWeightForExtendStep {
    code: Vec<u8>,
    is_single_extend: bool,
    count_sum: usize,
    counts: BinaryHeap<(usize, PatternId, PatternRankId)>,
}

#[derive(Debug, Clone)]
enum EdgeWeight {
    Pattern(EdgeWeightForJoin),
    ExtendStep(EdgeWeightForExtendStep),
}

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

#[derive(Debug)]
pub struct Catalogue {
    store: Graph<VertexWeight, EdgeWeight>,
    pattern_v_locate_map: HashMap<Vec<u8>, NodeIndex>,
    encoder: Encoder,
}

impl Catalogue {
    pub fn build_from_meta(
        pattern_meta: &PatternMeta, pattern_size_limit: usize, same_label_vertex_limit: usize,
    ) -> Catalogue {
        let mut catalog = Catalogue {
            store: Graph::new(),
            pattern_v_locate_map: HashMap::new(),
            encoder: Encoder::init_by_pattern_meta(pattern_meta, same_label_vertex_limit),
        };
        let mut queue = VecDeque::new();
        for vertex_label in pattern_meta.vertex_label_ids_iter() {
            let new_pattern = Pattern::from((0, vertex_label));
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
            if relaxed_pattern.get_vertex_num() >= pattern_size_limit {
                continue;
            }
            let extend_steps = relaxed_pattern.get_extend_steps(pattern_meta, same_label_vertex_limit);
            for extend_step in extend_steps.iter() {
                let extend_step_code: Vec<u8> = Cipher::encode_to(extend_step, &catalog.encoder);
                let new_pattern = relaxed_pattern.extend(extend_step).unwrap();
                let new_pattern_code: Vec<u8> = Cipher::encode_to(&new_pattern, &catalog.encoder);
                let (new_pattern_index, existed) = if let Some(pattern_index) = catalog
                    .pattern_v_locate_map
                    .get(&new_pattern_code)
                {
                    (*pattern_index, true)
                } else {
                    (
                        catalog.store.add_node(VertexWeight {
                            code: new_pattern_code.clone(),
                            count: 0,
                            best_approach: None,
                        }),
                        false,
                    )
                };
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
                    catalog
                        .pattern_v_locate_map
                        .insert(new_pattern_code.clone(), new_pattern_index);
                    queue.push_back((new_pattern, new_pattern_index));
                }
            }
        }
        catalog
    }

    pub fn build_from_pattern(pattern: &Pattern) -> Catalogue {
        let mut catalog = Catalogue {
            store: Graph::new(),
            pattern_v_locate_map: HashMap::new(),
            encoder: Encoder::init_by_pattern(pattern, 4),
        };
        let mut queue = VecDeque::new();
        let mut relaxed_patterns = BTreeSet::new();
        for vertex in pattern.vertices_iter() {
            let vertex_id = vertex.get_id();
            let vertex_label = vertex.get_label();
            let new_pattern = Pattern::from((vertex_id, vertex_label));
            let new_pattern_code: Vec<u8> = Cipher::encode_to(&new_pattern, &catalog.encoder);
            let new_pattern_index = if let Some(pattern_index) = catalog
                .pattern_v_locate_map
                .get(&new_pattern_code)
            {
                *pattern_index
            } else {
                let pattern_index = catalog.store.add_node(VertexWeight {
                    code: new_pattern_code.clone(),
                    count: 0,
                    best_approach: None,
                });
                catalog
                    .pattern_v_locate_map
                    .insert(new_pattern_code, pattern_index);
                pattern_index
            };
            queue.push_back((new_pattern, new_pattern_index));
        }
        while let Some((relaxed_pattern, relaxed_pattern_index)) = queue.pop_front() {
            let relaxed_pattern_vertices: BTreeSet<PatternId> = relaxed_pattern
                .vertices_iter()
                .map(|v| v.get_id())
                .collect();
            if relaxed_patterns.contains(&relaxed_pattern_vertices) {
                continue;
            } else {
                relaxed_patterns.insert(relaxed_pattern_vertices.clone());
            }
            let mut added_vertices_ids = BTreeSet::new();
            for vertex_id in relaxed_pattern
                .vertices_iter()
                .map(|v| v.get_id())
            {
                let vertex = pattern.get_vertex_from_id(vertex_id).unwrap();
                for (adj_vertex_id, adj_connections) in vertex.adjacent_vertices_iter() {
                    if relaxed_pattern_vertices.contains(&adj_vertex_id)
                        || added_vertices_ids.contains(&adj_vertex_id)
                    {
                        continue;
                    }
                    let mut add_edges_ids: DynIter<PatternId> =
                        Box::new(adj_connections.iter().map(|(e_id, _)| *e_id));
                    for (adj_adj_vertex_id, adj_adj_connections) in pattern
                        .get_vertex_from_id(adj_vertex_id)
                        .unwrap()
                        .adjacent_vertices_iter()
                    {
                        if adj_adj_vertex_id != vertex.get_id()
                            && relaxed_pattern_vertices.contains(&adj_adj_vertex_id)
                        {
                            add_edges_ids = Box::new(
                                add_edges_ids.chain(
                                    adj_adj_connections
                                        .iter()
                                        .map(|(e_id, _)| *e_id),
                                ),
                            );
                        }
                    }
                    let add_edges: Vec<&PatternEdge> = add_edges_ids
                        .map(|edge_id| pattern.get_edge_from_id(edge_id).unwrap())
                        .collect();
                    let new_pattern = relaxed_pattern
                        .clone()
                        .extend_by_edges(add_edges.iter().map(|&e| e))
                        .unwrap();
                    let new_pattern_code: Vec<u8> = Cipher::encode_to(&new_pattern, &catalog.encoder);
                    let (new_pattern_index, existed) = if let Some(pattern_index) = catalog
                        .pattern_v_locate_map
                        .get(&new_pattern_code)
                    {
                        (*pattern_index, true)
                    } else {
                        (
                            catalog.store.add_node(VertexWeight {
                                code: new_pattern_code.clone(),
                                count: 0,
                                best_approach: None,
                            }),
                            false,
                        )
                    };
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
                    let extend_step = ExtendStep::from((target_v_label, extend_edges));
                    let extend_step_code: Vec<u8> = Cipher::encode_to(&extend_step, &catalog.encoder);
                    if !existed {
                        catalog
                            .pattern_v_locate_map
                            .insert(new_pattern_code, new_pattern_index);
                    }
                    let mut found_extend_step = false;
                    for connection_weight in catalog
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
                    if !found_extend_step {
                        catalog.store.add_edge(
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
        catalog
    }
}

impl Catalogue {
    fn get_pattern_index(&self, pattern_code: &Vec<u8>) -> Option<NodeIndex> {
        self.pattern_v_locate_map
            .get(pattern_code)
            .cloned()
    }

    fn get_pattern_weight(&self, pattern_index: NodeIndex) -> Option<&VertexWeight> {
        self.store.node_weight(pattern_index)
    }

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

impl MatchingStrategy for (&Pattern, &Catalogue, &PatternMeta) {
    fn build_logical_plan(&self) -> crate::error::IrResult<pb::LogicalPlan> {
        let &(pattern, catalog, pattern_meta) = self;
        println!("{:?}", catalog.store);
        let pattern_code: Vec<u8> = Cipher::encode_to(pattern, &catalog.encoder);
        if let Some(node_index) = catalog.get_pattern_index(&pattern_code) {
            let mut trace_pattern = pattern.clone();
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
                    if !trace_pattern.vertex_has_predicate(target_vertex_id)
                        && !trace_pattern.vertex_has_property(target_vertex_id)
                    {
                        let definite_extend_step =
                            DefiniteExtendStep::new(target_vertex_id, &trace_pattern).unwrap();
                        definite_extend_steps.push(definite_extend_step);
                        trace_pattern.remove_vertex(target_vertex_id);
                        trace_pattern_index = pre_pattern_index;
                        trace_pattern_weight = pre_pattern_weight;
                        found_best_extend = true;
                        break;
                    }
                }
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
                    let definite_extend_step =
                        DefiniteExtendStep::new(target_vertex_id, &trace_pattern).unwrap();
                    definite_extend_steps.push(definite_extend_step);
                    trace_pattern.remove_vertex(target_vertex_id);
                    trace_pattern_index = pre_pattern_index;
                    trace_pattern_weight = pre_pattern_weight;
                }
            }
            let mut match_plan = pb::LogicalPlan::default();
            let mut child_offset = 1;
            let source = {
                let source_vertex = trace_pattern.vertices_iter().last().unwrap();
                let source_vertex_label_id = source_vertex.get_label();
                let source_vertex_label_name = pattern_meta
                    .get_vertex_label_name(source_vertex_label_id)
                    .unwrap();
                pb::Scan {
                    scan_opt: 0,
                    alias: Some((source_vertex.get_id() as i32).into()),
                    params: Some(query_params(vec![source_vertex_label_name.into()], vec![], None)),
                    idx_predicate: None,
                }
            };
            let mut pre_node = pb::logical_plan::Node { opr: Some(source.into()), children: vec![] };
            for definite_extend_step in definite_extend_steps.into_iter().rev() {
                let edge_expands = definite_extend_step.generate_expand_operators();
                let edge_expands_num = edge_expands.len();
                let edge_expands_ids: Vec<i32> = (0..edge_expands_num as i32)
                    .map(|i| i + child_offset)
                    .collect();
                for &i in edge_expands_ids.iter() {
                    pre_node.children.push(i);
                }
                match_plan.nodes.push(pre_node);
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
            }
            pre_node.children.push(child_offset);
            match_plan.nodes.push(pre_node);
            let sink = {
                pb::Sink {
                    tags: pattern
                        .vertices_with_tag_iter()
                        .map(|v_id| common_pb::NameOrIdKey { key: Some((v_id as i32).into()) })
                        .collect(),
                    id_name_mappings: vec![],
                }
            };
            match_plan
                .nodes
                .push(pb::logical_plan::Node { opr: Some(sink.into()), children: vec![] });
            Ok(match_plan)
        } else {
            Err(IrError::Unsupported("Cannot locate the pattern in the catalog".to_string()))
        }
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

#[cfg(test)]
mod test {
    use std::convert::TryInto;
    use std::sync::Once;

    use pegasus_client::builder::JobBuilder;

    use crate::catalogue::test_cases::pattern_cases::*;
    use crate::catalogue::test_cases::pattern_meta_cases::*;
    use crate::plan::logical::LogicalPlan;
    use crate::plan::patmat::MatchingStrategy;
    use crate::plan::physical::AsPhysical;
    use graph_proxy::{InitializeJobCompiler, QueryExpGraph};
    use pegasus::result::{ResultSink, ResultStream};
    use pegasus::{run_opt, Configuration, JobConf, StartupError};
    use pegasus_server::job::{JobAssembly, JobDesc};
    use pegasus_server::JobRequest;
    use runtime::IRJobAssembly;

    use super::Catalogue;

    static INIT: Once = Once::new();

    lazy_static! {
        static ref FACTORY: IRJobAssembly = initialize_job_compiler();
    }

    pub fn initialize() {
        INIT.call_once(|| {
            start_pegasus();
        });
    }

    fn start_pegasus() {
        match pegasus::startup(Configuration::singleton()) {
            Ok(_) => {
                lazy_static::initialize(&FACTORY);
            }
            Err(err) => match err {
                StartupError::AlreadyStarted(_) => {}
                _ => panic!("start pegasus failed"),
            },
        }
    }

    fn initialize_job_compiler() -> IRJobAssembly {
        let query_exp_graph = QueryExpGraph::new(1);
        query_exp_graph.initialize_job_compiler()
    }

    fn submit_query(job_req: JobRequest, num_workers: u32) -> ResultStream<Vec<u8>> {
        let mut conf = JobConf::default();
        conf.workers = num_workers;
        let (tx, rx) = crossbeam_channel::unbounded();
        let sink = ResultSink::new(tx);
        let cancel_hook = sink.get_cancel_hook().clone();
        let results = ResultStream::new(conf.job_id, cancel_hook, rx);
        let service = &FACTORY;
        let job = JobDesc { input: job_req.source, plan: job_req.plan, resource: job_req.resource };
        run_opt(conf, sink, move |worker| service.assemble(&job, worker)).expect("submit job failure;");
        results
    }

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

    #[test]
    fn test_generate_matching_plan_for_ldbc_pattern_from_pb_case1() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case1().unwrap();
        let catalog = Catalogue::build_from_pattern(&ldbc_pattern);
        let ldbc_graph_meta = get_ldbc_pattern_meta();
        let pb_plan = (&ldbc_pattern, &catalog, &ldbc_graph_meta)
            .build_logical_plan()
            .unwrap();
        initialize();
        let plan: LogicalPlan = pb_plan.try_into().unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.get_meta().clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();
        let request = job_builder.build().unwrap();
        let mut results = submit_query(request, 2);
        let mut count = 0;
        while let Some(result) = results.next() {
            if let Ok(_) = result {
                count += 1;
            }
        }
        println!("{}", count);
    }
}
