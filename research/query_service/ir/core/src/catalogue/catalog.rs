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

use std::collections::{BTreeSet, BinaryHeap, HashMap, VecDeque};

use petgraph::graph::{EdgeIndex, Graph, NodeIndex};

use crate::catalogue::codec::{Cipher, Encoder};
use crate::catalogue::pattern::Pattern;
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::{DynIter, PatternId, PatternRankId};

use super::extend_step::{ExtendEdge, ExtendStep};
use super::pattern::PatternEdge;
use super::PatternDirection;

#[derive(Debug)]
struct VertexWeight {
    code: Vec<u8>,
    count: usize,
    best_approach: Option<EdgeIndex>,
}

#[derive(Debug)]
struct EdgeWeightForJoin {
    pattern_index: NodeIndex,
}

#[derive(Debug, Clone)]
struct EdgeWeightForExtendStep {
    code: Vec<u8>,
    count_sum: usize,
    counts: BinaryHeap<(usize, PatternId, PatternRankId)>,
}

#[derive(Debug)]
enum EdgeWeight {
    Pattern(EdgeWeightForJoin),
    ExtendStep(EdgeWeightForExtendStep),
}

impl EdgeWeight {
    pub fn get_extend_step_weight(&self) -> Option<EdgeWeightForExtendStep> {
        if let EdgeWeight::ExtendStep(w) = self {
            Some(w.clone())
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
            for extend_step in extend_steps {
                let extend_step_code: Vec<u8> = Cipher::encode_to(&extend_step, &catalog.encoder);
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

#[cfg(test)]
mod test {
    use crate::catalogue::test_cases::pattern_cases::*;
    use crate::catalogue::test_cases::pattern_meta_cases::*;

    use super::Catalogue;

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
