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
//!

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter::FromIterator;
use std::path::Path;
use std::sync::{mpsc, Arc};
use std::{thread, vec};

use crate::catalogue::catalog::{get_definite_extend_steps_recursively, Catalogue};
use crate::catalogue::extend_step::{DefiniteExtendEdge, DefiniteExtendStep};
use crate::catalogue::pattern::Pattern;
use crate::catalogue::{DynIter, PatternId, PatternLabelId};

use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::{DefaultId, GlobalStoreTrait, GraphDBConfig, InternalId, LabelId, LargeGraphDB};
use petgraph::graph::NodeIndex;

type PatternRecord = BTreeMap<PatternId, DefaultId>;

impl Catalogue {
    pub fn estimate_graph(
        &mut self, graph: Arc<LargeGraphDB<DefaultId, InternalId>>, rate: f64, limit: Option<usize>,
    ) {
        let mut relaxed_patterns_indices = BTreeSet::new();
        let mut pattern_counts_map = BTreeMap::new();
        let mut extend_counts_map = BTreeMap::new();
        let mut pattern_nodes = self.get_start_pattern_nodes(&graph, rate, limit);
        for &(_, start_pattern_index, start_pattern_count, _) in pattern_nodes.iter() {
            relaxed_patterns_indices.insert(start_pattern_index);
            pattern_counts_map.insert(start_pattern_index, start_pattern_count);
        }
        while pattern_nodes.len() > 0 {
            let mut next_pattern_nodes = vec![];
            for (pattern, pattern_index, pattern_count, pattern_records) in pattern_nodes.into_iter() {
                let pattern_records = Arc::new(pattern_records);
                let pattern = Arc::new(pattern);
                for approach in self.pattern_out_approaches_iter(pattern_index) {
                    if let Some(extend_weight) = self
                        .get_approach_weight(approach.get_approach_index())
                        .and_then(|approach_weight| approach_weight.get_extend_weight())
                    {
                        if extend_weight.is_unset() {
                            let extend_step = Arc::new(extend_weight.get_extend_step().clone());
                            let target_pattern = pattern.extend(&extend_step).unwrap();
                            let target_vertex_id = target_pattern.get_max_vertex_id();
                            let mut extend_nums_counts = HashMap::new();
                            let mut target_pattern_records = Vec::new();
                            let (tx_count, rx_count) = mpsc::channel();
                            let (tx_records, rx_records) = mpsc::channel();
                            let thread_num = 8;
                            let mut thread_handles = vec![];
                            for thread_id in 0..thread_num {
                                let extend_step = Arc::clone(&extend_step);
                                let pattern_records = Arc::clone(&pattern_records);
                                let graph = Arc::clone(&graph);
                                let pattern = Arc::clone(&pattern);
                                let tx_count = tx_count.clone();
                                let tx_records = tx_records.clone();
                                let start_index = (pattern_records.len() / thread_num) * thread_id;
                                let end_index = if thread_id == thread_num - 1 {
                                    pattern_records.len()
                                } else {
                                    (pattern_records.len() / thread_num) * (thread_id + 1)
                                };
                                let thread_handle = thread::spawn(move || {
                                    for pattern_record in &pattern_records[start_index..end_index] {
                                        let mut intersect_vertices_set = BTreeSet::new();
                                        for (i, extend_edge) in extend_step.iter().enumerate() {
                                            let adj_vertices_set = get_adj_vertices_set(
                                                &graph,
                                                pattern_record,
                                                &DefiniteExtendEdge::from_extend_edge(
                                                    extend_edge,
                                                    &pattern,
                                                )
                                                .unwrap(),
                                                extend_step.get_target_vertex_label(),
                                            );
                                            tx_count
                                                .send((extend_edge.clone(), adj_vertices_set.len()))
                                                .unwrap();
                                            intersect_vertices_set = intersect_sets(
                                                intersect_vertices_set,
                                                adj_vertices_set,
                                                i == 0,
                                            );
                                        }
                                        for target_pattern_record in
                                            intersect_vertices_set
                                                .iter()
                                                .map(|&adj_vertex_id| {
                                                    let mut target_pattern_record = pattern_record.clone();
                                                    target_pattern_record
                                                        .insert(target_vertex_id, adj_vertex_id);
                                                    target_pattern_record
                                                })
                                        {
                                            tx_records.send(target_pattern_record).unwrap();
                                        }
                                    }
                                });
                                thread_handles.push(thread_handle);
                            }
                            for thread_handle in thread_handles {
                                thread_handle.join().unwrap();
                            }
                            while let Ok((extend_edge, count)) = rx_count.try_recv() {
                                *extend_nums_counts
                                    .entry(extend_edge)
                                    .or_insert(0.0) += count as f64
                            }
                            while let Ok(target_pattern_record) = rx_records.try_recv() {
                                target_pattern_records.push(target_pattern_record);
                            }
                            if pattern_records.len() != 0 {
                                for (_, count) in extend_nums_counts.iter_mut() {
                                    *count /= pattern_records.len() as f64
                                }
                            }
                            let target_pattern_count = if pattern_records.len() == 0 {
                                0
                            } else {
                                (pattern_count as f64
                                    * (target_pattern_records.len() as f64 / pattern_records.len() as f64))
                                    as usize
                            };
                            target_pattern_records = sample_records(target_pattern_records, rate, limit);
                            let target_pattern_index = approach.get_target_pattern_index();
                            extend_counts_map.insert(approach, extend_nums_counts);
                            if !relaxed_patterns_indices.contains(&target_pattern_index) {
                                next_pattern_nodes.push((
                                    target_pattern,
                                    target_pattern_index,
                                    target_pattern_count,
                                    target_pattern_records,
                                ));
                                relaxed_patterns_indices.insert(target_pattern_index);
                                if self
                                    .get_pattern_weight(target_pattern_index)
                                    .unwrap()
                                    .is_unset()
                                {
                                    pattern_counts_map.insert(target_pattern_index, target_pattern_count);
                                }
                            }
                        }
                    }
                }
            }
            pattern_nodes = next_pattern_nodes;
        }
        println!("{:?}", pattern_counts_map);
        println!("{:?}", extend_counts_map);
        for (pattern_index, pattern_count) in pattern_counts_map {
            self.set_pattern_count(pattern_index, pattern_count);
        }
        for (extend_index, extend_nums_counts) in extend_counts_map.into_iter() {
            self.set_extend_count(extend_index, extend_nums_counts);
        }
    }

    fn get_start_pattern_indices(&self) -> Vec<NodeIndex> {
        let mut start_pattern_indices = vec![];
        for entry_index in self.entries_iter() {
            if self
                .get_pattern_weight(entry_index)
                .unwrap()
                .is_unset()
            {
                start_pattern_indices.push(entry_index);
            }
        }
        for pattern_index in self
            .pattern_indices_iter()
            .filter(|&pattern_index| {
                self.get_pattern_weight(pattern_index)
                    .unwrap()
                    .is_unset_out_related()
            })
        {
            start_pattern_indices.push(pattern_index)
        }
        start_pattern_indices
    }

    fn get_start_pattern_nodes(
        &mut self, graph: &LargeGraphDB<DefaultId, InternalId>, rate: f64, limit: Option<usize>,
    ) -> Vec<(Pattern, NodeIndex, usize, Vec<PatternRecord>)> {
        let mut pattern_nodes = Vec::new();
        for start_pattern_index in self.get_start_pattern_indices() {
            let pattern = self
                .get_pattern_weight(start_pattern_index)
                .unwrap()
                .get_pattern()
                .clone();
            let (extend_steps, _) =
                get_definite_extend_steps_recursively(self, start_pattern_index, pattern.clone());
            let mut pattern_records = get_src_records(graph, extend_steps, limit);
            let pattern_count = pattern_records.len();
            pattern_records = sample_records(pattern_records, rate, limit);
            pattern_nodes.push((pattern, start_pattern_index, pattern_count, pattern_records));
        }
        pattern_nodes
    }
}

fn get_src_records(
    graph: &LargeGraphDB<DefaultId, InternalId>, extend_steps: Vec<DefiniteExtendStep>,
    limit: Option<usize>,
) -> Vec<PatternRecord> {
    let mut extend_steps = extend_steps.into_iter();
    let first_extend_step = extend_steps.next().unwrap();
    let src_vertex_label = first_extend_step.get_target_vertex_label();
    let src_pattern_vertex_id = first_extend_step.get_target_vertex_id();
    let mut pattern_records: DynIter<PatternRecord> = Box::new(
        graph
            .get_all_vertices(Some(&vec![src_vertex_label as LabelId]))
            .map(|graph_vertex| PatternRecord::from_iter([(src_pattern_vertex_id, graph_vertex.get_id())])),
    );
    while let Some(extend_step) = extend_steps.next() {
        if let Some(upper_bound) = limit {
            pattern_records = Box::new(pattern_records.take(upper_bound));
        }
        pattern_records = Box::new(pattern_records.flat_map(move |pattern_record| {
            let target_vertex_label = extend_step.get_target_vertex_label();
            let mut intersect_vertices = BTreeSet::new();
            for (i, extend_edge) in extend_step.iter().enumerate() {
                let adjacent_vertices =
                    get_adj_vertices_set(graph, &pattern_record, extend_edge, target_vertex_label);
                intersect_vertices = intersect_sets(intersect_vertices, adjacent_vertices, i == 0);
            }
            let target_pattern_vertex_id = extend_step.get_target_vertex_id();
            intersect_vertices
                .into_iter()
                .map(move |adj_graph_vertex_id| {
                    let mut new_pattern_record = pattern_record.clone();
                    new_pattern_record.insert(target_pattern_vertex_id, adj_graph_vertex_id);
                    new_pattern_record
                })
        }));
    }
    pattern_records.collect()
}

fn get_adj_vertices_set(
    graph: &LargeGraphDB<DefaultId, InternalId>, pattern_record: &PatternRecord,
    extend_edge: &DefiniteExtendEdge, target_vertex_label: PatternLabelId,
) -> BTreeSet<DefaultId> {
    let src_pattern_vertex_id = extend_edge.get_src_vertex_id();
    let src_graph_vertex_id = *pattern_record
        .get(&src_pattern_vertex_id)
        .unwrap();
    let edge_label = extend_edge.get_edge_label();
    let direction = extend_edge.get_direction();
    graph
        .get_adj_vertices(src_graph_vertex_id, Some(&vec![edge_label as LabelId]), direction.into())
        .filter(|graph_vertex| graph_vertex.get_label()[0] == (target_vertex_label as LabelId))
        .map(|graph_vertex| graph_vertex.get_id())
        .collect()
}

fn intersect_sets<T: Clone + Ord>(set1: BTreeSet<T>, set2: BTreeSet<T>, is_start: bool) -> BTreeSet<T> {
    if is_start {
        set2
    } else {
        set1.intersection(&set2).cloned().collect()
    }
}

fn sample_records(records: Vec<PatternRecord>, rate: f64, limit: Option<usize>) -> Vec<PatternRecord> {
    let expected_len = if let Some(upper_bound) = limit {
        std::cmp::min(((records.len() as f64) * rate).floor() as usize, upper_bound)
    } else {
        ((records.len() as f64) * rate).floor() as usize
    };
    let step = (1.0 / rate).floor() as usize;
    records
        .into_iter()
        .enumerate()
        .filter(|&(i, _)| i % step == 0)
        .enumerate()
        .filter(|&(i, _)| i < expected_len)
        .map(|(_, (_, record))| record)
        .collect()
}

pub fn load_sample_graph(graph_path: &str) -> LargeGraphDB<DefaultId, InternalId> {
    info!("Read the sample graph data from {:?}.", graph_path);
    GraphDBConfig::default()
        .root_dir(graph_path)
        .partition(1)
        .schema_file(
            Path::new(graph_path)
                .join(DIR_GRAPH_SCHEMA)
                .join(FILE_SCHEMA),
        )
        .open()
        .expect("Open graph error")
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use crate::catalogue::catalog::get_definite_extend_steps_recursively;
    use crate::catalogue::extend_step::{ExtendEdge, ExtendStep};
    use crate::catalogue::pattern::{PatternEdge, PatternVertex};
    use crate::catalogue::sample::*;
    use crate::catalogue::PatternDirection;

    fn get_src_records_from_label(
        graph: &LargeGraphDB<DefaultId, InternalId>, vertex_label: PatternLabelId,
    ) -> Vec<PatternRecord> {
        graph
            .get_all_vertices(Some(&vec![vertex_label as LabelId]))
            .map(|graph_vertex| PatternRecord::from_iter([(0, graph_vertex.get_id())]))
            .collect()
    }

    fn get_adj_vertices_set(
        graph: &LargeGraphDB<DefaultId, InternalId>, src_pattern: &Pattern, extend_edge: &ExtendEdge,
        target_vertex_label: PatternLabelId, pattern_record: &PatternRecord,
    ) -> Option<BTreeSet<DefaultId>> {
        if let Some(src_pattern_vertex) =
            src_pattern.get_vertex_from_rank(extend_edge.get_src_vertex_rank())
        {
            let src_pattern_vertex_id = src_pattern_vertex.get_id();
            if let Some(&src_graph_vertex_id) = pattern_record.get(&src_pattern_vertex_id) {
                let direction = extend_edge.get_direction();
                let edge_label = extend_edge.get_edge_label();
                Some(
                    graph
                        .get_adj_vertices(
                            src_graph_vertex_id,
                            Some(&vec![edge_label as LabelId]),
                            direction.into(),
                        )
                        .filter(|graph_vertex| {
                            graph_vertex.get_label()[0] == (target_vertex_label as LabelId)
                        })
                        .map(|graph_vertex| graph_vertex.get_id())
                        .collect(),
                )
            } else {
                None
            }
        } else {
            None
        }
    }

    fn get_adj_vertices_sets(
        graph: &LargeGraphDB<DefaultId, InternalId>, src_pattern: &Pattern, extend_step: &ExtendStep,
        pattern_record: &PatternRecord,
    ) -> Option<Vec<(ExtendEdge, BTreeSet<DefaultId>)>> {
        let mut adj_vertices_sets = vec![];
        for extend_edge in extend_step.iter() {
            if let Some(adj_vertices_set) = get_adj_vertices_set(
                graph,
                src_pattern,
                extend_edge,
                extend_step.get_target_vertex_label(),
                pattern_record,
            ) {
                adj_vertices_sets.push((extend_edge.clone(), adj_vertices_set));
            } else {
                return None;
            }
        }
        Some(adj_vertices_sets)
    }

    fn intersect_adj_vertices_sets(
        mut adj_vertices_sets: Vec<(ExtendEdge, BTreeSet<DefaultId>)>,
    ) -> BTreeSet<DefaultId> {
        adj_vertices_sets.sort_by(|(_, vertices_set1), (_, vertices_set2)| {
            vertices_set1.len().cmp(&vertices_set2.len())
        });
        let (_, mut set_after_intersect) = adj_vertices_sets.pop().unwrap();
        for (_, adj_vertices_set) in adj_vertices_sets.into_iter() {
            set_after_intersect = set_after_intersect
                .intersection(&adj_vertices_set)
                .cloned()
                .collect();
        }
        set_after_intersect
    }

    #[test]
    fn test_create_sample_graph() {
        let sample_graph = load_sample_graph("../core/resource/test_graph");
        let total_count = sample_graph.count_all_vertices(None);
        let coach_count = sample_graph
            .get_all_vertices(Some(&vec![0]))
            .map(|vertex| vertex.get_id())
            .collect::<Vec<DefaultId>>()
            .len();
        let player_count = sample_graph
            .get_all_vertices(Some(&vec![1]))
            .map(|vertex| vertex.get_id())
            .collect::<Vec<DefaultId>>()
            .len();
        let fan_count = sample_graph
            .get_all_vertices(Some(&vec![2]))
            .map(|vertex| vertex.get_id())
            .collect::<Vec<DefaultId>>()
            .len();
        let ticket_count = sample_graph
            .get_all_vertices(Some(&vec![3]))
            .map(|vertex| vertex.get_id())
            .collect::<Vec<DefaultId>>()
            .len();
        assert_eq!(total_count, 30100);
        assert_eq!(coach_count, 10000);
        assert_eq!(player_count, 10000);
        assert_eq!(fan_count, 10000);
        assert_eq!(ticket_count, 100);
    }

    #[test]
    fn test_get_src_records_from_label() {
        let sample_graph = load_sample_graph("../core/resource/test_graph");
        let coach_src_records = get_src_records_from_label(&sample_graph, 0);
        assert_eq!(coach_src_records.len(), 10000);
        let player_src_records = get_src_records_from_label(&sample_graph, 1);
        assert_eq!(player_src_records.len(), 10000);
        let fan_src_records = get_src_records_from_label(&sample_graph, 2);
        assert_eq!(fan_src_records.len(), 10000);
        let ticket_src_records = get_src_records_from_label(&sample_graph, 3);
        assert_eq!(ticket_src_records.len(), 100);
    }

    #[test]
    fn test_get_adj_vertices_set() {
        let sample_graph = load_sample_graph("../core/resource/test_graph");
        let coach_src_record = get_src_records_from_label(&sample_graph, 0)[0].clone();
        let coach_src_pattern = Pattern::from(PatternVertex::new(0, 0));
        let player_src_record = get_src_records_from_label(&sample_graph, 1)[0].clone();
        let player_src_pattern = Pattern::from(PatternVertex::new(0, 1));
        let fan_src_record = get_src_records_from_label(&sample_graph, 2)[0].clone();
        let fan_src_pattern = Pattern::from(PatternVertex::new(0, 2));
        let ticket_src_record = get_src_records_from_label(&sample_graph, 3)[0].clone();
        let ticket_src_pattern = Pattern::from(PatternVertex::new(0, 3));
        let guide_out_extend_edge = ExtendEdge::new(0, 0, PatternDirection::Out);
        let guide_in_extend_edge = ExtendEdge::new(0, 0, PatternDirection::In);
        let loved_by_out_extend_edge = ExtendEdge::new(0, 1, PatternDirection::Out);
        let loved_by_in_extend_edge = ExtendEdge::new(0, 1, PatternDirection::In);
        let buy_out_extend_edge = ExtendEdge::new(0, 2, PatternDirection::Out);
        let buy_in_extend_edge = ExtendEdge::new(0, 2, PatternDirection::In);
        let players_from_coach_guide = get_adj_vertices_set(
            &sample_graph,
            &coach_src_pattern,
            &guide_out_extend_edge,
            1,
            &coach_src_record,
        )
        .unwrap();
        assert_eq!(players_from_coach_guide.len(), 100);
        let coaches_from_player_guide = get_adj_vertices_set(
            &sample_graph,
            &player_src_pattern,
            &guide_in_extend_edge,
            0,
            &player_src_record,
        )
        .unwrap();
        assert_eq!(coaches_from_player_guide.len(), 100);
        let fans_from_player_loved_by = get_adj_vertices_set(
            &sample_graph,
            &player_src_pattern,
            &loved_by_out_extend_edge,
            2,
            &player_src_record,
        )
        .unwrap();
        assert_eq!(fans_from_player_loved_by.len(), 100);
        let players_from_fan_loved_by = get_adj_vertices_set(
            &sample_graph,
            &fan_src_pattern,
            &loved_by_in_extend_edge,
            1,
            &fan_src_record,
        )
        .unwrap();
        assert_eq!(players_from_fan_loved_by.len(), 100);
        let tickets_from_fan_buy =
            get_adj_vertices_set(&sample_graph, &fan_src_pattern, &buy_out_extend_edge, 3, &fan_src_record)
                .unwrap();
        assert_eq!(tickets_from_fan_buy.len(), 1);
        let fans_from_ticket_buy = get_adj_vertices_set(
            &sample_graph,
            &ticket_src_pattern,
            &buy_in_extend_edge,
            2,
            &ticket_src_record,
        )
        .unwrap();
        assert_eq!(fans_from_ticket_buy.len(), 1);
    }

    #[test]
    fn test_get_adj_vertices_sets() {
        let sample_graph = load_sample_graph("../core/resource/test_graph");
        let coach_src_record = get_src_records_from_label(&sample_graph, 0)[0].clone();
        let coach_src_pattern = Pattern::from(PatternVertex::new(0, 0));
        let guide_out_extend_step = ExtendStep::new(1, vec![ExtendEdge::new(0, 0, PatternDirection::Out)]);
        let player_sets = get_adj_vertices_sets(
            &sample_graph,
            &coach_src_pattern,
            &guide_out_extend_step,
            &coach_src_record,
        )
        .unwrap();
        assert_eq!(player_sets.len(), 1);
        assert_eq!(player_sets[0].1.len(), 100);
    }

    #[test]
    fn test_intersect_adj_vertices_sets() {
        let sample_graph = load_sample_graph("../core/resource/test_graph");
        let coach_src_pattern = Pattern::from(PatternVertex::new(0, 0));
        let guide_out_extend_step = ExtendStep::new(1, vec![ExtendEdge::new(0, 0, PatternDirection::Out)]);
        let coach_src_record_0 = get_src_records_from_label(&sample_graph, 0)[0].clone();
        let coach_src_record_1 = get_src_records_from_label(&sample_graph, 0)[50].clone();
        let mut player_sets_0 = get_adj_vertices_sets(
            &sample_graph,
            &coach_src_pattern,
            &guide_out_extend_step,
            &coach_src_record_0,
        )
        .unwrap();
        let mut player_sets_1 = get_adj_vertices_sets(
            &sample_graph,
            &coach_src_pattern,
            &guide_out_extend_step,
            &coach_src_record_1,
        )
        .unwrap();
        player_sets_0.append(&mut player_sets_1);
        assert_eq!(intersect_adj_vertices_sets(player_sets_0).len(), 50);
    }

    #[test]
    fn test_sample_records() {
        let sample_graph = load_sample_graph("../core/resource/test_graph");
        let mut coach_src_records = get_src_records_from_label(&sample_graph, 0);
        let rate = 0.35;
        coach_src_records = sample_records(coach_src_records, rate, None);
        assert_eq!(coach_src_records.len(), 3500);
    }

    #[test]
    fn test_build_and_update_catalog() {
        let coach_vertex = PatternVertex::new(0, 0);
        let player_vertex = PatternVertex::new(1, 1);
        let fan_vertex = PatternVertex::new(2, 2);
        let ticket_vertex = PatternVertex::new(3, 3);
        let coach_guide_player_edge = PatternEdge::new(0, 0, coach_vertex, player_vertex);
        let player_lovded_by_fan_edge = PatternEdge::new(1, 1, player_vertex, fan_vertex);
        let fan_buy_ticket_edge = PatternEdge::new(2, 2, fan_vertex, ticket_vertex);
        let pattern = Pattern::try_from(vec![
            coach_guide_player_edge,
            player_lovded_by_fan_edge,
            fan_buy_ticket_edge,
        ])
        .unwrap();
        let mut catalog = Catalogue::build_from_pattern(&pattern);
        assert_eq!(catalog.get_patterns_num(), 10);
        assert_eq!(catalog.get_approaches_num(), 12);
        let sample_graph = Arc::new(load_sample_graph("../core/resource/test_graph"));
        catalog.estimate_graph(Arc::clone(&sample_graph), 0.1, Some(10000));
        println!("{:?}", pattern.generate_optimized_match_plan_greedily(&catalog));
    }

    #[test]
    fn test_get_src_records_from_extend_steps() {
        let coach_vertex = PatternVertex::new(0, 0);
        let player_vertex = PatternVertex::new(1, 1);
        let fan_vertex = PatternVertex::new(2, 2);
        let ticket_vertex = PatternVertex::new(3, 3);
        let coach_guide_player_edge = PatternEdge::new(0, 0, coach_vertex, player_vertex);
        let player_lovded_by_fan_edge = PatternEdge::new(1, 1, player_vertex, fan_vertex);
        let fan_buy_ticket_edge = PatternEdge::new(2, 2, fan_vertex, ticket_vertex);
        let pattern = Pattern::try_from(vec![
            coach_guide_player_edge,
            player_lovded_by_fan_edge,
            fan_buy_ticket_edge,
        ])
        .unwrap();
        let mut catalog = Catalogue::build_from_pattern(&pattern);
        let sample_graph = Arc::new(load_sample_graph("../core/resource/test_graph"));
        catalog.estimate_graph(Arc::clone(&sample_graph), 0.1, Some(10000));
        let pattern_index = catalog
            .get_pattern_index(&pattern.encode_to())
            .unwrap();
        let (extend_steps, _) = get_definite_extend_steps_recursively(&mut catalog, pattern_index, pattern);
        let pattern_records = get_src_records(&sample_graph, extend_steps, None);
        assert_eq!(pattern_records.len(), 1000000);
    }

    #[test]
    fn test_update_catalog_from_new_pattern() {
        let coach_vertex = PatternVertex::new(0, 0);
        let player_vertex = PatternVertex::new(1, 1);
        let fan_vertex = PatternVertex::new(2, 2);
        let ticket_vertex = PatternVertex::new(3, 3);
        let coach_guide_player_edge = PatternEdge::new(0, 0, coach_vertex, player_vertex);
        let player_lovded_by_fan_edge = PatternEdge::new(1, 1, player_vertex, fan_vertex);
        let fan_buy_ticket_edge = PatternEdge::new(2, 2, fan_vertex, ticket_vertex);
        let pattern1 =
            Pattern::try_from(vec![coach_guide_player_edge.clone(), player_lovded_by_fan_edge.clone()])
                .unwrap();
        let pattern2 = Pattern::try_from(vec![
            coach_guide_player_edge,
            player_lovded_by_fan_edge,
            fan_buy_ticket_edge,
        ])
        .unwrap();
        let sample_graph = Arc::new(load_sample_graph("../core/resource/test_graph"));
        let mut catalog = Catalogue::build_from_pattern(&pattern1);
        catalog.estimate_graph(Arc::clone(&sample_graph), 0.1, Some(10000));
        catalog.update_catalog_by_pattern(&pattern2);
        assert_eq!(catalog.get_patterns_num(), 10);
        assert_eq!(catalog.get_approaches_num(), 12);
        catalog.estimate_graph(Arc::clone(&sample_graph), 0.1, Some(10000));
    }
}
