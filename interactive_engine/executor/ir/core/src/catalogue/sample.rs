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

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::iter::FromIterator;
use std::path::Path;

use crate::catalogue::catalog::Catalogue;
use crate::catalogue::extend_step::{ExtendEdge, ExtendStep};
use crate::catalogue::pattern::{Pattern, PatternVertex};
use crate::catalogue::{PatternId, PatternLabelId};

use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::{DefaultId, GlobalStoreTrait, GraphDBConfig, InternalId, LabelId, LargeGraphDB};

type PatternRecord = HashMap<PatternId, DefaultId>;

pub fn create_sample_graph(graph_path: &str) -> LargeGraphDB<DefaultId, InternalId> {
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
    if let Some(src_pattern_vertex) = src_pattern.get_vertex_from_rank(extend_edge.get_src_vertex_rank()) {
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
                    .filter(|graph_vertex| graph_vertex.get_label()[0] == (target_vertex_label as LabelId))
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
    adj_vertices_sets
        .sort_by(|(_, vertices_set1), (_, vertices_set2)| vertices_set1.len().cmp(&vertices_set2.len()));
    let (_, mut set_after_intersect) = adj_vertices_sets.pop().unwrap();
    for (_, adj_vertices_set) in adj_vertices_sets.into_iter() {
        set_after_intersect = set_after_intersect
            .intersection(&adj_vertices_set)
            .cloned()
            .collect();
    }
    set_after_intersect
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

impl Catalogue {
    pub fn estimate_graph(
        &mut self, graph: &LargeGraphDB<DefaultId, InternalId>, rate: f64, limit: Option<usize>,
    ) {
        let mut relaxed_patterns_indices = BTreeSet::new();
        let mut pattern_counts_map = BTreeMap::new();
        let mut extend_counts_map = BTreeMap::new();
        let mut queue = VecDeque::new();
        for (entry_index, entry_label) in self.entries_iter() {
            let pattern = Pattern::from(PatternVertex::new(0, entry_label));
            let mut pattern_records = get_src_records_from_label(graph, entry_label);
            let pattern_count = pattern_records.len();
            pattern_records = sample_records(pattern_records, rate, limit);
            queue.push_back((pattern, entry_index, pattern_count, pattern_records));
        }
        while let Some((pattern, pattern_index, pattern_count, pattern_records)) = queue.pop_front() {
            relaxed_patterns_indices.insert(pattern_index);
            pattern_counts_map.insert(pattern_index, pattern_count);
            for approach in self.pattern_out_approaches_iter(pattern_index) {
                if let Some(extend_weight) = approach
                    .get_approach_weight()
                    .get_extend_weight()
                {
                    let extend_step = extend_weight.get_extend_step();
                    let target_pattern = pattern.extend(&extend_step).unwrap();
                    let mut extend_nums_counts = HashMap::new();
                    let mut target_pattern_records = Vec::new();
                    for pattern_record in pattern_records.iter() {
                        let adj_vertices_sets =
                            get_adj_vertices_sets(graph, &pattern, &extend_step, pattern_record).unwrap();
                        for (extend_edge, adj_vertices_set) in adj_vertices_sets.iter() {
                            *extend_nums_counts
                                .entry(extend_edge.clone())
                                .or_insert(0.0) += adj_vertices_set.len() as f64;
                        }
                        let adj_vertices_set = intersect_adj_vertices_sets(adj_vertices_sets);
                        target_pattern_records.extend(adj_vertices_set.iter().map(|&adj_vertex_id| {
                            let mut target_pattern_record = pattern_record.clone();
                            target_pattern_record.insert(target_pattern_record.len(), adj_vertex_id);
                            target_pattern_record
                        }));
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
                    let apprach_index = approach.get_approach_index();
                    let target_pattern_index = approach.get_target_pattern_index();
                    extend_counts_map.insert(apprach_index, extend_nums_counts);
                    if !relaxed_patterns_indices.contains(&target_pattern_index) {
                        queue.push_back((
                            target_pattern,
                            target_pattern_index,
                            target_pattern_count,
                            target_pattern_records,
                        ));
                    }
                }
            }
        }
        println!("{:?}", pattern_counts_map);
        println!("{:?}", extend_counts_map);
        for (pattern_index, pattern_count) in pattern_counts_map {
            self.set_pattern_count(pattern_index, pattern_count);
        }
        for (extend_index, extend_nums_counts) in extend_counts_map {
            self.set_extend_count(extend_index, extend_nums_counts);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use crate::catalogue::pattern::{PatternEdge, PatternVertex};
    use crate::catalogue::sample::*;
    use crate::catalogue::PatternDirection;

    #[test]
    fn test_create_sample_graph() {
        let sample_graph = create_sample_graph("../core/resource/test_graph");
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
        let sample_graph = create_sample_graph("../core/resource/test_graph");
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
        let sample_graph = create_sample_graph("../core/resource/test_graph");
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
        let sample_graph = create_sample_graph("../core/resource/test_graph");
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
        let sample_graph = create_sample_graph("../core/resource/test_graph");
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
        let sample_graph = create_sample_graph("../core/resource/test_graph");
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
        let sample_graph = create_sample_graph("../core/resource/test_graph");
        catalog.estimate_graph(&sample_graph, 0.1, Some(10000));
        println!("{:?}", pattern.generate_optimized_match_plan(&catalog));
    }
}
