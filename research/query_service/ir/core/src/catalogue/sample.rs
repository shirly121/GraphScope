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

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::iter::FromIterator;
use std::path::Path;

use crate::catalogue::catalog::Catalogue;
use crate::catalogue::codec::Cipher;
use crate::catalogue::extend_step::{ExtendEdge, ExtendStep};
use crate::catalogue::pattern::{Pattern, PatternVertex};
use crate::catalogue::{PatternId, PatternLabelId};

use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::{DefaultId, GlobalStoreTrait, GraphDBConfig, InternalId, LabelId, LargeGraphDB};

type PatternRecord = HashMap<PatternId, DefaultId>;

fn create_sample_graph(graph_path: &str) -> LargeGraphDB<DefaultId, InternalId> {
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
) -> Option<Vec<(usize, BTreeSet<DefaultId>)>> {
    let mut adj_vertices_sets = vec![];
    for extend_edge in extend_step.iter() {
        if let Some(adj_vertices_set) = get_adj_vertices_set(
            graph,
            src_pattern,
            extend_edge,
            extend_step.get_target_vertex_label(),
            pattern_record,
        ) {
            adj_vertices_sets.push((extend_edge.get_src_vertex_rank(), adj_vertices_set));
        } else {
            return None;
        }
    }
    Some(adj_vertices_sets)
}

fn insersect_adj_vertices_sets(
    mut adj_vertices_sets: Vec<(usize, BTreeSet<DefaultId>)>,
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

fn sample_records(records: Vec<PatternRecord>, rate: f64, limit: usize) -> Vec<PatternRecord> {
    let expected_len = std::cmp::min(((records.len() as f64) * rate).floor() as usize, limit);
    let step = (1.0 / rate).floor() as usize;
    records
        .into_iter()
        .enumerate()
        .filter(|&(i, _)| i % step == 0 && i < expected_len)
        .map(|(_, record)| record)
        .collect()
}

impl Catalogue {
    pub fn estimate_graph(&mut self, graph: &LargeGraphDB<DefaultId, InternalId>, rate: f64, limit: usize) {
        let mut relaxed_patterns: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut queue = VecDeque::new();
        for (entry_label, entry_node) in self.entries_iter() {
            let pattern = Pattern::from(PatternVertex::new(0, entry_label));
            let pattern_code: Vec<u8> = Cipher::encode_to(&pattern, self.get_encoder());
            let records = get_src_records_from_label(graph, entry_label);
            let records_num = records.len();
            let records_after_sample = sample_records(records, rate, limit);
            let records_after_sample_num = records_after_sample.len();
            let actual_rate = (records_after_sample_num as f64) / (records_num as f64);
            queue.push_back((pattern, pattern_code, records_after_sample, actual_rate));
        }
        while let Some((pattern, code, records, rate)) = queue.pop_front() {}
    }
}
