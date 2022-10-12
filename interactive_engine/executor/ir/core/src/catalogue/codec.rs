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
use std::collections::BinaryHeap;
use std::convert::TryFrom;

use crate::catalogue::extend_step::{ExtendEdge, ExtendStep};
use crate::catalogue::pattern::{Pattern, PatternEdge, PatternVertex};
use crate::catalogue::{PatternDirection, PatternId, PatternLabelId};

impl Pattern {
    pub fn encode_to(&self) -> Vec<u8> {
        if self.get_edges_num() > 0 {
            let mut edge_ids: Vec<PatternId> = self
                .edges_iter()
                .map(|edge| edge.get_id())
                .collect();
            edge_ids.sort_by(|e1_id, e2_id| {
                let e1_rank = self.get_edge_rank(*e1_id).unwrap();
                let e2_rank = self.get_edge_rank(*e2_id).unwrap();
                e1_rank.cmp(&e2_rank)
            });
            let mut pattern_code = Vec::with_capacity(edge_ids.len() * 20);
            for edge_id in edge_ids {
                let edge = self.get_edge(edge_id).unwrap();
                let edge_label = edge.get_label();
                let start_vertex = edge.get_start_vertex();
                let start_vertex_rank = self
                    .get_vertex_rank(start_vertex.get_id())
                    .unwrap();
                let start_vertex_label = start_vertex.get_label();
                let end_vertex = edge.get_end_vertex();
                let end_vertex_rank = self
                    .get_vertex_rank(end_vertex.get_id())
                    .unwrap();
                let end_vertex_label = end_vertex.get_label();
                pattern_code.extend_from_slice(&label_to_u8_array(edge_label));
                pattern_code.extend_from_slice(&id_to_u8_array(start_vertex_rank));
                pattern_code.extend_from_slice(&label_to_u8_array(start_vertex_label));
                pattern_code.extend_from_slice(&id_to_u8_array(end_vertex_rank));
                pattern_code.extend_from_slice(&label_to_u8_array(end_vertex_label));
            }
            pattern_code
        } else if self.get_vertices_num() == 1 {
            Vec::from(label_to_u8_array(self.get_max_vertex_label().unwrap()))
        } else {
            vec![]
        }
    }

    pub fn decode_from(code: &[u8]) -> Option<Pattern> {
        if code.len() == 0 {
            None
        } else if code.len() % 20 == 0 {
            let mut pattern_edges = Vec::with_capacity(code.len() / 20);
            for i in 0..(code.len() / 20) {
                let edge_id = i;
                let k = i * 20;
                let edge_label = u8_array_to_label(&code[k..k + 4]);
                let start_vertex = PatternVertex::new(
                    u8_array_to_id(&code[k + 4..k + 8]),
                    u8_array_to_label(&code[k + 8..k + 12]),
                );
                let end_vertex = PatternVertex::new(
                    u8_array_to_id(&code[k + 12..k + 16]),
                    u8_array_to_label(&code[k + 16..k + 20]),
                );
                pattern_edges.push(PatternEdge::new(edge_id, edge_label, start_vertex, end_vertex));
            }
            Some(Pattern::try_from(pattern_edges).unwrap())
        } else if code.len() == 4 {
            let pattern_label = u8_array_to_label(code);
            Some(Pattern::from(PatternVertex::new(0, pattern_label)))
        } else {
            None
        }
    }
}

impl ExtendStep {
    pub fn encode_to(&self) -> Vec<u8> {
        let extend_edges = self.iter().collect::<BinaryHeap<&ExtendEdge>>();
        let mut extend_code = Vec::with_capacity(extend_edges.len() * 9 + 4);
        for extend_edge in extend_edges {
            extend_code.extend_from_slice(&id_to_u8_array(extend_edge.get_src_vertex_rank()));
            extend_code.extend_from_slice(&label_to_u8_array(extend_edge.get_edge_label()));
            extend_code.push(extend_edge.get_direction().into())
        }
        extend_code.extend_from_slice(&label_to_u8_array(self.get_target_vertex_label()));
        extend_code
    }

    pub fn decode_from(code: &[u8]) -> Option<ExtendStep> {
        if code.len() % 9 == 4 && code.len() != 4 {
            let extend_edges_num = code.len() / 9;
            let mut extend_edges = Vec::with_capacity(extend_edges_num);
            for i in 0..extend_edges_num {
                let k = i * 9;
                let src_vertex_rank = u8_array_to_id(&code[k..k + 4]);
                let edge_label = u8_array_to_label(&code[k + 4..k + 8]);
                let dir = if code[k + 8] == 0 {
                    PatternDirection::Out
                } else if code[k + 8] == 1 {
                    PatternDirection::In
                } else {
                    return None;
                };
                extend_edges.push(ExtendEdge::new(src_vertex_rank, edge_label, dir));
            }
            let target_vertex_label = u8_array_to_label(&code[code.len() - 4..code.len()]);
            Some(ExtendStep::new(target_vertex_label, extend_edges))
        } else {
            None
        }
    }
}

fn label_to_u8_array(label: PatternLabelId) -> [u8; 4] {
    u32_to_u8_array(label as u32)
}

fn id_to_u8_array(id: PatternId) -> [u8; 4] {
    u32_to_u8_array(id as u32)
}

fn u32_to_u8_array(num: u32) -> [u8; 4] {
    let first_u8 = ((num & 0xff000000) >> 24) as u8;
    let second_u8 = ((num & 0xff0000) >> 16) as u8;
    let third_u8 = ((num & 0xff00) >> 8) as u8;
    let fourth_u8 = (num & 0xff) as u8;
    [first_u8, second_u8, third_u8, fourth_u8]
}

fn u8_array_to_label(u8_array: &[u8]) -> PatternLabelId {
    u8_array_to_u32(u8_array) as PatternLabelId
}

fn u8_array_to_id(u8_array: &[u8]) -> PatternId {
    u8_array_to_u32(u8_array) as PatternId
}

fn u8_array_to_u32(u8_array: &[u8]) -> u32 {
    assert_eq!(u8_array.len(), 4);
    u8_array[3] as u32
        + ((u8_array[2] as u32) << 8)
        + ((u8_array[1] as u32) << 16)
        + ((u8_array[0] as u32) << 24)
}
