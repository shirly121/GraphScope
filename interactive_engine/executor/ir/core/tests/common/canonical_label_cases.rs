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

use std::collections::HashMap;
use std::convert::TryFrom;

use ir_common::KeyId;
use ir_core::catalogue::pattern::*;
use ir_core::catalogue::{PatternId, PatternLabelId};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

pub const TAG_A: KeyId = 0;
pub const TAG_B: KeyId = 1;
pub const TAG_C: KeyId = 2;
pub const TAG_D: KeyId = 3;
pub const TAG_E: KeyId = 4;
pub const TAG_F: KeyId = 5;
pub const TAG_G: KeyId = 6;
pub const TAG_H: KeyId = 7;

fn gen_edge_label_map(edges: Vec<String>) -> HashMap<String, PatternLabelId> {
    let mut rng = StdRng::from_seed([0; 32]);
    let mut values: Vec<PatternLabelId> = (0..=255).collect();
    values.shuffle(&mut rng);
    let mut edge_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut rank = 0;
    for edge in edges {
        if rank >= values.len() {
            panic!("Error in gen_edge_label_map: rank of out of scope");
        }
        edge_label_map.insert(edge, values[rank]);
        rank += 1;
    }

    edge_label_map
}

fn gen_group_ids(max_id: PatternId) -> Vec<PatternId> {
    let mut rng = rand::thread_rng();
    let mut ids: Vec<PatternId> = (0..=max_id).collect();
    ids.shuffle(&mut rng);
    ids
}

fn new_pattern_edge(
    id: PatternId, label: PatternLabelId, start_v_id: PatternId, end_v_id: PatternId,
    start_v_label: PatternLabelId, end_v_label: PatternLabelId,
) -> PatternEdge {
    let start_vertex = PatternVertex::new(start_v_id, start_v_label);
    let end_vertex = PatternVertex::new(end_v_id, end_v_label);
    PatternEdge::new(id, label, start_vertex, end_vertex)
}

// Test Cases for Index Ranking
pub fn build_pattern_rank_ranking_case1() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(1);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    let pattern_vec = vec![new_pattern_edge(
        0,
        *edge_label_map.get("A->A").unwrap(),
        *vertex_id_map.get("A0").unwrap(),
        *vertex_id_map.get("A1").unwrap(),
        *vertex_label_map.get("A").unwrap(),
        *vertex_label_map.get("A").unwrap(),
    )];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case2() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(1);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    let edge_ids = gen_group_ids(1);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case3() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(2);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    let edge_ids = gen_group_ids(1);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case4() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(2);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    let edge_ids = gen_group_ids(2);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case5() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(2);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    let edge_ids = gen_group_ids(2);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case6() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->A")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(2);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    let edge_ids = gen_group_ids(3);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("B->A").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case7() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(2);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    let edge_ids = gen_group_ids(3);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case8() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(3);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[3]);
    let edge_ids = gen_group_ids(3);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case9() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(3);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[3]);
    let edge_ids = gen_group_ids(4);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case10() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(3);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[3]);
    let edge_ids = gen_group_ids(5);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case11() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(4);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[3]);
    vertex_id_map.insert(String::from("B2"), vertex_ids[4]);
    let edge_ids = gen_group_ids(6);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case12() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(5);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[3]);
    vertex_id_map.insert(String::from("B2"), vertex_ids[4]);
    vertex_id_map.insert(String::from("B3"), vertex_ids[5]);
    let edge_ids = gen_group_ids(7);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B3").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case13() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> = gen_edge_label_map(vec![
        String::from("A->A"),
        String::from("A->B"),
        String::from("B->B"),
        String::from("B->A"),
    ]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(5);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[3]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[4]);
    vertex_id_map.insert(String::from("B2"), vertex_ids[5]);
    let edge_ids = gen_group_ids(7);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("B->A").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case14() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> = gen_edge_label_map(vec![
        String::from("A->A"),
        String::from("A->B"),
        String::from("B->B"),
        String::from("B->C"),
    ]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    vertex_label_map.insert(String::from("C"), 3);
    let vertex_ids = gen_group_ids(6);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[3]);
    vertex_id_map.insert(String::from("B2"), vertex_ids[4]);
    vertex_id_map.insert(String::from("B3"), vertex_ids[5]);
    vertex_id_map.insert(String::from("C0"), vertex_ids[6]);
    let edge_ids = gen_group_ids(8);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B3").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[8],
            *edge_label_map.get("B->C").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_id_map.get("C0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("C").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case15() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    vertex_label_map.insert(String::from("C"), 3);
    let vertex_ids = gen_group_ids(9);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[4]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[5]);
    vertex_id_map.insert(String::from("B2"), vertex_ids[6]);
    vertex_id_map.insert(String::from("C0"), vertex_ids[8]);
    vertex_id_map.insert(String::from("C1"), vertex_ids[9]);
    let edge_ids = gen_group_ids(8);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("B->C").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("C0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("C").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("B->C").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("C1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("C").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[8],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case16() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> = gen_edge_label_map(vec![
        String::from("A->A"),
        String::from("A->B"),
        String::from("B->C"),
        String::from("C->D"),
    ]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    vertex_label_map.insert(String::from("C"), 3);
    vertex_label_map.insert(String::from("D"), 4);
    let vertex_ids = gen_group_ids(9);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[4]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[5]);
    vertex_id_map.insert(String::from("B2"), vertex_ids[6]);
    vertex_id_map.insert(String::from("C0"), vertex_ids[7]);
    vertex_id_map.insert(String::from("C1"), vertex_ids[8]);
    vertex_id_map.insert(String::from("D0"), vertex_ids[9]);
    let edge_ids = gen_group_ids(9);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("B->C").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("C0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("C").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("B->C").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("C1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("C").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[8],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[9],
            *edge_label_map.get("C->D").unwrap(),
            *vertex_id_map.get("C1").unwrap(),
            *vertex_id_map.get("D0").unwrap(),
            *vertex_label_map.get("C").unwrap(),
            *vertex_label_map.get("D").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case17() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(5);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("A4"), vertex_ids[4]);
    vertex_id_map.insert(String::from("A5"), vertex_ids[5]);
    let edge_ids = gen_group_ids(4);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case17_even_num_chain() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(6);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("A4"), vertex_ids[4]);
    vertex_id_map.insert(String::from("A5"), vertex_ids[5]);
    vertex_id_map.insert(String::from("A6"), vertex_ids[6]);
    let edge_ids = gen_group_ids(5);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_id_map.get("A6").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case17_long_chain() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(10);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("A4"), vertex_ids[4]);
    vertex_id_map.insert(String::from("A5"), vertex_ids[5]);
    vertex_id_map.insert(String::from("A6"), vertex_ids[6]);
    vertex_id_map.insert(String::from("A7"), vertex_ids[7]);
    vertex_id_map.insert(String::from("A8"), vertex_ids[8]);
    vertex_id_map.insert(String::from("A9"), vertex_ids[9]);
    vertex_id_map.insert(String::from("A10"), vertex_ids[10]);
    let edge_ids = gen_group_ids(10);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_id_map.get("A6").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A6").unwrap(),
            *vertex_id_map.get("A7").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A7").unwrap(),
            *vertex_id_map.get("A8").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[8],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A8").unwrap(),
            *vertex_id_map.get("A9").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[9],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A9").unwrap(),
            *vertex_id_map.get("A10").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case17_special_id_situation_1() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_id_map.insert(String::from("A0"), 2);
    vertex_id_map.insert(String::from("A1"), 5);
    vertex_id_map.insert(String::from("A2"), 3);
    vertex_id_map.insert(String::from("A3"), 0);
    vertex_id_map.insert(String::from("A4"), 1);
    vertex_id_map.insert(String::from("A5"), 4);
    let pattern_vec = vec![
        new_pattern_edge(
            2,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            3,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            1,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            4,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            0,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case17_special_id_situation_2() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_id_map.insert(String::from("A0"), 2);
    vertex_id_map.insert(String::from("A1"), 1);
    vertex_id_map.insert(String::from("A2"), 3);
    vertex_id_map.insert(String::from("A3"), 0);
    vertex_id_map.insert(String::from("A4"), 4);
    vertex_id_map.insert(String::from("A5"), 5);
    let pattern_vec = vec![
        new_pattern_edge(
            0,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            1,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            2,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            4,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            3,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case18() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(5);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("A4"), vertex_ids[4]);
    vertex_id_map.insert(String::from("A5"), vertex_ids[5]);
    let edge_ids = gen_group_ids(5);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case19() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> = gen_edge_label_map(vec![
        String::from("A->A"),
        String::from("A->B"),
        String::from("A->C"),
        String::from("A->D"),
        String::from("A->E"),
    ]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    vertex_label_map.insert(String::from("C"), 3);
    vertex_label_map.insert(String::from("D"), 4);
    vertex_label_map.insert(String::from("E"), 5);
    let vertex_ids = gen_group_ids(13);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("A4"), vertex_ids[4]);
    vertex_id_map.insert(String::from("A5"), vertex_ids[5]);
    vertex_id_map.insert(String::from("A6"), vertex_ids[6]);
    vertex_id_map.insert(String::from("A7"), vertex_ids[7]);
    vertex_id_map.insert(String::from("A8"), vertex_ids[8]);
    vertex_id_map.insert(String::from("A9"), vertex_ids[9]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[10]);
    vertex_id_map.insert(String::from("C0"), vertex_ids[11]);
    vertex_id_map.insert(String::from("D0"), vertex_ids[12]);
    vertex_id_map.insert(String::from("E0"), vertex_ids[13]);
    let edge_ids = gen_group_ids(13);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A6").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A7").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[8],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A8").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[9],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_id_map.get("A9").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[10],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A6").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[11],
            *edge_label_map.get("A->D").unwrap(),
            *vertex_id_map.get("A7").unwrap(),
            *vertex_id_map.get("D0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("D").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[12],
            *edge_label_map.get("A->C").unwrap(),
            *vertex_id_map.get("A8").unwrap(),
            *vertex_id_map.get("C0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("C").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[13],
            *edge_label_map.get("A->E").unwrap(),
            *vertex_id_map.get("A9").unwrap(),
            *vertex_id_map.get("E0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("E").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case20() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(4);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("A4"), vertex_ids[4]);
    let edge_ids = gen_group_ids(11);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[8],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[9],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[10],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[11],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::try_from(pattern_vec).unwrap(), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case21() -> Pattern {
    let vertex0 = PatternVertex::new(0, 1);
    let vertex1 = PatternVertex::new(1, 2);
    let vertex3 = PatternVertex::new(3, 0);
    let vertex4 = PatternVertex::new(4, 1);
    let vertex5 = PatternVertex::new(5, 2);
    let edge0 = PatternEdge::new(0, 13, vertex0, vertex1);
    let edge2 = PatternEdge::new(2, 11, vertex0, vertex3);
    let edge4 = PatternEdge::new(4, 11, vertex4, vertex3);
    let edge6 = PatternEdge::new(6, 13, vertex4, vertex5);
    let edges = vec![edge0, edge2, edge4, edge6];
    Pattern::try_from(edges).unwrap()
}