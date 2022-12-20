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

use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use ir_common::KeyId;
use ir_core::catalogue::PatternDirection;
use ir_core::catalogue::pattern::*;
use ir_core::catalogue::{PatternId, PatternLabelId};
use ir_core::error::IrError;
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

pub fn query_params(
    tables: Vec<common_pb::NameOrId>, columns: Vec<common_pb::NameOrId>,
    predicate: Option<common_pb::Expression>,
) -> pb::QueryParams {
    pb::QueryParams {
        tables,
        columns,
        is_all_columns: false,
        limit: None,
        predicate,
        sample_ratio: 1.0,
        extra: HashMap::new(),
    }
}

pub fn build_edge_expand_opr(e_label: PatternLabelId, direction: PatternDirection) -> pb::EdgeExpand {
    let dir: i32 = match direction {
        PatternDirection::Out => 0,
        PatternDirection::In => 1,
    };
    pb::EdgeExpand {
        v_tag: None,
        direction: dir,
        params: Some(query_params(vec![e_label.into()], vec![], None)), //HASCREATOR
        expand_opt: 0,
        alias: None,
    }
}

fn new_pattern_edge(
    id: PatternId, label: PatternLabelId, start_v_id: PatternId, end_v_id: PatternId,
    start_v_label: PatternLabelId, end_v_label: PatternLabelId,
) -> PatternEdge {
    let start_vertex = PatternVertex::new(start_v_id, start_v_label);
    let end_vertex = PatternVertex::new(end_v_id, end_v_label);
    PatternEdge::new(id, label, start_vertex, end_vertex)
}

// Entity Label
const PLACE: PatternLabelId = 0;
const PERSON: PatternLabelId = 1;
const COMMENT: PatternLabelId = 2;
const POST: PatternLabelId = 3;
const FORUM: PatternLabelId = 4;
const ORGANISATION: PatternLabelId = 5;
const TAGCLASS: PatternLabelId = 6;
const TAG: PatternLabelId = 7;
const COUNTRY: PatternLabelId = 8;
const CITY: PatternLabelId = 9;
const CONTINENT: PatternLabelId = 10;
const COMPANY: PatternLabelId = 11;
const UNIVERSITY: PatternLabelId = 12;

// Relation Label
const HASCREATOR: PatternLabelId = 0;
const HASTAG: PatternLabelId = 1;
const REPLYOF: PatternLabelId = 3;
const CONTAINEROF: PatternLabelId = 5;
const HASMEMBER: PatternLabelId = 6;
const HASMODERATOR: PatternLabelId = 7;
const HASINTEREST: PatternLabelId = 10;
const ISLOCATEDIN: PatternLabelId = 11;
const KNOWS: PatternLabelId = 12;
const LIKES: PatternLabelId = 13;
const STUDYAT: PatternLabelId = 15;
const WORKAT: PatternLabelId = 16;
const ISPARTOF: PatternLabelId = 17;
const HASTYPE: PatternLabelId = 21;
const ISSUBCLASSOF: PatternLabelId = 22;



pub fn build_gb_query_1() -> Result<Pattern, IrError> {
    // define pb pattern message
    let pattern_edges: Vec<PatternEdge> = vec![
        new_pattern_edge(0, KNOWS, 0, 1, PERSON, PERSON),
        new_pattern_edge(1, STUDYAT, 0, 2, PERSON, UNIVERSITY),
        new_pattern_edge(2, STUDYAT, 1, 2, PERSON, UNIVERSITY),
    ];
    Pattern::try_from(pattern_edges)
}

pub fn build_gb_query_6() -> Result<Pattern, IrError> {
    // define pb pattern message
    let pattern_edges: Vec<PatternEdge> = vec![
        new_pattern_edge(0, ISLOCATEDIN, 0, 1, PERSON, CITY),
        new_pattern_edge(1, HASCREATOR, 2, 0, COMMENT, PERSON),
        new_pattern_edge(2, ISLOCATEDIN, 3, 1, PERSON, CITY),
        new_pattern_edge(3, LIKES, 3, 2, PERSON, COMMENT),
    ];
    Pattern::try_from(pattern_edges)
}

pub fn build_gb_query_9() -> Result<Pattern, IrError> {
    let pattern_edges: Vec<PatternEdge> = vec![
        new_pattern_edge(0, REPLYOF, 0, 1, COMMENT, COMMENT),
        new_pattern_edge(1, HASCREATOR, 0, 2, COMMENT, PERSON),
        new_pattern_edge(2, HASCREATOR, 1, 3, COMMENT, PERSON),
        new_pattern_edge(3, KNOWS, 3, 2, PERSON, PERSON),
        new_pattern_edge(4, HASMEMBER, 4, 2, FORUM, PERSON),
        new_pattern_edge(5, HASMEMBER, 4, 3, FORUM, PERSON),
    ];
    Pattern::try_from(pattern_edges)
}

pub fn build_gb_query_12() -> Result<Pattern, IrError> {
    let pattern_edges: Vec<PatternEdge> = vec![
        new_pattern_edge(0, HASCREATOR, 0, 1, COMMENT, PERSON),
        new_pattern_edge(1, LIKES, 1, 2, PERSON, COMMENT),
        new_pattern_edge(2, REPLYOF, 2, 0, COMMENT, COMMENT),
        new_pattern_edge(3, HASCREATOR, 2, 3, COMMENT, PERSON),
        new_pattern_edge(4, LIKES, 3, 0, PERSON, COMMENT),
    ];
    Pattern::try_from(pattern_edges)
}

pub fn build_gb_query_14() -> Result<Pattern, IrError> {
    let pattern_edges: Vec<PatternEdge> = vec![
        new_pattern_edge(0, HASCREATOR, 0, 1, COMMENT, PERSON),
        new_pattern_edge(1, HASTAG, 0, 2, COMMENT, TAG),
        new_pattern_edge(2, HASTAG, 3, 2, COMMENT, TAG),
        new_pattern_edge(3, REPLYOF, 3, 4, COMMENT, POST),
        new_pattern_edge(4, CONTAINEROF, 5, 4, FORUM, POST),
        new_pattern_edge(5, HASMEMBER, 5, 1, FORUM, PERSON),
    ];
    Pattern::try_from(pattern_edges)
}
