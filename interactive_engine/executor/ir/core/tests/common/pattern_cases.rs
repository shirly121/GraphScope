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

use ir_common::expr_parse::str_to_expr_pb;
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use ir_common::KeyId;
use ir_core::catalogue::PatternDirection;
use ir_core::catalogue::pattern::*;
use ir_core::catalogue::{PatternId, PatternLabelId};
use ir_core::error::IrError;
use ir_core::plan::meta::PlanMeta;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

use crate::common::pattern_meta_cases::*;

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

/// The pattern looks like:
/// ```text
///      A <-> A
/// ```
/// where <-> means two edges
///
/// A's label's id is 0
///
/// The edges's labels' id are both 0
///
/// The left A has id 0
///
/// The right A has id 1
pub fn build_pattern_case1() -> Pattern {
    let vertex_1 = PatternVertex::new(0, 0);
    let vertex_2 = PatternVertex::new(1, 0);
    let edge_1 = PatternEdge::new(0, 0, vertex_1, vertex_2);
    let edge_2 = PatternEdge::new(1, 0, vertex_2, vertex_1);
    let pattern_vec = vec![edge_1, edge_2];
    Pattern::try_from(pattern_vec).unwrap()
}

/// The pattern looks like:
/// ```text
///         B
///       /   \
///      A <-> A
/// ```
/// where <-> means two edges
///
/// A's label id is 0, B's label id is 1
///
/// The edges between two As have label id 0
///
/// The edges between A and B have label id 1
///
/// The left A has id 0
///
/// The right A has id 1
///
/// B has id 2
pub fn build_pattern_case2() -> Pattern {
    let pattern_vec = vec![
        new_pattern_edge(0, 0, 0, 1, 0, 0),
        new_pattern_edge(1, 0, 1, 0, 0, 0),
        new_pattern_edge(2, 1, 0, 2, 0, 1),
        new_pattern_edge(3, 1, 1, 2, 0, 1),
    ];
    Pattern::try_from(pattern_vec).unwrap()
}

/// The pattern looks like:
/// ```text
///    B(2) -> B(3)
///     |       |
///     A(0) -> A(1)
/// ```
/// where <-> means two edges
///
/// Vertex Label Map:
/// ```text
///     A: 0, B: 1,
/// ```
/// Edge Label Map:
/// ```text
///     A-A: 0, A->B: 1, B-B: 2,
/// ```
pub fn build_pattern_case3() -> Pattern {
    let edge_ids = gen_group_ids(3);
    let pattern_vec = vec![
        new_pattern_edge(edge_ids[0], 0, 0, 1, 0, 0),
        new_pattern_edge(edge_ids[1], 1, 0, 2, 0, 1),
        new_pattern_edge(edge_ids[2], 1, 1, 3, 0, 1),
        new_pattern_edge(edge_ids[3], 2, 2, 3, 1, 1),
    ];
    Pattern::try_from(pattern_vec).unwrap()
}

/// The pattern looks like:
/// ```text
///     B(2)  -> B(3)
///     |        |
///     A(0) <-> A(1)
/// ```
/// where <-> means two edges
///
/// Vertex Label Map:
/// ```text
///     A: 0, B: 1,
/// ```
/// Edge Label Map:
/// ```text
///     A-A: 0, A->B: 1, B-B: 2,
/// ```
pub fn build_pattern_case4() -> Pattern {
    let edge_ids = gen_group_ids(4);
    let pattern_vec = vec![
        new_pattern_edge(edge_ids[0], 0, 0, 1, 0, 0),
        new_pattern_edge(edge_ids[1], 0, 1, 0, 0, 0),
        new_pattern_edge(edge_ids[2], 1, 0, 2, 0, 1),
        new_pattern_edge(edge_ids[3], 1, 1, 3, 0, 1),
        new_pattern_edge(edge_ids[4], 2, 2, 3, 1, 1),
    ];
    Pattern::try_from(pattern_vec).unwrap()
}

/// The pattern looks like
/// ```text
///         A(0) -> B(0)    A(1) <-> A(2)
///         |               |
/// C(0) -> B(1) <- A(3) -> B(2) <- C(1) <- D(0)
///         |
///         C(2)
/// ```
/// where <-> means bidirectional edges
///
/// Vertex Label Map
/// ```text
///     A: 3, B: 2, C: 1, D: 0
/// ```
/// Edge Label Map:
/// ```text
///     A-A: 20, A-B: 30, B-C: 15, C-D: 5
/// ```
pub fn build_pattern_case5() -> Pattern {
    let label_a = 3;
    let label_b = 2;
    let label_c = 1;
    let label_d = 0;
    let id_vec_a: Vec<PatternId> = vec![100, 200, 300, 400];
    let id_vec_b: Vec<PatternId> = vec![10, 20, 30];
    let id_vec_c: Vec<PatternId> = vec![1, 2, 3];
    let id_vec_d: Vec<PatternId> = vec![1000];
    let edge_ids = gen_group_ids(10);
    let pattern_vec = vec![
        new_pattern_edge(edge_ids[0], 15, id_vec_c[0], id_vec_b[1], label_c, label_b),
        new_pattern_edge(edge_ids[1], 30, id_vec_a[0], id_vec_b[1], label_a, label_b),
        new_pattern_edge(edge_ids[2], 15, id_vec_c[2], id_vec_b[1], label_c, label_b),
        new_pattern_edge(edge_ids[3], 30, id_vec_a[0], id_vec_b[0], label_a, label_b),
        new_pattern_edge(edge_ids[4], 30, id_vec_a[3], id_vec_b[1], label_a, label_b),
        new_pattern_edge(edge_ids[5], 30, id_vec_a[3], id_vec_b[2], label_a, label_b),
        new_pattern_edge(edge_ids[6], 30, id_vec_a[1], id_vec_b[2], label_a, label_b),
        new_pattern_edge(edge_ids[7], 20, id_vec_a[1], id_vec_a[2], label_a, label_a),
        new_pattern_edge(edge_ids[8], 20, id_vec_a[2], id_vec_a[1], label_a, label_a),
        new_pattern_edge(edge_ids[9], 15, id_vec_c[1], id_vec_b[2], label_c, label_b),
        new_pattern_edge(edge_ids[10], 5, id_vec_d[0], id_vec_c[1], label_d, label_c),
    ];
    Pattern::try_from(pattern_vec).unwrap()
}

/// The pattern looks like:
/// ```text
///     B <- A -> C
/// ```
/// Vertex Label Map:
/// ```text
///     A: 1, B: 2, C: 3
/// ```
/// Edge Label Map:
/// ```text
///     A->B: 1, A->C: 2
/// ```
pub fn build_pattern_case6() -> Pattern {
    let pattern_edge1 = new_pattern_edge(0, 1, 0, 1, 1, 2);
    let pattern_edge2 = new_pattern_edge(1, 2, 0, 2, 1, 3);
    let pattern_vec = vec![pattern_edge1, pattern_edge2];
    Pattern::try_from(pattern_vec).unwrap()
}

/// The pattern looks like:
/// ```text
///         A
///        /|\
///       / D \
///      //  \ \
///     B  ->  C
/// ```
/// Vertex Label Map:
/// ```text
///     A: 1, B: 2, C: 3, D: 4
/// ```
/// Edge Label Map:
/// ```text
///     A->B: 0, A->C: 1, B->C: 2, A->D: 3, B->D: 4, D->C: 5
/// ```
pub fn build_pattern_case7() -> Pattern {
    let edge_1 = new_pattern_edge(0, 1, 0, 1, 1, 2);
    let edge_2 = new_pattern_edge(1, 2, 0, 2, 1, 3);
    let edge_3 = new_pattern_edge(2, 3, 1, 2, 2, 3);
    let edge_4 = new_pattern_edge(3, 4, 0, 3, 1, 4);
    let edge_5 = new_pattern_edge(4, 5, 1, 3, 2, 4);
    let edge_6 = new_pattern_edge(5, 6, 3, 2, 4, 3);
    let pattern_edges = vec![edge_1, edge_2, edge_3, edge_4, edge_5, edge_6];
    Pattern::try_from(pattern_edges).unwrap()
}

/// The pattern looks like:
/// ```text
///     A -> A -> B
/// ```
/// Vertex Label Map:
/// ```text
///     A: 1, B: 2
/// ```
/// Edge Label Map:
/// ```text
///     A->A: 0, A->B: 3
/// ```
pub fn build_pattern_case8() -> Pattern {
    let edge_1 = new_pattern_edge(0, 0, 0, 1, 1, 1);
    let edge_2 = new_pattern_edge(1, 3, 1, 2, 1, 2);
    let pattern_edges = vec![edge_1, edge_2];
    Pattern::try_from(pattern_edges).unwrap()
}

/// The pattern looks like:
/// ```text
///          C
///       /  |   \
///     A -> A -> B
/// ```
/// Vertex Label Map:
/// ```text
///     A: 1, B: 2, C: 3
/// ```
/// Edge Label Map:
/// ```text
///     A->A: 0, A->C: 1, B->C: 2, A->B: 3
/// ```
pub fn build_pattern_case9() -> Pattern {
    let edge_1 = new_pattern_edge(0, 0, 0, 1, 1, 1);
    let edge_2 = new_pattern_edge(1, 3, 1, 2, 1, 2);
    let edge_3 = new_pattern_edge(2, 1, 0, 3, 1, 3);
    let edge_4 = new_pattern_edge(3, 1, 1, 3, 1, 3);
    let edge_5 = new_pattern_edge(4, 2, 2, 3, 2, 3);
    let pattern_edges = vec![edge_1, edge_2, edge_3, edge_4, edge_5];
    Pattern::try_from(pattern_edges).unwrap()
}

/// Pattern from modern schema file
///
/// Person only Pattern
pub fn build_modern_pattern_case1() -> Pattern {
    Pattern::from(PatternVertex::new(0, 0))
}

/// Software only Pattern
pub fn build_modern_pattern_case2() -> Pattern {
    Pattern::from(PatternVertex::new(0, 1))
}

/// The pattern looks like:
/// ```text
///     Person -> knows -> Person
/// ```
pub fn build_modern_pattern_case3() -> Pattern {
    let pattern_edge = new_pattern_edge(0, 0, 0, 1, 0, 0);
    Pattern::try_from(vec![pattern_edge]).unwrap()
}

/// The pattern looks like:
/// ```text
///     Person -> created -> Software
/// ```
pub fn build_modern_pattern_case4() -> Pattern {
    let pattern_edge = new_pattern_edge(0, 1, 0, 1, 0, 1);
    Pattern::try_from(vec![pattern_edge]).unwrap()
}

/// The pattern looks like:
///```text
///           Software
///   create/         \create
///  Person -> knows -> Person
/// ```
/// create and knows are edge labels
///
/// Software and Person are vertex labels
pub fn build_modern_pattern_case5() -> Pattern {
    let pattern_edge1 = new_pattern_edge(0, 0, 0, 1, 0, 0);
    let pattern_edge2 = new_pattern_edge(1, 1, 0, 2, 0, 1);
    let pattern_edge3 = new_pattern_edge(2, 1, 1, 2, 0, 1);
    Pattern::try_from(vec![pattern_edge1, pattern_edge2, pattern_edge3]).unwrap()
}

/// Pattern from ldbc schema file
/// ```text
///     Person -> knows -> Person
/// ```
pub fn build_ldbc_pattern_case1() -> Pattern {
    let pattern_edge = new_pattern_edge(0, 12, 0, 1, 1, 1);
    Pattern::try_from(vec![pattern_edge]).unwrap()
}

/// Pattern from ldbc schema file and build from pb::Pattern message
/// ```text
///           Person
///     knows/      \knows
///     Person  ->  Person
/// ```
/// knows is the edge label
///
/// Person is the vertex label
pub fn build_ldbc_pattern_from_pb_case1() -> Result<Pattern, IrError> {
    let ldbc_pattern_mata = get_ldbc_pattern_meta();
    // define pb pattern message
    let expand_opr = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
        expand_opt: 0,
        alias: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
        ],
    };
    Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
}

/// Pattern from ldbc schema file and build from pb::Pattern message
/// ```text
///           University
///     study at/      \study at
///        Person  ->   Person
/// ```
pub fn build_ldbc_pattern_from_pb_case2() -> Result<Pattern, IrError> {
    let ldbc_pattern_mata = get_ldbc_pattern_meta();
    // define pb pattern message
    let expand_opr1 = pb::EdgeExpand {
        v_tag: None,
        direction: 1,                                              // in
        params: Some(query_params(vec![15.into()], vec![], None)), //STUDYAT
        expand_opt: 0,
        alias: None,
    };
    let expand_opr2 = pb::EdgeExpand {
        v_tag: None,
        direction: 1,                                              // in
        params: Some(query_params(vec![15.into()], vec![], None)), //STUDYAT
        expand_opt: 0,
        alias: None,
    };
    let expand_opr3 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr1)),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr2)),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr3)),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
        ],
    };
    Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
}

/// Pattern from ldbc schema file and build from pb::Pattern message
/// 4 Persons know each other
pub fn build_ldbc_pattern_from_pb_case3() -> Result<Pattern, IrError> {
    let ldbc_pattern_mata = get_ldbc_pattern_meta();
    // define pb pattern message
    let expand_opr = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_C.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
        ],
    };
    Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
}

/// Pattern from ldbc schema file and build from pb::Pattern message
/// ```text
///             City
///      lives/     \lives
///     Person      Person
///     likes \      / has creator
///           Comment
/// ```
pub fn build_ldbc_pattern_from_pb_case4() -> Result<Pattern, IrError> {
    let ldbc_pattern_mata = get_ldbc_pattern_meta();
    // define pb pattern message
    let expand_opr1 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
        expand_opt: 0,
        alias: None,
    };
    let expand_opr2 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
        expand_opt: 0,
        alias: None,
    };
    let expand_opr3 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![13.into()], vec![], None)), //LIKES
        expand_opt: 0,
        alias: None,
    };
    let expand_opr4 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                             // out
        params: Some(query_params(vec![0.into()], vec![], None)), //HASCREATOR
        expand_opt: 0,
        alias: None,
    };
    let select_comment =
        pb::Select { predicate: Some(str_to_expr_pb("@.~label == 2".to_string()).unwrap()) };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr1)),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr2)),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr3)),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_D.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr4)),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_D.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Select(select_comment)),
                }],
                end: None,
                join_kind: 0,
            },
        ],
    };
    Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
}

/// Pattern from ldbc schema file and build from pb::Pattern message
/// ```text
///           Person
///     knows/      \knows
///    knows/       \knows
///   Person knows->knows Person
/// ```
/// knows->knows represents there are two edges with "knows" label between two people
pub fn build_ldbc_pattern_from_pb_case5() -> Result<Pattern, IrError> {
    let ldbc_pattern_mata = get_ldbc_pattern_meta();
    // define pb pattern message
    let expand_opr0 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let expand_opr1 = pb::EdgeExpand {
        v_tag: None,
        direction: 1,                                              // in
        params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                    },
                ],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                    },
                ],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                    },
                ],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
        ],
    };
    Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
}

pub fn build_ldbc_pattern_from_pb_case6() -> Result<Pattern, IrError> {
    let ldbc_pattern_mata = get_ldbc_pattern_meta();
    // define pb pattern message
    let expand_opr0 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
        expand_opt: 0,
        alias: None,
    };
    let expand_opr1 = pb::EdgeExpand {
        v_tag: None,
        direction: 1,                                              // in
        params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
        expand_opt: 0,
        alias: None,
    };
    let expand_opr2 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let expand_opr3 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![13.into()], vec![], None)), //LIKES
        expand_opt: 0,
        alias: None,
    };
    let expand_opr4 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                             // out
        params: Some(query_params(vec![0.into()], vec![], None)), //HASCREATOR
        expand_opt: 0,
        alias: None,
    };
    let select_comment =
        pb::Select { predicate: Some(str_to_expr_pb("@.~label == 2".to_string()).unwrap()) };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                    },
                ],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr3.clone())),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_D.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr4.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_D.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Select(select_comment)),
                }],
                end: None,
                join_kind: 0,
            },
        ],
    };
    Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
}

pub fn build_ldbc_bi3() -> Result<Pattern, IrError> {
    let ldbc_pattern_mata = get_ldbc_pattern_meta();
    // define pb pattern message
    let forum_has_moderator_person = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                             // out
        params: Some(query_params(vec![7.into()], vec![], None)), //HAS_MODERATOR
        expand_opt: 0,
        alias: None,
    };
    let person_is_located_in_place = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
        expand_opt: 0,
        alias: None,
    };
    let place_is_part_of_place = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![17.into()], vec![], None)), //ISPARTOF
        expand_opt: 0,
        alias: None,
    };
    let forum_container_of_post = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                             // out
        params: Some(query_params(vec![5.into()], vec![], None)), //CONTAINER_OF
        expand_opt: 0,
        alias: None,
    };
    let comment_reply_of_post = pb::EdgeExpand {
        v_tag: None,
        direction: 1,                                             // in
        params: Some(query_params(vec![3.into()], vec![], None)), //REPLY_OF
        expand_opt: 0,
        alias: None,
    };
    let comment_has_tag_tag = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                             // out
        params: Some(query_params(vec![1.into()], vec![], None)), //HAS_TAG
        expand_opt: 0,
        alias: None,
    };
    let tag_has_type_tagclass = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![21.into()], vec![], None)), //HAS_TYPE
        expand_opt: 0,
        alias: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(forum_has_moderator_person.clone())),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(person_is_located_in_place.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_C.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(place_is_part_of_place.clone())),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(forum_container_of_post.clone())),
                }],
                end: Some(TAG_E.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_E.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(comment_reply_of_post.clone())),
                }],
                end: Some(TAG_F.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_F.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(comment_has_tag_tag.clone())),
                }],
                end: Some(TAG_G.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_G.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(tag_has_type_tagclass.clone())),
                }],
                end: Some(TAG_H.into()),
                join_kind: 0,
            },
        ],
    };

    Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
}

pub fn build_ldbc_bi11() -> Result<Pattern, IrError> {
    let ldbc_pattern_mata = get_ldbc_pattern_meta();
    // define pb pattern message
    let expand_opr0 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // both
        params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let expand_opr1 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
        expand_opt: 0,
        alias: None,
    };
    let expand_opr2 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![17.into()], vec![], None)), //ISPARTOF
        expand_opt: 0,
        alias: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                }],
                end: Some(TAG_E.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_C.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                }],
                end: Some(TAG_F.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_D.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                }],
                end: Some(TAG_H.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_E.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                }],
                end: Some(TAG_H.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_F.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                }],
                end: Some(TAG_H.into()),
                join_kind: 0,
            },
        ],
    };
    
    Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
}
