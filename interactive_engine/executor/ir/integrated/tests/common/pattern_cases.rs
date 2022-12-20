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

use std::fs::File;

use ir_common::expr_parse::str_to_expr_pb;
use ir_common::generated::algebra as pb;
use ir_core::catalogue::pattern::Pattern;
use ir_core::catalogue::pattern_meta::PatternMeta;
use ir_core::error::{IrError, IrResult};
use ir_core::plan::meta::PlanMeta;
use ir_core::{plan::meta::Schema, JsonIO};

use crate::common::test::*;

pub fn get_ldbc_pattern_meta() -> PatternMeta {
    let ldbc_schema_file = File::open("../core/resource/ldbc_schema_broad.json").unwrap();
    let ldbc_schema = Schema::from_json(ldbc_schema_file).unwrap();
    PatternMeta::from(ldbc_schema)
}

// Pattern from ldbc schema file and build from pb::Pattern message
//           Person
//     knows/      \knows
//      Person -> Person
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

// Pattern from ldbc schema file and build from pb::Pattern message
//           University
//     study at/      \study at
//      Person   ->    Person
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

// Pattern from ldbc schema file and build from pb::Pattern message
// 4 Persons know each other
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

// Pattern from ldbc schema file and build from pb::Pattern message
//             City
//      lives/     \lives
//     Person      Person
//     likes \      / has creator
//           Comment
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

pub fn build_ldbc_pattern_from_pb_case5() -> Result<Pattern, IrError> {
    let ldbc_pattern_mata = get_ldbc_pattern_meta();
    // define pb pattern message
    let person_knows_person = pb::EdgeExpand {
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
                    item: Some(pb::pattern::binder::Item::Edge(person_knows_person.clone())),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(person_knows_person.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(person_knows_person.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(person_knows_person.clone())),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(person_knows_person.clone())),
                }],
                end: Some(TAG_E.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_D.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(person_knows_person.clone())),
                }],
                end: Some(TAG_E.into()),
                join_kind: 0,
            },
        ],
    };
    Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
}

pub fn build_ldbc_pattern_from_pb_case6() -> Result<Pattern, IrError> {
    let ldbc_pattern_mata = get_ldbc_pattern_meta();
    // define pb pattern message
    let expand_opr = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
        expand_opt: 0,
        alias: None,
    };
    let edge_expand_has_interest = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![10.into()], vec![], None)), // HAS_INTEREST
        expand_opt: 0,
        alias: None,
    };
    let edge_expand_has_type = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![21.into()], vec![], None)), // HAS_INTEREST
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
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(edge_expand_has_interest.clone())),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_D.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(edge_expand_has_type.clone())),
                }],
                end: Some(TAG_E.into()),
                join_kind: 0,
            },
        ],
    };
    Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
}

pub fn build_ldbc_bi3() -> IrResult<Pattern> {
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

pub fn build_ldbc_bi4_subtask_1() -> IrResult<Pattern> {
    let ldbc_pattern_mata = get_ldbc_pattern_meta();
    // Define Pattern Edges
    let forum_has_member_person = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                             // out
        params: Some(query_params(vec![6.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let person_is_located_in_place = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![11.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let place_is_part_of_place = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![17.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(forum_has_member_person.clone())),
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
        ],
    };
    Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
}

pub fn build_ldbc_bi4_subtask_2() -> IrResult<Pattern> {
    let ldbc_pattern_mata = get_ldbc_pattern_meta();
    // Define Pattern Edges
    let forum_container_of_post = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                             // out
        params: Some(query_params(vec![5.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let post_reply_of_message = pb::EdgeExpand {
        v_tag: None,
        direction: 1,                                             // in
        params: Some(query_params(vec![3.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let message_has_creator_person = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                             // out
        params: Some(query_params(vec![0.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let person_has_member_forum = pb::EdgeExpand {
        v_tag: None,
        direction: 1,                                             // in
        params: Some(query_params(vec![6.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(forum_container_of_post.clone())),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(post_reply_of_message.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_C.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(message_has_creator_person.clone())),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_D.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(person_has_member_forum.clone())),
                }],
                end: Some(TAG_E.into()),
                join_kind: 0,
            },
        ],
    };
    Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
}

pub fn build_ldbc_bi11() -> IrResult<Pattern> {
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
