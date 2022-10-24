//
//! Copyright 2021 Alibaba Group Holding Limited.
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
//!

mod common;

#[cfg(test)]
mod test {
    use std::convert::TryFrom;
    use std::convert::TryInto;
    use std::fs::File;
    use std::sync::Arc;
    use std::time::Instant;

    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_core::catalogue::catalog::get_definite_extend_steps_recursively;
    use ir_core::catalogue::catalog::Catalogue;
    use ir_core::catalogue::pattern::Pattern;
    use ir_core::catalogue::pattern::PatternEdge;
    use ir_core::catalogue::pattern::PatternVertex;
    use ir_core::catalogue::pattern_meta::PatternMeta;
    use ir_core::catalogue::sample::{get_src_records, load_sample_graph};
    use ir_core::error::IrError;
    use ir_core::plan::logical::LogicalPlan;
    use ir_core::plan::meta::PlanMeta;
    use ir_core::plan::physical::AsPhysical;
    use ir_core::{plan::meta::Schema, JsonIO};
    use pegasus_client::builder::JobBuilder;

    use crate::common::test::*;
    fn print_pb_logical_plan(pb_plan: &pb::LogicalPlan) {
        let mut id = 0;
        pb_plan.nodes.iter().for_each(|node| {
            println!("ID: {:?}, {:?}", id, node);
            id += 1;
        });
        println!("Roots: {:?}", pb_plan.roots);
    }
    pub fn get_ldbc_pattern_meta() -> PatternMeta {
        let ldbc_schema_file = File::open("../core/resource/ldbc_schema_edit.json").unwrap();
        let ldbc_schema = Schema::from_json(ldbc_schema_file).unwrap();
        PatternMeta::from(ldbc_schema)
    }

    // Pattern from ldbc schema file and build from pb::Pattern message
    //           Person
    //     knows/      \knows
    //      Person -> Person
    pub fn build_ldbc_pattern_gb3() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
            expand_opt: 0,
            alias: None,
        };
        let select_person =
            pb::Select { predicate: Some(str_to_expr_pb("@.~label == 1".to_string()).unwrap()) };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_person.clone())),
                        },
                    ],
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
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    // Pattern from ldbc schema file and build from pb::Pattern message
    //           University
    //     study at/      \study at
    //      Person   ->    Person
    pub fn build_ldbc_pattern_gb1() -> Result<Pattern, IrError> {
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
    pub fn build_ldbc_pattern_gb13() -> Result<Pattern, IrError> {
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
    pub fn build_ldbc_pattern_gb6() -> Result<Pattern, IrError> {
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
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    fn build_ldbc_bi11() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr0 = pb::EdgeExpand {
            v_tag: None,
            direction: 2,                                              // both
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

    pub fn build_ldbc_pattern_gb4() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr1 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // in
            params: Some(query_params(vec![1.into()], vec![], None)), //HASTAG
            expand_opt: 0,
            alias: None,
        };
        let expand_opr2 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // in
            params: Some(query_params(vec![0.into()], vec![], None)), //HASCREATOR
            expand_opt: 0,
            alias: None,
        };
        let expand_opr3 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![10.into()], vec![], None)), //HASINTEREST
            expand_opt: 0,
            alias: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2)),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1)),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr3)),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_pattern_gb2() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
            expand_opt: 0,
            alias: None,
        };
        let select_person =
            pb::Select { predicate: Some(str_to_expr_pb("@.~label == 1".to_string()).unwrap()) };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_person.clone())),
                        },
                    ],
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
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_pattern_gb5() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
            expand_opt: 0,
            alias: None,
        };
        let select_person =
            pb::Select { predicate: Some(str_to_expr_pb("@.~label == 1".to_string()).unwrap()) };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_person.clone())),
                        },
                    ],
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
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                    }],
                    end: Some(TAG_A.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_pattern_gb7() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr1 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
            expand_opt: 0,
            alias: None,
        };
        let expand_opr2 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![0.into()], vec![], None)), //HASCREATOR
            expand_opt: 0,
            alias: None,
        };
        let expand_opr4 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                             // out
            params: Some(query_params(vec![3.into()], vec![], None)), //REPLYOF
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
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr4.clone())),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2)),
                    }],
                    end: Some(TAG_A.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_pattern_gb8() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr1 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
            expand_opt: 0,
            alias: None,
        };
        let expand_opr2 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![10.into()], vec![], None)), //HASINTEREST
            expand_opt: 0,
            alias: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1)),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2)),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_pattern_gb9() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr1 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
            expand_opt: 0,
            alias: None,
        };
        let expand_opr2 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![0.into()], vec![], None)), //HASCREATOR
            expand_opt: 0,
            alias: None,
        };
        let expand_opr3 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![6.into()], vec![], None)), //HASMEMBER
            expand_opt: 0,
            alias: None,
        };
        let expand_opr4 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                             // out
            params: Some(query_params(vec![3.into()], vec![], None)), //REPLYOF
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
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr4.clone())),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2)),
                    }],
                    end: Some(TAG_A.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_E.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr3.clone())),
                    }],
                    end: Some(TAG_A.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_E.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr3)),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_pattern_gb10() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr1 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
            expand_opt: 0,
            alias: None,
        };
        let expand_opr2 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![0.into()], vec![], None)), //HASCREATOR
            expand_opt: 0,
            alias: None,
        };
        let expand_opr3 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![1.into()], vec![], None)), //HASTAG
            expand_opt: 0,
            alias: None,
        };
        let expand_opr4 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                             // out
            params: Some(query_params(vec![3.into()], vec![], None)), //REPLYOF
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
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr4.clone())),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2)),
                    }],
                    end: Some(TAG_A.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr3.clone())),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr3)),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_pattern_gb11() -> Result<Pattern, IrError> {
        let mut edges = Vec::new();
        let v1 = PatternVertex::new(0, 7);
        let v2 = PatternVertex::new(1, 2);
        let v3 = PatternVertex::new(2, 2);
        let v4 = PatternVertex::new(3, 1);
        edges.push(PatternEdge::new(1, 0, v2, v4));
        edges.push(PatternEdge::new(2, 13, v4, v3));
        edges.push(PatternEdge::new(3, 1, v2, v1));
        edges.push(PatternEdge::new(4, 1, v3, v1));
        edges.push(PatternEdge::new(5, 10, v4, v1));
        Pattern::try_from(edges)
    }

    pub fn build_ldbc_pattern_gb12() -> Result<Pattern, IrError> {
        let mut edges = Vec::new();
        let v1 = PatternVertex::new(0, 2);
        let v2 = PatternVertex::new(1, 2);
        let v3 = PatternVertex::new(2, 1);
        let v4 = PatternVertex::new(3, 1);
        edges.push(PatternEdge::new(1, 0, v2, v4));
        edges.push(PatternEdge::new(2, 0, v1, v3));
        edges.push(PatternEdge::new(3, 13, v4, v1));
        edges.push(PatternEdge::new(4, 13, v3, v2));
        edges.push(PatternEdge::new(5, 3, v1, v2));
        Pattern::try_from(edges)
    }

    pub fn build_ldbc_pattern_gb14() -> Result<Pattern, IrError> {
        let mut edges = Vec::new();
        let v1 = PatternVertex::new(0, 2);
        let v2 = PatternVertex::new(1, 3);
        let v3 = PatternVertex::new(2, 4);
        let v4 = PatternVertex::new(3, 1);
        let v5 = PatternVertex::new(4, 2);
        let v6 = PatternVertex::new(5, 7);
        edges.push(PatternEdge::new(1, 3, v1, v2));
        edges.push(PatternEdge::new(2, 5, v3, v2));
        edges.push(PatternEdge::new(3, 6, v4, v3));
        edges.push(PatternEdge::new(4, 0, v5, v4));
        edges.push(PatternEdge::new(5, 1, v5, v6));
        edges.push(PatternEdge::new(6, 1, v1, v6));
        Pattern::try_from(edges)
    }

    pub fn build_ldbc_pattern_gb15() -> Result<Pattern, IrError> {
        let mut edges = Vec::new();
        let v1 = PatternVertex::new(0, 1);
        let v2 = PatternVertex::new(1, 2);
        let v3 = PatternVertex::new(2, 3);
        let v4 = PatternVertex::new(3, 4);
        let v5 = PatternVertex::new(4, 1);
        edges.push(PatternEdge::new(1, 0, v2, v1));
        edges.push(PatternEdge::new(2, 3, v2, v3));
        edges.push(PatternEdge::new(3, 5, v4, v3));
        edges.push(PatternEdge::new(4, 6, v4, v5));
        Pattern::try_from(edges)
    }

    pub fn build_ldbc_pattern_gb16() -> Result<Pattern, IrError> {
        let mut edges = Vec::new();
        let v1 = PatternVertex::new(0, 6);
        let v2 = PatternVertex::new(1, 7);
        let v3 = PatternVertex::new(2, 2);
        let v4 = PatternVertex::new(3, 3);
        let v5 = PatternVertex::new(4, 4);
        let v6 = PatternVertex::new(5, 1);
        let v7 = PatternVertex::new(6, 9);
        let v8 = PatternVertex::new(7, 8);
        edges.push(PatternEdge::new(1, 21, v2, v1));
        edges.push(PatternEdge::new(2, 1, v3, v2));
        edges.push(PatternEdge::new(3, 3, v3, v4));
        edges.push(PatternEdge::new(4, 5, v5, v4));
        edges.push(PatternEdge::new(5, 7, v5, v6));
        edges.push(PatternEdge::new(6, 11, v6, v7));
        edges.push(PatternEdge::new(7, 17, v7, v8));
        Pattern::try_from(edges)
    }

    pub fn build_ldbc_pattern_gb17() -> Result<Pattern, IrError> {
        let mut edges = Vec::new();
        let v1 = PatternVertex::new(0, 1);
        let v2 = PatternVertex::new(1, 7);
        let v3 = PatternVertex::new(2, 2);
        let v4 = PatternVertex::new(3, 2);
        let v5 = PatternVertex::new(4, 1);
        edges.push(PatternEdge::new(1, 10, v1, v2));
        edges.push(PatternEdge::new(2, 0, v3, v1));
        edges.push(PatternEdge::new(3, 1, v3, v2));
        edges.push(PatternEdge::new(4, 0, v4, v5));
        edges.push(PatternEdge::new(5, 3, v4, v3));
        edges.push(PatternEdge::new(6, 13, v5, v3));
        Pattern::try_from(edges)
    }

    pub fn build_ldbc_pattern_gb18() -> Result<Pattern, IrError> {
        let mut edges = Vec::new();
        let v0 = PatternVertex::new(0, 4);
        let v1 = PatternVertex::new(1, 1);
        let v2 = PatternVertex::new(2, 7);
        let v3 = PatternVertex::new(3, 2);
        let v4 = PatternVertex::new(4, 2);
        let v5 = PatternVertex::new(5, 1);
        edges.push(PatternEdge::new(0, 1, v0, v2));
        edges.push(PatternEdge::new(1, 7, v0, v1));
        edges.push(PatternEdge::new(2, 0, v3, v1));
        edges.push(PatternEdge::new(3, 1, v3, v2));
        edges.push(PatternEdge::new(4, 0, v4, v5));
        edges.push(PatternEdge::new(5, 3, v4, v3));
        edges.push(PatternEdge::new(6, 13, v5, v3));
        Pattern::try_from(edges)
    }


    #[test]
    fn test_generate_optimized_matching_plan_for_ldbc_pattern_from_pb_case1() {
        if let Ok(sample_graph_path) = std::env::var("SAMPLE_PATH") {
            let sample_graph = Arc::new(load_sample_graph(&sample_graph_path));
            let ldbc_pattern = build_ldbc_pattern_gb3().unwrap();
            println!("start building catalog...");
            let catalog_build_start_time = Instant::now();
            let mut catalog = Catalogue::build_from_pattern(&ldbc_pattern);
            catalog.estimate_graph(sample_graph, 1.0, None);
            println!("building catalog time cost is: {:?} s", catalog_build_start_time.elapsed().as_secs());
            println!("start generating plan...");
            let plan_generation_start_time = Instant::now();
            let pb_plan = ldbc_pattern
                .generate_optimized_match_plan_recursively(&mut catalog)
                .unwrap();
            let plan: LogicalPlan = pb_plan.try_into().unwrap();
            println!(
                "generating plan time cost is: {:?} ms",
                plan_generation_start_time.elapsed().as_millis()
            );
            println!("{:?}", plan);
            initialize();
            let mut job_builder = JobBuilder::default();
            let mut plan_meta = plan.get_meta().clone();
            plan.add_job_builder(&mut job_builder, &mut plan_meta)
                .unwrap();
            let request = job_builder.build().unwrap();
            println!("start executing query...");
            let query_execution_start_time = Instant::now();
            let mut results = submit_query(request, 2);
            let mut count = 0;
            while let Some(result) = results.next() {
                if let Ok(_) = result {
                    count += 1;
                }
            }
            println!("{}", count);
            println!(
                "executing query time cost is {:?} ms",
                query_execution_start_time.elapsed().as_millis()
            );
        };
    }

    #[test]
    fn test_generate_simple_matching_plan_for_ldbc_pattern_from_pb_case1() {
        let ldbc_pattern = build_ldbc_pattern_gb3().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan()
            .unwrap();
        println!("{:?}", pb_plan);
        initialize();
        let plan: LogicalPlan = pb_plan.try_into().unwrap();
        println!("{:?}", plan);
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

    #[test]
    fn test_get_src_records_pb_case1() {
        if let Ok(sample_graph_path) = std::env::var("SAMPLE_PATH") {
            let graph = load_sample_graph("/Users/meloyang/opt/Graphs/csr_ldbc_graph/scale_1/bin_p1");
            let sample_graph = Arc::new(load_sample_graph(&sample_graph_path));
            let ldbc_pattern = build_ldbc_pattern_gb3().unwrap();
            println!("start building catalog...");
            let catalog_build_start_time = Instant::now();
            let mut catalog = Catalogue::build_from_pattern(&ldbc_pattern);
            catalog.estimate_graph(sample_graph, 1.0, None);
            println!("building catalog time cost is: {:?} s", catalog_build_start_time.elapsed().as_secs());
            println!("start executing query...");
            let query_execution_start_time = Instant::now();
            let pattern_index = catalog
                .get_pattern_index(&ldbc_pattern.encode_to())
                .unwrap();
            let (extend_steps, _) =
                get_definite_extend_steps_recursively(&mut catalog, pattern_index, ldbc_pattern.clone());
            let results = get_src_records(&graph, extend_steps, None);
            println!("{}", results.len());
            println!(
                "executing query time cost is {:?} ms",
                query_execution_start_time.elapsed().as_millis()
            );
        };
    }

    #[test]
    fn test_generate_optimized_matching_plan_for_ldbc_pattern_gb() {
        let mut patterns=Vec::new();
        patterns.push(build_ldbc_pattern_gb1().unwrap());
        patterns.push(build_ldbc_pattern_gb2().unwrap());
        patterns.push(build_ldbc_pattern_gb3().unwrap());
        patterns.push(build_ldbc_pattern_gb4().unwrap());
        patterns.push(build_ldbc_pattern_gb5().unwrap());
        patterns.push(build_ldbc_pattern_gb6().unwrap());
        patterns.push(build_ldbc_pattern_gb7().unwrap());
        patterns.push(build_ldbc_pattern_gb8().unwrap());
        patterns.push(build_ldbc_pattern_gb9().unwrap());
        patterns.push(build_ldbc_pattern_gb10().unwrap());
        patterns.push(build_ldbc_pattern_gb11().unwrap());
        patterns.push(build_ldbc_pattern_gb12().unwrap());
        patterns.push(build_ldbc_pattern_gb13().unwrap());
        patterns.push(build_ldbc_pattern_gb14().unwrap());
        patterns.push(build_ldbc_pattern_gb15().unwrap());
        patterns.push(build_ldbc_pattern_gb16().unwrap());
        patterns.push(build_ldbc_pattern_gb17().unwrap());
        patterns.push(build_ldbc_pattern_gb18().unwrap());
        for i in patterns {
        if let Ok(sample_graph_path) = std::env::var("SAMPLE_PATH") {
            let sample_graph = Arc::new(load_sample_graph(&sample_graph_path));
            let ldbc_pattern = i;
            println!("start building catalog...");
            let catalog_build_start_time = Instant::now();
            let mut catalog = Catalogue::build_from_pattern(&ldbc_pattern);
            catalog.estimate_graph(sample_graph, 0.4, None);
            println!("building catalog time cost is: {:?} s", catalog_build_start_time.elapsed().as_secs());
            println!("start generating plan...");
            let plan_generation_start_time = Instant::now();
            let pb_plan = ldbc_pattern
                .generate_optimized_match_plan_recursively(&mut catalog)
                .unwrap();
            let plan: LogicalPlan = pb_plan.try_into().unwrap();
            println!("{:?}", plan);
            println!(
                "generating plan time cost is: {:?} ms",
                plan_generation_start_time.elapsed().as_millis()
            );
            initialize();
            let mut job_builder = JobBuilder::default();
            let mut plan_meta = plan.get_meta().clone();
            plan.add_job_builder(&mut job_builder, &mut plan_meta)
                .unwrap();
            let request = job_builder.build().unwrap();
            println!("start executing query...");
            let query_execution_start_time = Instant::now();
            let mut results = submit_query(request, 2);
            let mut count = 0;
            while let Some(result) = results.next() {
                if let Ok(_) = result {
                    count += 1;
                }
            }
            println!("{}", count);
            println!(
                "executing query time cost is {:?} ms",
                query_execution_start_time.elapsed().as_millis()
            );
        };
    }
    }

    #[test]
    fn test_generate_optimized_matching_plan_for_ldbc_pattern_from_pb_case2() {
        if let Ok(sample_graph_path) = std::env::var("SAMPLE_PATH") {
            let sample_graph = Arc::new(load_sample_graph(&sample_graph_path));
            let ldbc_pattern = build_ldbc_pattern_gb1().unwrap();
            println!("start building catalog...");
            let catalog_build_start_time = Instant::now();
            let mut catalog = Catalogue::build_from_pattern(&ldbc_pattern);
            catalog.estimate_graph(sample_graph, 1.0, None);
            println!("building catalog time cost is: {:?} s", catalog_build_start_time.elapsed().as_secs());
            println!("start generating plan...");
            let plan_generation_start_time = Instant::now();
            let pb_plan = ldbc_pattern
                .generate_optimized_match_plan_recursively(&mut catalog)
                .unwrap();
            let plan: LogicalPlan = pb_plan.try_into().unwrap();
            println!("{:?}", plan);
            println!(
                "generating plan time cost is: {:?} ms",
                plan_generation_start_time.elapsed().as_millis()
            );
            initialize();
            let mut job_builder = JobBuilder::default();
            let mut plan_meta = plan.get_meta().clone();
            plan.add_job_builder(&mut job_builder, &mut plan_meta)
                .unwrap();
            let request = job_builder.build().unwrap();
            println!("start executing query...");
            let query_execution_start_time = Instant::now();
            let mut results = submit_query(request, 2);
            let mut count = 0;
            while let Some(result) = results.next() {
                if let Ok(_) = result {
                    count += 1;
                }
            }
            println!("{}", count);
            println!(
                "executing query time cost is {:?} ms",
                query_execution_start_time.elapsed().as_millis()
            );
        };
    }

    #[test]
    fn test_generate_simple_matching_plan_for_ldbc_pattern_from_pb_case2() {
        let ldbc_pattern = build_ldbc_pattern_gb1().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan()
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

    #[test]
    fn test_generate_optimized_matching_plan_for_ldbc_pattern_from_pb_case3() {
        if let Ok(sample_graph_path) = std::env::var("SAMPLE_PATH") {
            let sample_graph = Arc::new(load_sample_graph(&sample_graph_path));
            let ldbc_pattern = build_ldbc_pattern_gb13().unwrap();
            println!("start building catalog...");
            let catalog_build_start_time = Instant::now();
            let mut catalog = Catalogue::build_from_pattern(&ldbc_pattern);
            catalog.estimate_graph(sample_graph, 1.0, None);
            println!("building catalog time cost is: {:?} s", catalog_build_start_time.elapsed().as_secs());
            println!("start generating plan...");
            let plan_generation_start_time = Instant::now();
            let pb_plan = ldbc_pattern
                .generate_optimized_match_plan_recursively(&mut catalog)
                .unwrap();
            let plan: LogicalPlan = pb_plan.try_into().unwrap();
            println!(
                "generating plan time cost is: {:?} ms",
                plan_generation_start_time.elapsed().as_millis()
            );
            println!("{:?}", plan);
            initialize();
            let mut job_builder = JobBuilder::default();
            let mut plan_meta = plan.get_meta().clone();
            plan.add_job_builder(&mut job_builder, &mut plan_meta)
                .unwrap();
            let request = job_builder.build().unwrap();
            println!("start executing query...");
            let query_execution_start_time = Instant::now();
            let mut results = submit_query(request, 2);
            let mut count = 0;
            while let Some(result) = results.next() {
                if let Ok(_) = result {
                    count += 1;
                }
            }
            println!("{}", count);
            println!(
                "executing query time cost is {:?} ms",
                query_execution_start_time.elapsed().as_millis()
            );
        };
    }

    #[test]
    fn test_generate_simple_matching_plan_for_ldbc_pattern_from_pb_case3() {
        let ldbc_pattern = build_ldbc_pattern_gb13().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan()
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

    #[test]
    fn test_generate_optimized_matching_plan_for_ldbc_pattern_from_pb_case4() {
        if let Ok(sample_graph_path) = std::env::var("SAMPLE_PATH") {
            let sample_graph = Arc::new(load_sample_graph(&sample_graph_path));
            let ldbc_pattern = build_ldbc_pattern_gb6().unwrap();
            println!("start building catalog...");
            let catalog_build_start_time = Instant::now();
            let mut catalog = Catalogue::build_from_pattern(&ldbc_pattern);
            catalog.estimate_graph(sample_graph, 1.0, None);
            println!("building catalog time cost is: {:?} s", catalog_build_start_time.elapsed().as_secs());
            println!("start generating plan...");
            let plan_generation_start_time = Instant::now();
            let pb_plan = ldbc_pattern
                .generate_optimized_match_plan_recursively(&mut catalog)
                .unwrap();
            print_pb_logical_plan(&pb_plan);
            let plan: LogicalPlan = pb_plan.try_into().unwrap();
            println!(
                "generating plan time cost is: {:?} ms",
                plan_generation_start_time.elapsed().as_millis()
            );
            initialize();
            let mut job_builder = JobBuilder::default();
            let mut plan_meta = plan.get_meta().clone();
            plan.add_job_builder(&mut job_builder, &mut plan_meta)
                .unwrap();
            println!("start executing query...");
            let query_execution_start_time = Instant::now();
            let request = job_builder.build().unwrap();
            let mut results = submit_query(request, 2);
            let mut count = 0;
            while let Some(result) = results.next() {
                if let Ok(_) = result {
                    count += 1;
                }
            }
            println!("{}", count);
            println!(
                "executing query time cost is {:?} ms",
                query_execution_start_time.elapsed().as_millis()
            );
        }
    }

    #[test]
    fn test_generate_simple_matching_plan_for_ldbc_pattern_from_pb_case4() {
        let ldbc_pattern = build_ldbc_pattern_gb6().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan()
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

    #[test]
    fn test_generate_optimized_matching_plan_for_ldbc_bi11() {
        if let Ok(sample_graph_path) = std::env::var("SAMPLE_PATH") {
            let sample_graph = Arc::new(load_sample_graph(&sample_graph_path));
            let ldbc_pattern = build_ldbc_bi11().unwrap();
            println!("start building catalog...");
            let catalog_build_start_time = Instant::now();
            let mut catalog = Catalogue::build_from_pattern(&ldbc_pattern);
            catalog.estimate_graph(sample_graph, 1.0, None);
            println!("building catalog time cost is: {:?} s", catalog_build_start_time.elapsed().as_secs());
            println!("start generating plan...");
            let plan_generation_start_time = Instant::now();
            let pb_plan = ldbc_pattern
                .generate_optimized_match_plan_recursively(&mut catalog)
                .unwrap();
            let plan: LogicalPlan = pb_plan.try_into().unwrap();
            println!(
                "generating plan time cost is: {:?} ms",
                plan_generation_start_time.elapsed().as_millis()
            );
            println!("{:?}", plan);
            initialize();
            let mut job_builder = JobBuilder::default();
            let mut plan_meta = plan.get_meta().clone();
            plan.add_job_builder(&mut job_builder, &mut plan_meta)
                .unwrap();
            let request = job_builder.build().unwrap();
            println!("start executing query...");
            let query_execution_start_time = Instant::now();
            let mut results = submit_query(request, 2);
            let mut count = 0;
            while let Some(result) = results.next() {
                if let Ok(_) = result {
                    count += 1;
                }
            }
            println!("{}", count);
            println!(
                "executing query time cost is {:?} ms",
                query_execution_start_time.elapsed().as_millis()
            );
        }
    }

    #[test]
    fn test_generate_simple_matching_plan_for_ldbc_bi11() {
        let ldbc_pattern = build_ldbc_bi11().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan()
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
