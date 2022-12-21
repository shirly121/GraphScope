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

pub mod from_edges {
    use std::convert::TryFrom;

    use ir_common::{KeyId, LabelId};
    use ir_core::catalogue::pattern::{Pattern, PatternEdge, PatternVertex};
    use ir_core::catalogue::{PatternId, PatternLabelId};
    use ir_core::error::IrResult;

    use crate::common::test::*;

    fn new_pattern_edge(
        id: PatternId, label: PatternLabelId, start_v_id: KeyId, end_v_id: KeyId, start_v_label: LabelId,
        end_v_label: LabelId,
    ) -> PatternEdge {
        let start_vertex = PatternVertex::new(start_v_id as PatternId, start_v_label as PatternLabelId);
        let end_vertex = PatternVertex::new(end_v_id as PatternId, end_v_label as PatternLabelId);
        PatternEdge::new(id, label, start_vertex, end_vertex)
    }

    ///```text
    ///           Person
    ///     knows/      \knows
    ///      Person -> Person
    ///```
    pub fn build_ldbc_basecase1() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, KNOWS, TAG_A, TAG_B, PERSON, PERSON),
            new_pattern_edge(1, KNOWS, TAG_A, TAG_C, PERSON, PERSON),
            new_pattern_edge(2, KNOWS, TAG_B, TAG_C, PERSON, PERSON),
        ];
        Pattern::try_from(pattern_edges)
    }

    ///```text
    ///           University
    ///     study at/      \study at
    ///      Person   ->    Person
    ///```
    pub fn build_ldbc_basecase2() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, KNOWS, TAG_A, TAG_B, PERSON, PERSON),
            new_pattern_edge(1, STUDYAT, TAG_A, TAG_C, PERSON, UNIVERSITY),
            new_pattern_edge(2, STUDYAT, TAG_B, TAG_C, PERSON, UNIVERSITY),
        ];
        Pattern::try_from(pattern_edges)
    }

    /// 4 People know each other
    pub fn build_ldbc_basecase3() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, KNOWS, TAG_A, TAG_B, PERSON, PERSON),
            new_pattern_edge(1, KNOWS, TAG_A, TAG_C, PERSON, PERSON),
            new_pattern_edge(2, KNOWS, TAG_A, TAG_D, PERSON, PERSON),
            new_pattern_edge(3, KNOWS, TAG_B, TAG_C, PERSON, PERSON),
            new_pattern_edge(4, KNOWS, TAG_B, TAG_D, PERSON, PERSON),
            new_pattern_edge(5, KNOWS, TAG_C, TAG_D, PERSON, PERSON),
        ];
        Pattern::try_from(pattern_edges)
    }

    ///```text
    ///             City
    ///      lives/     \lives
    ///     Person      Person
    ///     likes \      / has creator
    ///           Comment
    //```
    pub fn build_ldbc_basecase4() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, ISLOCATEDIN, TAG_A, TAG_C, PERSON, CITY),
            new_pattern_edge(1, ISLOCATEDIN, TAG_B, TAG_C, PERSON, CITY),
            new_pattern_edge(2, LIKES, TAG_A, TAG_D, PERSON, COMMENT),
            new_pattern_edge(3, HASCREATOR, TAG_D, TAG_B, COMMENT, PERSON),
        ];
        Pattern::try_from(pattern_edges)
    }

    pub fn build_ldbc_basecase5() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, KNOWS, TAG_A, TAG_B, PERSON, PERSON),
            new_pattern_edge(1, KNOWS, TAG_A, TAG_C, PERSON, PERSON),
            new_pattern_edge(2, KNOWS, TAG_A, TAG_D, PERSON, PERSON),
            new_pattern_edge(3, KNOWS, TAG_A, TAG_E, PERSON, PERSON),
            new_pattern_edge(4, KNOWS, TAG_B, TAG_C, PERSON, PERSON),
            new_pattern_edge(5, KNOWS, TAG_D, TAG_E, PERSON, PERSON),
        ];
        Pattern::try_from(pattern_edges)
    }

    ///```text
    ///           Person
    ///     knows/      \knows
    ///      Person -> Person  -[HAS_INTEREST]-> Tag -[HAS_TYPE]-> Tagclass
    ///```
    pub fn build_ldbc_basecase6() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, KNOWS, TAG_A, TAG_B, PERSON, PERSON),
            new_pattern_edge(1, KNOWS, TAG_A, TAG_C, PERSON, PERSON),
            new_pattern_edge(2, KNOWS, TAG_B, TAG_C, PERSON, PERSON),
            new_pattern_edge(3, HASINTEREST, TAG_B, TAG_D, PERSON, TAG),
            new_pattern_edge(4, HASTYPE, TAG_D, TAG_E, TAG, TAGCLASS),
        ];
        Pattern::try_from(pattern_edges)
    }

    pub fn build_ldbc_bi3() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, ISPARTOF, TAG_B, TAG_A, CITY, COUNTRY),
            new_pattern_edge(1, ISLOCATEDIN, TAG_C, TAG_B, PERSON, CITY),
            new_pattern_edge(2, HASMODERATOR, TAG_D, TAG_C, FORUM, PERSON),
            new_pattern_edge(3, CONTAINEROF, TAG_D, TAG_E, FORUM, POST),
            new_pattern_edge(4, REPLYOF, TAG_F, TAG_E, COMMENT, POST),
            new_pattern_edge(5, HASTAG, TAG_F, TAG_G, COMMENT, TAG),
            new_pattern_edge(6, HASTYPE, TAG_G, TAG_H, TAG, TAGCLASS),
        ];
        Pattern::try_from(pattern_edges)
    }

    pub fn build_ldbc_bi11() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, KNOWS, TAG_A, TAG_B, PERSON, PERSON),
            new_pattern_edge(1, KNOWS, TAG_A, TAG_C, PERSON, PERSON),
            new_pattern_edge(2, KNOWS, TAG_B, TAG_C, PERSON, PERSON),
            new_pattern_edge(3, ISLOCATEDIN, TAG_A, TAG_D, PERSON, CITY),
            new_pattern_edge(4, ISLOCATEDIN, TAG_B, TAG_E, PERSON, CITY),
            new_pattern_edge(5, ISLOCATEDIN, TAG_C, TAG_F, PERSON, CITY),
            new_pattern_edge(6, ISPARTOF, TAG_D, TAG_G, CITY, COUNTRY),
            new_pattern_edge(7, ISPARTOF, TAG_E, TAG_G, CITY, COUNTRY),
            new_pattern_edge(8, ISPARTOF, TAG_F, TAG_G, CITY, COUNTRY),
        ];
        Pattern::try_from(pattern_edges)
    }

    pub fn build_ldbc_gb9() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, REPLYOF, TAG_A, TAG_B, COMMENT, COMMENT),
            new_pattern_edge(1, HASCREATOR, TAG_A, TAG_C, COMMENT, PERSON),
            new_pattern_edge(2, HASCREATOR, TAG_B, TAG_C, COMMENT, PERSON),
            new_pattern_edge(3, KNOWS, TAG_D, TAG_C, PERSON, PERSON),
            new_pattern_edge(4, HASMEMBER, TAG_E, TAG_C, FORUM, PERSON),
            new_pattern_edge(5, HASMEMBER, TAG_E, TAG_D, FORUM, PERSON),
        ];
        Pattern::try_from(pattern_edges)
    }

    pub fn build_ldbc_gb10() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, REPLYOF, TAG_A, TAG_B, COMMENT, COMMENT),
            new_pattern_edge(1, HASCREATOR, TAG_A, TAG_C, COMMENT, PERSON),
            new_pattern_edge(2, HASCREATOR, TAG_B, TAG_C, COMMENT, PERSON),
            new_pattern_edge(3, KNOWS, TAG_D, TAG_C, PERSON, PERSON),
            new_pattern_edge(4, HASTAG, TAG_A, TAG_E, COMMENT, TAG),
            new_pattern_edge(5, HASTAG, TAG_B, TAG_E, COMMENT, TAG),
        ];
        Pattern::try_from(pattern_edges)
    }

    pub fn build_ldbc_gb11() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, HASTAG, TAG_A, TAG_C, COMMENT, TAG),
            new_pattern_edge(1, LIKES, TAG_B, TAG_A, PERSON, COMMENT),
            new_pattern_edge(2, HASINTEREST, TAG_B, TAG_C, PERSON, TAG),
            new_pattern_edge(3, HASTAG, TAG_D, TAG_C, COMMENT, TAG),
            new_pattern_edge(4, HASCREATOR, TAG_D, TAG_B, COMMENT, PERSON),
        ];
        Pattern::try_from(pattern_edges)
    }

    pub fn build_ldbc_gb14() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, HASCREATOR, TAG_A, TAG_B, COMMENT, PERSON),
            new_pattern_edge(1, HASTAG, TAG_A, TAG_C, COMMENT, TAG),
            new_pattern_edge(2, HASMEMBER, TAG_D, TAG_B, FORUM, PERSON),
            new_pattern_edge(3, HASTAG, TAG_E, TAG_C, COMMENT, TAG),
            new_pattern_edge(4, CONTAINEROF, TAG_D, TAG_F, FORUM, POST),
            new_pattern_edge(5, REPLYOF, TAG_E, TAG_F, COMMENT, POST),
        ];
        Pattern::try_from(pattern_edges)
    }

    pub fn build_ldbc_gb15() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, HASCREATOR, TAG_B, TAG_A, COMMENT, PERSON),
            new_pattern_edge(1, REPLYOF, TAG_B, TAG_C, COMMENT, POST),
            new_pattern_edge(2, CONTAINEROF, TAG_D, TAG_C, FORUM, POST),
            new_pattern_edge(3, HASMEMBER, TAG_D, TAG_E, FORUM, PERSON),
        ];
        Pattern::try_from(pattern_edges)
    }

    pub fn build_ldbc_gb16() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, HASTYPE, TAG_B, TAG_A, TAG, TAGCLASS),
            new_pattern_edge(1, HASTAG, TAG_C, TAG_B, COMMENT, TAG),
            new_pattern_edge(2, REPLYOF, TAG_C, TAG_D, COMMENT, POST),
            new_pattern_edge(3, CONTAINEROF, TAG_E, TAG_D, FORUM, POST),
            new_pattern_edge(4, HASMODERATOR, TAG_E, TAG_F, FORUM, PERSON),
            new_pattern_edge(5, ISLOCATEDIN, TAG_F, TAG_G, PERSON, CITY),
            new_pattern_edge(6, ISPARTOF, TAG_G, TAG_H, CITY, COUNTRY),
        ];
        Pattern::try_from(pattern_edges)
    }

    pub fn build_ldbc_gb17() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, HASCREATOR, TAG_A, TAG_B, COMMENT, PERSON),
            new_pattern_edge(1, REPLYOF, TAG_A, TAG_C, COMMENT, COMMENT),
            new_pattern_edge(2, LIKES, TAG_B, TAG_C, PERSON, COMMENT),
            new_pattern_edge(3, HASCREATOR, TAG_C, TAG_D, COMMENT, PERSON),
            new_pattern_edge(4, HASTAG, TAG_C, TAG_E, COMMENT, TAG),
            new_pattern_edge(5, HASINTEREST, TAG_D, TAG_E, PERSON, TAG),
        ];
        Pattern::try_from(pattern_edges)
    }

    pub fn build_ldbc_gb18() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, HASCREATOR, TAG_A, TAG_B, COMMENT, PERSON),
            new_pattern_edge(1, REPLYOF, TAG_A, TAG_C, COMMENT, COMMENT),
            new_pattern_edge(2, LIKES, TAG_B, TAG_C, PERSON, COMMENT),
            new_pattern_edge(3, HASCREATOR, TAG_C, TAG_D, COMMENT, PERSON),
            new_pattern_edge(4, HASTAG, TAG_C, TAG_E, COMMENT, TAG),
            new_pattern_edge(5, HASMODERATOR, TAG_F, TAG_D, FORUM, PERSON),
            new_pattern_edge(6, HASTAG, TAG_F, TAG_E, FORUM, TAG),
        ];
        Pattern::try_from(pattern_edges)
    }

    pub fn build_ldbc_gb20() -> IrResult<Pattern> {
        let pattern_edges: Vec<PatternEdge> = vec![
            new_pattern_edge(0, LIKES, TAG_A, TAG_B, PERSON, COMMENT),
            new_pattern_edge(1, HASCREATOR, TAG_B, TAG_C, COMMENT, PERSON),
            new_pattern_edge(2, ISLOCATEDIN, TAG_A, TAG_D, PERSON, PLACE),
            new_pattern_edge(3, ISLOCATEDIN, TAG_C, TAG_D, PERSON, PLACE),
            new_pattern_edge(4, ISLOCATEDIN, TAG_E, TAG_D, PERSON, PLACE),
            new_pattern_edge(5, ISLOCATEDIN, TAG_G, TAG_D, PERSON, PLACE),
            new_pattern_edge(6, LIKES, TAG_E, TAG_F, PERSON, COMMENT),
            new_pattern_edge(7, HASCREATOR, TAG_F, TAG_G, COMMENT, PERSON),
        ];
        Pattern::try_from(pattern_edges)
    }
}

pub mod from_pb {
    use ir_common::generated::algebra::{self as pb};
    use ir_core::catalogue::pattern::Pattern;
    use ir_core::catalogue::{PatternDirection, PatternLabelId};
    use ir_core::error::{IrError, IrResult};
    use ir_core::plan::meta::PlanMeta;

    use crate::common::test::*;

    fn build_extend_opr(e_label: PatternLabelId, direction: PatternDirection) -> pb::EdgeExpand {
        let dir: i32 = match direction {
            PatternDirection::Out => 0,
            PatternDirection::In => 1,
        };
        pb::EdgeExpand {
            v_tag: None,
            direction: dir,
            params: Some(query_params(vec![e_label.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
        }
    }

    // Pattern from ldbc schema file and build from pb::Pattern message
    //           Person
    //     knows/      \knows
    //      Person -> Person
    pub fn build_ldbc_basecase1() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
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
    pub fn build_ldbc_basecase2() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            STUDYAT,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            STUDYAT,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
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
    pub fn build_ldbc_basecase3() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
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
    pub fn build_ldbc_basecase4() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISLOCATEDIN,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISLOCATEDIN,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            LIKES,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_basecase5() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_basecase6() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASINTEREST,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTYPE,
                            PatternDirection::Out,
                        ))),
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
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASMODERATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISLOCATEDIN,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISPARTOF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            CONTAINEROF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_E.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            REPLYOF,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_F.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_F.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTAG,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_G.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_G.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTYPE,
                            PatternDirection::Out,
                        ))),
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
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASMEMBER,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISLOCATEDIN,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISPARTOF,
                            PatternDirection::Out,
                        ))),
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
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            CONTAINEROF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            REPLYOF,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASMEMBER,
                            PatternDirection::In,
                        ))),
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
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISLOCATEDIN,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISLOCATEDIN,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISLOCATEDIN,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_F.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISPARTOF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_H.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_E.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISPARTOF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_H.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_F.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISPARTOF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_H.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_gb9() -> IrResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            REPLYOF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASMEMBER,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASMEMBER,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_gb10() -> IrResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            REPLYOF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            KNOWS,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTAG,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTAG,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_gb11() -> IrResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            LIKES,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTAG,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASINTEREST,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTAG,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_gb14() -> IrResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTAG,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASMEMBER,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTAG,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            CONTAINEROF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_F.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_E.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            REPLYOF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_F.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_gb15() -> IrResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            REPLYOF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            CONTAINEROF,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASMEMBER,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_gb16() -> IrResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTYPE,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTAG,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            REPLYOF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            CONTAINEROF,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_E.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASMODERATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_F.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_F.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISLOCATEDIN,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_G.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_G.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISPARTOF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_H.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_gb17() -> IrResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            REPLYOF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            LIKES,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTAG,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASINTEREST,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_gb18() -> IrResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            REPLYOF,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            LIKES,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTAG,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASMODERATOR,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_F.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_E.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASTAG,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_F.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    pub fn build_ldbc_gb20() -> IrResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            LIKES,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISLOCATEDIN,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISLOCATEDIN,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISLOCATEDIN,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            ISLOCATEDIN,
                            PatternDirection::In,
                        ))),
                    }],
                    end: Some(TAG_G.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_E.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            LIKES,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_F.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_F.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(build_extend_opr(
                            HASCREATOR,
                            PatternDirection::Out,
                        ))),
                    }],
                    end: Some(TAG_G.into()),
                    join_kind: 0,
                },
            ],
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }
}
