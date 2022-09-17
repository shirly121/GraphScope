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
    use std::convert::TryInto;
    use std::fs::File;
    use std::sync::Arc;
    use std::time::Instant;

    use ir_common::generated::algebra as pb;
    use ir_core::catalogue::catalog::Catalogue;
    use ir_core::catalogue::pattern::Pattern;
    use ir_core::catalogue::pattern_meta::PatternMeta;
    use ir_core::catalogue::sample::load_sample_graph;
    use ir_core::error::IrError;
    use ir_core::plan::logical::LogicalPlan;
    use ir_core::plan::meta::PlanMeta;
    use ir_core::plan::physical::AsPhysical;
    use ir_core::{plan::meta::Schema, JsonIO};
    use pegasus_client::builder::JobBuilder;

    use crate::common::test::*;

    pub fn get_ldbc_pattern_meta() -> PatternMeta {
        let ldbc_schema_file = File::open("../core/resource/ldbc_schema_edit.json").unwrap();
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
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &PlanMeta::default())
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
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &PlanMeta::default())
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
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &PlanMeta::default())
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
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &PlanMeta::default())
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
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &PlanMeta::default())
    }

    #[test]
    fn test_generate_optimized_matching_plan_for_ldbc_pattern_from_pb_case1() {
        if let Ok(sample_graph_path) = std::env::var("SAMPLE_PATH") {
            let sample_graph = Arc::new(load_sample_graph(&sample_graph_path));
            let ldbc_pattern = build_ldbc_pattern_from_pb_case1().unwrap();
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
        let ldbc_pattern = build_ldbc_pattern_from_pb_case1().unwrap();
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
    fn test_generate_optimized_matching_plan_for_ldbc_pattern_from_pb_case2() {
        if let Ok(sample_graph_path) = std::env::var("SAMPLE_PATH") {
            let sample_graph = Arc::new(load_sample_graph(&sample_graph_path));
            let ldbc_pattern = build_ldbc_pattern_from_pb_case2().unwrap();
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
        let ldbc_pattern = build_ldbc_pattern_from_pb_case2().unwrap();
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
            let ldbc_pattern = build_ldbc_pattern_from_pb_case3().unwrap();
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
        let ldbc_pattern = build_ldbc_pattern_from_pb_case3().unwrap();
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
            let ldbc_pattern = build_ldbc_pattern_from_pb_case4().unwrap();
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
        let ldbc_pattern = build_ldbc_pattern_from_pb_case4().unwrap();
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
