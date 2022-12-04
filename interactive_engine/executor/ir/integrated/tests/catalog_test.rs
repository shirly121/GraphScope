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
    use std::collections::HashMap;
    use std::convert::TryInto;
    use std::time::Instant;
    use std::env;

    use ir_common::generated::algebra::{self as pb, logical_plan};
    use ir_common::generated::common::{self as common_pb};
    use ir_core::catalogue::catalog::{Catalogue, PatMatPlanSpace};
    use ir_core::catalogue::pattern::Pattern;
    use ir_core::catalogue::pattern_meta::PatternMeta;
    use ir_core::catalogue::plan::get_definite_extend_steps;
    use ir_core::catalogue::sample::{get_src_records, load_sample_graph};
    use ir_core::catalogue::{PatternDirection, PatternLabelId};
    use ir_core::error::{IrError, IrResult};
    use ir_core::plan::logical::LogicalPlan;
    use ir_core::plan::physical::AsPhysical;
    use pegasus_client::builder::JobBuilder;

    use crate::common::{self, test::*};

    use graph_store::prelude::{
        DefaultId, GlobalStoreTrait, GlobalStoreUpdate, GraphDBConfig, InternalId, LargeGraphDB, MutableGraphDB,
    };
    use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
    use std::path::Path;

    fn generate_pattern_match_plan(pattern: &Pattern, catalogue: &Catalogue) -> IrResult<pb::LogicalPlan> {
        println!("start generating plan...");
        let plan_generation_start_time = Instant::now();
        let pb_plan: pb::LogicalPlan = pattern
            .generate_optimized_match_plan_recursively(&mut catalogue.clone(), &get_ldbc_pattern_meta(), is_distributed)
            .expect("Failed to generate pattern match plan");
        print_pb_logical_plan(&pb_plan);
        println!("generating plan time cost is: {:?} ms", plan_generation_start_time.elapsed().as_millis());
        Ok(pb_plan)
    }

    fn print_pb_logical_plan(pb_plan: &pb::LogicalPlan) {
        let mut id = 0;
        pb_plan.nodes.iter().for_each(|node| {
            println!("ID: {:?}, {:?}", id, node);
            id += 1;
        });
        println!("Roots: {:?}", pb_plan.roots);
    }

    fn execute_pb_logical_plan(pb_plan: pb::LogicalPlan) {
        initialize();
        let plan: LogicalPlan = pb_plan.try_into().unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.get_meta().clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();
        let request = job_builder.build().unwrap();
        println!("start executing query...");
        let query_execution_start_time = Instant::now();
        let number_of_cores: u32 = match env::var("NUM_CORES_GRAPHSCOPE") {
            Ok(val) => val.parse().unwrap(),
            Err(_e) => 1,
        };
        println!("number of cores: {}", number_of_cores);
        let mut results = submit_query(request, number_of_cores);
        let mut count = 0;
        while let Some(result) = results.next() {
            if let Ok(_) = result {
                count += 1;
            }
        }
        println!("Pattern Match Output: {}", count);
        println!("executing query time cost is {:?} ms", query_execution_start_time.elapsed().as_millis());
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

    fn generate_naive_pattern_match_plan_for_ldbc_pattern_from_pb_case1() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case1().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_pattern_from_pb_case1() {
        let pattern = build_ldbc_pattern_from_pb_case1().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn test_get_src_records_pb_case1() {
        if let Ok(sample_graph_path) = std::env::var("SAMPLE_PATH") {
            let graph = load_sample_graph("/Users/meloyang/opt/Graphs/csr_ldbc_graph/scale_1/bin_p1");
            let sample_graph = Arc::new(load_sample_graph(&sample_graph_path));
            let ldbc_pattern = build_ldbc_pattern_from_pb_case1().unwrap();
            println!("start building catalog...");
            let catalog_build_start_time = Instant::now();
            let mut catalog = Catalogue::build_from_pattern(&ldbc_pattern, PatMatPlanSpace::ExtendWithIntersection);
            catalog.estimate_graph(sample_graph, 1.0, HashMap::new(), None, 8);
            println!("building catalog time cost is: {:?} s", catalog_build_start_time.elapsed().as_secs());
            println!("start executing query...");
            let query_execution_start_time = Instant::now();
            let (extend_steps, _) = get_definite_extend_steps(ldbc_pattern.clone(), &mut catalog);
            let results = get_src_records(&graph, extend_steps, None);
            println!("{}", results.len());
            println!(
                "executing query time cost is {:?} ms",
                query_execution_start_time.elapsed().as_millis()
            );
        };
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_pattern_from_pb_case2() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case2().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_pattern_from_pb_case2() {
        let pattern = build_ldbc_pattern_from_pb_case2().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_pattern_from_pb_case3() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case3().unwrap();
        let mut pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        pb_plan.roots = vec![1];
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_pattern_from_pb_case3() {
        let pattern = build_ldbc_pattern_from_pb_case3().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_pattern_from_pb_case4() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case4().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_pattern_from_pb_case4() {
        let pattern = build_ldbc_pattern_from_pb_case4().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_pattern_from_pb_case5() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case5().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_pattern_from_pb_case5() {
        let pattern = build_ldbc_pattern_from_pb_case5().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_pattern_from_pb_case6() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case6().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_pattern_from_pb_case6() {
        let pattern = build_ldbc_pattern_from_pb_case6().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_bi3() {
        let ldbc_pattern = build_ldbc_bi3().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_bi3() {
        let pattern = build_ldbc_bi3().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_bi4_subtask_1() {
        let ldbc_pattern = build_ldbc_bi4_subtask_1().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_bi4_subtask_1() {
        let pattern = build_ldbc_bi4_subtask_1().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_bi4_subtask_2() {
        let ldbc_pattern = build_ldbc_bi4_subtask_2().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_bi4_subtask_2() {
        let pattern = build_ldbc_bi4_subtask_2().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_bi11() {
        let ldbc_pattern = build_ldbc_bi11().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_bi11() {
        let pattern = build_ldbc_bi11().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue =
            Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue)
            .expect("Failed to generate pattern match plan");
        // execute_pb_logical_plan(pb_plan);
    }

    /// HandWritten Logical Plans
    fn build_scan_opr(v_label: Option<PatternLabelId>) -> logical_plan::Operator {
        let query_table = if let Some(label) = v_label { vec![(label as i32).into()] } else { vec![] };
        pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(query_table, vec![], None)),
            idx_predicate: None,
        }
        .into()
    }

    fn build_as_opr(alias: Option<i32>) -> logical_plan::Operator {
        match alias {
            Some(tag) => pb::As { alias: Some(tag.into()) }.into(),
            None => pb::As { alias: None }.into(),
        }
    }

    fn build_expand_opr(
        src: i32, dst: i32, dir: PatternDirection, e_label: PatternLabelId,
    ) -> logical_plan::Operator {
        pb::EdgeExpand {
            // use start vertex id as tag
            v_tag: Some(src.into()),
            direction: dir as i32,
            params: Some(query_params(vec![(e_label as i32).into()], vec![], None)),
            expand_opt: 0,
            // use target vertex id as alias
            alias: Some(dst.into()),
        }
        .into()
    }

    fn build_join_opr(left_keys: Vec<i32>, right_keys: Vec<i32>) -> logical_plan::Operator {
        let join_keys_left: Vec<common_pb::Variable> = left_keys
            .into_iter()
            .map(|join_id| common_pb::Variable { tag: join_id.try_into().ok(), property: None })
            .collect();
        let join_keys_right: Vec<common_pb::Variable> = right_keys
            .into_iter()
            .map(|join_id| common_pb::Variable { tag: join_id.try_into().ok(), property: None })
            .collect();
        pb::Join { left_keys: join_keys_left, right_keys: join_keys_right, kind: 0 }.into()
    }

    fn build_sink_opr(tags: Vec<i32>) -> logical_plan::Operator {
        pb::Sink {
            tags: tags
                .iter()
                .map(|&tag| common_pb::NameOrIdKey { key: Some(tag.into()) })
                .collect(),
            sink_target: default_sink_target(),
        }
        .into()
    }

    fn build_node(opr: logical_plan::Operator, children: Vec<i32>) -> pb::logical_plan::Node {
        pb::logical_plan::Node { opr: Some(opr.into()), children }
    }

    #[test]
    fn handwritten_matching_plan_for_ldbc_pattern_from_pb_case6() {
        let v_person_label: i32 = 1;
        let e_knows_label: i32 = 12;
        let mut pb_plan = pb::LogicalPlan::default();
        pb_plan.nodes = vec![
            build_node(build_scan_opr(Some(v_person_label)), vec![1, 3]),
            build_node(build_as_opr(Some(0)), vec![2]),
            build_node(build_expand_opr(0, 1, PatternDirection::Out, e_knows_label), vec![5]),
            build_node(build_as_opr(Some(2)), vec![4]),
            build_node(build_expand_opr(2, 1, PatternDirection::In, e_knows_label), vec![5]),
            build_node(build_join_opr(vec![1], vec![1]), vec![6]),
            build_node(build_sink_opr(vec![0, 1, 2]), vec![]),
        ];
        pb_plan.roots = vec![0];

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn handwritten_matching_plan_for_ldbc_pattern_from_pb_case7() {
        let v_person_label: i32 = 1;
        let v_tag_label: i32 = 7;
        let e_knows_label: i32 = 12;
        let e_has_interested_label: i32 = 10;
        let mut pb_plan = pb::LogicalPlan::default();
        pb_plan.nodes = vec![
            build_node(build_scan_opr(None), vec![1, 4]),
            build_node(build_scan_opr(Some(v_person_label)), vec![2]),
            build_node(build_as_opr(Some(0)), vec![3]),
            build_node(build_expand_opr(0, 1, PatternDirection::In, e_knows_label), vec![7]),
            build_node(build_scan_opr(Some(v_tag_label)), vec![5]),
            build_node(build_as_opr(Some(2)), vec![6]),
            build_node(build_expand_opr(2, 1, PatternDirection::In, e_has_interested_label), vec![7]),
            build_node(build_join_opr(vec![1], vec![1]), vec![8]),
            build_node(build_sink_opr(vec![0, 1, 2]), vec![]),
        ];
        pb_plan.roots = vec![0];

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn handwritten_matching_plan_for_ldbc_pattern_from_pb_case8() {
        let v_person_label: i32 = 1;
        let v_tag_label: i32 = 7;
        let e_knows_label: i32 = 12;
        let e_has_interested_label: i32 = 10;
        let e_has_type_label: i32 = 21;
        let mut pb_plan = pb::LogicalPlan::default();
        pb_plan.nodes = vec![
            build_node(build_scan_opr(None), vec![1, 7]),
            build_node(build_scan_opr(Some(v_person_label)), vec![2]),
            build_node(build_as_opr(Some(0)), vec![3]),
            build_node(build_expand_opr(0, 1, PatternDirection::Out, e_knows_label), vec![4, 5]),
            build_node(build_expand_opr(0, 2, PatternDirection::Out, e_knows_label), vec![6]),
            build_node(build_expand_opr(1, 2, PatternDirection::In, e_knows_label), vec![6]),
            pb::logical_plan::Node {
                opr: Some(pb::Intersect { parents: vec![], key: Some((2 as i32).into()) }.into()),
                children: vec![11],
            },
            build_node(build_scan_opr(Some(v_person_label)), vec![8]),
            build_node(build_as_opr(Some(2)), vec![9]),
            build_node(build_expand_opr(2, 3, PatternDirection::Out, e_has_interested_label), vec![10]),
            build_node(build_expand_opr(3, 4, PatternDirection::Out, e_has_type_label), vec![11]),
            build_node(build_join_opr(vec![2], vec![2]), vec![12]),
            build_node(build_sink_opr(vec![0, 1, 2, 3, 4]), vec![]),
        ];
        pb_plan.roots = vec![0];

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn handwritten_matching_plan_for_ldbc_pattern_from_pb_case6_with_as_node() {
        let v_person_label: i32 = 1;
        let v_tag_label: i32 = 7;
        let e_knows_label: i32 = 12;
        let e_has_interested_label: i32 = 10;
        let e_has_type_label: i32 = 21;
        let mut pb_plan = pb::LogicalPlan::default();
        pb_plan.nodes = vec![
            build_node(build_scan_opr(None), vec![1]),
            build_node(build_as_opr(None), vec![2, 8]),
            build_node(build_scan_opr(Some(v_person_label)), vec![3]),
            build_node(build_as_opr(Some(0)), vec![4]),
            build_node(build_expand_opr(0, 1, PatternDirection::Out, e_knows_label), vec![5, 6]),
            build_node(build_expand_opr(0, 2, PatternDirection::Out, e_knows_label), vec![7]),
            build_node(build_expand_opr(1, 2, PatternDirection::In, e_knows_label), vec![7]),
            pb::logical_plan::Node {
                opr: Some(pb::Intersect { parents: vec![], key: Some((2 as i32).into()) }.into()),
                children: vec![12],
            },
            build_node(build_scan_opr(Some(v_person_label)), vec![9]),
            build_node(build_as_opr(Some(2)), vec![10]),
            build_node(build_expand_opr(2, 3, PatternDirection::Out, e_has_interested_label), vec![11]),
            build_node(build_expand_opr(3, 4, PatternDirection::Out, e_has_type_label), vec![12]),
            build_node(build_join_opr(vec![2], vec![2]), vec![13]),
            build_node(build_sink_opr(vec![0, 1, 2, 3, 4]), vec![]),
        ];
        pb_plan.roots = vec![0];

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn handwritten_plan_for_bi_11() {
        let v_label_person: i32 = 1;
        let v_label_place: i32 = 0;
        let e_label_person_knows_person: i32 = 12;
        let e_label_person_lives_in_place: i32 = 11;
        let e_label_place_is_part_of_place: i32 = 17;
        let mut pb_plan = pb::LogicalPlan::default();
        pb_plan.nodes = vec![
            build_node(build_scan_opr(None), vec![]),
            build_node(build_scan_opr(Some(v_label_person)), vec![2]),
            build_node(build_as_opr(Some(0)), vec![3]),
            // build_node(build_expand_opr(0, 1, dir, e_label), children)
        ];
    }

    #[test]
    fn handwritten_plan_for_bi_4_subtask_2_build() {
        let v_label_comment: i32 = 2;
        let v_label_forum: i32 = 4;
        let e_label_forum_containerof_post: i32 = 5;
        let e_label_reply_of: i32 = 3;
        let e_label_hascreator: i32 = 0;
        let e_label_hasmember: i32 = 6;
        let mut pb_plan = pb::LogicalPlan::default();
        pb_plan.nodes = vec![
            build_node(build_scan_opr(Some(v_label_comment)), vec![1]),
            build_node(build_as_opr(Some(2)), vec![2]),
            build_node(build_expand_opr(2, 1, PatternDirection::Out, e_label_reply_of), vec![3]),
            build_node(build_expand_opr(1, 0, PatternDirection::In, e_label_forum_containerof_post), vec![4]),
            build_node(build_sink_opr(vec![0, 1, 2]), vec![]),
        ];
        pb_plan.roots = vec![0];

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn handwritten_plan_for_bi_4_subtask_2_probe() {
        let v_label_comment: i32 = 2;
        let v_label_forum: i32 = 4;
        let e_label_forum_containerof_post: i32 = 5;
        let e_label_reply_of: i32 = 3;
        let e_label_hascreator: i32 = 0;
        let e_label_hasmember: i32 = 6;
        let mut pb_plan = pb::LogicalPlan::default();
        pb_plan.nodes = vec![
            build_node(build_scan_opr(Some(v_label_forum)), vec![1]),
            build_node(build_as_opr(Some(4)), vec![2]),
            build_node(build_expand_opr(4, 3, PatternDirection::Out, e_label_hasmember), vec![3]),
            build_node(build_expand_opr(3, 2, PatternDirection::In, e_label_hascreator), vec![4]),
            build_node(build_sink_opr(vec![2, 3, 4]), vec![]),
        ];
        pb_plan.roots = vec![0];

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn handwritten_plan_for_bi_4_subtask_2_join() {
        let v_label_comment: i32 = 2;
        let v_label_forum: i32 = 4;
        let e_label_forum_containerof_post: i32 = 5;
        let e_label_reply_of: i32 = 3;
        let e_label_hascreator: i32 = 0;
        let e_label_hasmember: i32 = 6;
        let mut pb_plan = pb::LogicalPlan::default();
        pb_plan.nodes = vec![
            build_node(build_scan_opr(None), vec![1, 5]),
            build_node(build_scan_opr(Some(v_label_forum)), vec![2]),
            build_node(build_as_opr(Some(0)), vec![3]),
            build_node(build_expand_opr(0, 1, PatternDirection::Out, e_label_forum_containerof_post), vec![4]),
            build_node(build_expand_opr(1, 2, PatternDirection::In, e_label_reply_of), vec![9]),
            build_node(build_scan_opr(Some(v_label_forum)), vec![6]),
            build_node(build_as_opr(Some(4)), vec![7]),
            build_node(build_expand_opr(4, 3, PatternDirection::Out, e_label_hasmember), vec![8]),
            build_node(build_expand_opr(3, 2, PatternDirection::In, e_label_hascreator), vec![9]),
            build_node(build_join_opr(vec![2], vec![2]), vec![10]),
            build_node(build_sink_opr(vec![0, 1, 2, 3, 4]), vec![]),
        ];
        pb_plan.roots = vec![0];

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn handwritten_plan_for_bi_4_subtask_2_join_with_as_node() {
        let v_label_comment: i32 = 2;
        let v_label_forum: i32 = 4;
        let e_label_forum_containerof_post: i32 = 5;
        let e_label_reply_of: i32 = 3;
        let e_label_hascreator: i32 = 0;
        let e_label_hasmember: i32 = 6;
        let mut pb_plan = pb::LogicalPlan::default();
        pb_plan.nodes = vec![
            build_node(build_scan_opr(None), vec![1]),
            build_node(build_as_opr(None), vec![2, 6]),
            build_node(build_scan_opr(Some(v_label_forum)), vec![3]),
            build_node(build_as_opr(Some(0)), vec![4]),
            build_node(build_expand_opr(0, 1, PatternDirection::Out, e_label_forum_containerof_post), vec![5]),
            build_node(build_expand_opr(1, 2, PatternDirection::In, e_label_reply_of), vec![10]),
            build_node(build_scan_opr(Some(v_label_forum)), vec![7]),
            build_node(build_as_opr(Some(4)), vec![8]),
            build_node(build_expand_opr(4, 3, PatternDirection::Out, e_label_hasmember), vec![9]),
            build_node(build_expand_opr(3, 2, PatternDirection::In, e_label_hascreator), vec![10]),
            build_node(build_join_opr(vec![2], vec![2]), vec![11]),
            build_node(build_sink_opr(vec![0, 1, 2, 3, 4]), vec![]),
        ];
        pb_plan.roots = vec![0];

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }
}
