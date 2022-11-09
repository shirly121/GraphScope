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
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::collections::HashSet;
    use std::convert::TryInto;
    use std::fs::File;
    use std::sync::Arc;
    use std::time::Instant;

    use graph_proxy::apis::{DynDetails, GraphElement, QueryParams, Statement, Vertex, ID};
    use graph_proxy::{create_exp_store, SimplePartition};
    use graph_store::graph_db::GlobalStoreTrait;
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_core::catalogue::catalog::Catalogue;
    use ir_core::catalogue::pattern::Pattern;
    use ir_core::catalogue::pattern_meta::PatternMeta;
    use ir_core::catalogue::plan::get_definite_extend_steps_recursively;
    use ir_core::catalogue::sample::{get_src_records, load_sample_graph};
    use ir_core::error::IrError;
    use ir_core::plan::logical::LogicalPlan;
    use ir_core::plan::meta::PlanMeta;
    use ir_core::plan::physical::AsPhysical;
    use ir_core::{plan::meta::Schema, JsonIO};
    use pegasus::api::{Map, Sink};
    use pegasus::JobConf;
    use pegasus_client::builder::JobBuilder;
    use petgraph::Direction;
    use runtime::process::operator::flatmap::FlatMapFuncGen;
    use runtime::process::operator::map::FilterMapFuncGen;
    use runtime::process::operator::map::Intersection;
    use runtime::process::operator::source::SourceOperator;
    use runtime::process::record::Entry;
    use runtime::process::record::{CompleteEntry, Record, SimpleEntry};

    use crate::common::test::*;

    fn source_gen(
        alias: Option<common_pb::NameOrId>,
    ) -> Box<dyn Iterator<Item = Record<CompleteEntry>> + Send> {
        let scan_opr_pb = pb::Scan {
            scan_opt: 0,
            alias,
            params: Some(query_params(vec![1.into()], vec![], None)),
            idx_predicate: None,
        };
        let source = SourceOperator::new(
            pb::logical_plan::operator::Opr::Scan(scan_opr_pb),
            1,
            1,
            Arc::new(SimplePartition { num_servers: 1 }),
        )
        .unwrap();
        source.gen_source(0).unwrap()
    }

    fn source_gen_simple(
        alias: Option<common_pb::NameOrId>,
    ) -> Box<dyn Iterator<Item = Record<SimpleEntry>> + Send> {
        let scan_opr_pb = pb::Scan {
            scan_opt: 0,
            alias,
            params: Some(query_params(vec![1.into()], vec![], None)),
            idx_predicate: None,
        };
        let source = SourceOperator::new(
            pb::logical_plan::operator::Opr::Scan(scan_opr_pb),
            1,
            1,
            Arc::new(SimplePartition { num_servers: 1 }),
        )
        .unwrap();
        source.gen_source(0).unwrap()
    }

    fn get_partition(id: u64, num_servers: usize, worker_num: usize) -> u64 {
        let magic_num = id / (num_servers as u64);
        let num_servers = num_servers as u64;
        let worker_num = worker_num as u64;
        (id - magic_num * num_servers) * worker_num + magic_num % worker_num
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
    pub fn build_ldbc_pattern_from_pb_case1() -> Result<Pattern, IrError> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 1,                                              // in
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
                // pb::pattern::Sentence {
                //     start: Some(TAG_B.into()),
                //     binders: vec![pb::pattern::Binder {
                //         item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                //     }],
                //     end: Some(TAG_C.into()),
                //     join_kind: 0,
                // },
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
                .generate_optimized_match_plan_recursively(&mut catalog, &get_ldbc_pattern_meta(), false)
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
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
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
            let ldbc_pattern = build_ldbc_pattern_from_pb_case1().unwrap();
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
                .generate_optimized_match_plan_recursively(&mut catalog, &get_ldbc_pattern_meta(), false)
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
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
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
                .generate_optimized_match_plan_recursively(&mut catalog, &get_ldbc_pattern_meta(), false)
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
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
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
                .generate_optimized_match_plan_recursively(&mut catalog, &get_ldbc_pattern_meta(), false)
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
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
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
                .generate_optimized_match_plan_recursively(&mut catalog, &get_ldbc_pattern_meta(), false)
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
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
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
    fn expand_and_intersection_unfold_test_02() {
        create_exp_store();
        // A <-> B;
        let expand_opr1 = pb::EdgeExpand {
            v_tag: Some(TAG_B.into()),
            direction: 0,                                              // out
            params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
            expand_opt: 0,
            alias: Some(TAG_A.into()),
        };

        // A <-> C: expand C;
        let expand_opr2 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 1,                                              // in
            params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        // B <-> C: expand C and intersect on C;
        let expand_opr3 = pb::EdgeExpand {
            v_tag: Some(TAG_B.into()),
            direction: 1,                                              // in
            params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        // unfold tag C
        let unfold_opr = pb::Unfold { tag: Some(TAG_C.into()), alias: Some(TAG_C.into()) };
        println!("start executing query...");
        let query_execution_start_time = Instant::now();
        let conf = JobConf::new("expand_and_intersection_unfold_multiv_test");
        let mut result = pegasus::run(conf, || {
            let expand1 = expand_opr1.clone();
            let expand2 = expand_opr2.clone();
            let expand3 = expand_opr3.clone();
            let unfold = unfold_opr.clone();
            |input, output| {
                let source_iter = source_gen(Some(TAG_B.into()));
                let mut stream = input.input_from(source_iter)?;
                let flatmap_func1 = expand1.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func1.exec(input))?;
                let map_func2 = expand2.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| map_func2.exec(input))?;
                // let map_func3 = expand3.gen_filter_map().unwrap();
                // stream = stream.filter_map(move |input| map_func3.exec(input))?;
                // let unfold_func = unfold.gen_flat_map().unwrap();
                // stream = stream.flat_map(move |input| unfold_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut count = 0;
        while let Some(result) = result.next() {
            if let Ok(_) = result {
                count += 1;
            }
        }
        println!("{}", count);
        println!("executing query time cost is {:?} ms", query_execution_start_time.elapsed().as_millis());
    }

    #[test]
    fn expand_and_intersection_test_02() {
        create_exp_store();
        // A <-> B;
        let expand_opr1 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 1,                                              // in
            params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
            expand_opt: 0,
            alias: Some(TAG_B.into()),
        };

        // A <-> C: expand C;
        let expand_opr2 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 1,                                              // in
            params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        // B <-> C: expand C and intersect on C;
        let expand_opr3 = pb::EdgeExpand {
            v_tag: Some(TAG_B.into()),
            direction: 1,                                              // in
            params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        // unfold tag C
        let expand_intersect_opr = pb::ExpandAndIntersect { edge_expands: vec![expand_opr2, expand_opr3] };
        println!("start executing query...");
        let query_execution_start_time = Instant::now();
        let conf = JobConf::new("expand_and_intersection_unfold_multiv_test");
        let mut result = pegasus::run(conf, || {
            let expand1 = expand_opr1.clone();

            let expand_intersect = expand_intersect_opr.clone();
            |input, output| {
                let source_iter = source_gen_simple(Some(TAG_A.into()));
                let mut stream = input.input_from(source_iter)?;
                let flatmap_func1 = expand1.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func1.exec(input))?;
                let map_func2 = expand_intersect.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| map_func2.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut count = 0;
        while let Some(result) = result.next() {
            if let Ok(_) = result {
                count += 1;
            }
        }
        println!("{}", count);
        println!("executing query time cost is {:?} ms", query_execution_start_time.elapsed().as_millis());
    }

    #[test]
    fn pure_test() {
        create_exp_store();
        use graph_proxy::GRAPH;
        let conf = JobConf::new("pure test");
        let num_servers = if conf.servers().len() == 0 { 1 } else { conf.servers().len() };
        let worker_num = conf.workers as usize;
        println!("start executing query...");
        let query_execution_start_time = Instant::now();
        let mut result = pegasus::run(conf, move || {
            move |input, output| {
                let v_label_ids = vec![1];
                input
                    .input_from(
                        GRAPH
                            .get_all_vertices(Some(&v_label_ids))
                            .map(|v| (v.get_id() as u64))
                            .filter(move |v_id| {
                                let worker_index = pegasus::get_current_worker().index as u64;
                                get_partition(*v_id, num_servers, worker_num) == worker_index
                            })
                            .map(|v_id| {
                                Record::<SimpleEntry>::new(
                                    Vertex::new(v_id, None, DynDetails::Empty),
                                    Some(0),
                                )
                            }),
                    )?
                    // .repartition(move |id| Ok(get_partition(*id, num_servers, worker_num)))
                    .flat_map(|record| {
                        let v_id = record
                            .get(Some(0))
                            .unwrap()
                            .as_graph_vertex()
                            .unwrap()
                            .id();
                        let e_label_ids = vec![12];
                        let adj_vertices =
                            GRAPH.get_adj_vertices(v_id as usize, Some(&e_label_ids), Direction::Incoming);
                        Ok(adj_vertices.map(move |v| {
                            let mut new_record = record.clone();
                            new_record
                                .append(Vertex::new(v.get_id() as u64, None, DynDetails::Empty), Some(1));
                            new_record
                        }))
                    })?
                    // .repartition(move |path| Ok(get_partition(path[1], num_servers, worker_num)))
                    // .flat_map(|path| {
                    //     let extend_item_id = path[0];
                    //     let e_label_ids = vec![12];
                    //     let adj_vectices = GRAPH.get_adj_vertices(
                    //         extend_item_id as usize,
                    //         Some(&e_label_ids),
                    //         Direction::Incoming,
                    //     );
                    //     Ok(adj_vectices.map(move |v| {
                    //         let mut new_path = path.clone();
                    //         new_path.push(v.get_id() as u64);
                    //         new_path
                    //     }))
                    // })?
                    // .repartition(move |path| Ok(get_partition(path[1], num_servers, worker_num)))
                    .flat_map(|record| {
                        let extend_item_id0 = record
                            .get(Some(0))
                            .unwrap()
                            .as_graph_vertex()
                            .unwrap()
                            .id();
                        let extend_item_id1 = record
                            .get(Some(1))
                            .unwrap()
                            .as_graph_vertex()
                            .unwrap()
                            .id();
                        let e_label_ids = vec![12];
                        let adj_vectices0: HashSet<Vertex> = GRAPH
                            .get_adj_vertices(
                                extend_item_id0 as usize,
                                Some(&e_label_ids),
                                Direction::Incoming,
                            )
                            .map(|v| Vertex::new(v.get_id() as u64, None, DynDetails::Empty))
                            .collect();
                        let adj_vectices1: HashSet<Vertex> = GRAPH
                            .get_adj_vertices(
                                extend_item_id1 as usize,
                                Some(&e_label_ids),
                                Direction::Incoming,
                            )
                            .map(|v| Vertex::new(v.get_id() as u64, None, DynDetails::Empty))
                            .collect();
                        // let mut neighbors_intersection0 = Intersection::from_iter(adj_vectices0);
                        // let mut seeker = BTreeMap::new();
                        // for v in adj_vectices1 {
                        //     *seeker.entry(v).or_default() += 1;
                        // }
                        // neighbors_intersection0.intersect(&seeker);
                        let intersection: Vec<Vertex> = adj_vectices0
                            .intersection(&adj_vectices1)
                            .cloned()
                            .collect();
                        Ok(intersection.into_iter().map(move |vertex| {
                            let mut new_record = record.clone();
                            new_record.append(vertex, Some(2));
                            new_record
                        }))
                    })?
                    // .flat_map(|path| {
                    //     let extend_item_id0 = path[0];
                    //     let extend_item_id1 = path[1];
                    //     let e_label_ids = vec![12];
                    //     let adj_vectices1 = GRAPH
                    //         .get_adj_vertices(
                    //             extend_item_id1 as usize,
                    //             Some(&e_label_ids),
                    //             Direction::Outgoing,
                    //         )
                    //         .map(|v| v.get_id() as u64);
                    //     let vertices_set1: HashSet<u64> = adj_vectices1.collect();
                    //     let adj_vectices0 = GRAPH
                    //         .get_adj_vertices(
                    //             extend_item_id0 as usize,
                    //             Some(&e_label_ids),
                    //             Direction::Outgoing,
                    //         )
                    //         .map(|v| v.get_id() as u64);
                    //     // .filter(move |v| vertices_set1.contains(&(v.get_id() as u64)));
                    //     let vertices_set_0: HashSet<u64> = adj_vectices0.collect();
                    //     let intersection: Vec<u64> = vertices_set1
                    //         .intersection(&vertices_set_0)
                    //         .cloned()
                    //         .collect();
                    //     Ok(intersection.into_iter().map(move |v| {
                    //         let mut new_path = path.clone();
                    //         new_path.push(v);
                    //         new_path
                    //     }))
                    // })?
                    .sink_into(output)
            }
        })
        .unwrap();
        let mut count = 0;
        while let Some(result) = result.next() {
            if let Ok(_) = result {
                count += 1;
            }
        }
        println!("{}", count);
        println!("executing query time cost is {:?} ms", query_execution_start_time.elapsed().as_millis());
    }
}
