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
    use std::convert::{TryFrom, TryInto};
    use std::env;
    use std::sync::Arc;
    use std::time::Instant;

    use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
    use graph_store::prelude::{
        DefaultId, GlobalStoreTrait, GlobalStoreUpdate, GraphDBConfig, InternalId, LargeGraphDB,
        MutableGraphDB,
    };
    use ir_common::generated::algebra::{self as pb, logical_plan};
    use ir_common::generated::common::{self as common_pb};
    use ir_common::generated::results as result_pb;
    use ir_common::KeyId;
    use ir_core::catalogue::catalog::{Catalogue, PatMatPlanSpace};
    use ir_core::catalogue::pattern::Pattern;
    use ir_core::catalogue::{PatternId, PatternLabelId};
    use ir_core::error::{IrError, IrResult};
    use ir_core::plan::logical::LogicalPlan;
    use ir_core::plan::physical::AsPhysical;
    use pegasus::result::{ResultSink, ResultStream};
    use pegasus_client::builder::JobBuilder;
    use pegasus_server::JobRequest;
    use prost::Message;
    use runtime::process::record::{Entry, Record};

    use crate::common::{pattern_cases::from_edges::*, test::*};

    const IS_DISTRIBUTED: bool = false;

    fn generate_pattern_match_plan(
        pattern: &Pattern, catalogue: &Catalogue, is_distributed: bool,
    ) -> IrResult<pb::LogicalPlan> {
        println!("start generating plan...");
        let plan_generation_start_time = Instant::now();
        let pb_plan: pb::LogicalPlan = pattern
            .generate_optimized_match_plan(&mut catalogue.clone(), &get_ldbc_pattern_meta(), is_distributed)
            .expect("Failed to generate pattern match plan");
        println!("generating plan time cost is: {:?} ms", plan_generation_start_time.elapsed().as_millis());
        print_pb_logical_plan(&pb_plan);
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
        while let Some(result) = results.next() {
            if let Ok(record_code) = result {
                if let Some(record) = parse_result(record_code) {
                    println!("{:?}", record);
                }
            }
        }

        println!("executing query time cost is {:?} ms", query_execution_start_time.elapsed().as_millis());
    }

    fn parse_result(result: Vec<u8>) -> Option<Record> {
        let result: result_pb::Results = result_pb::Results::decode(result.as_slice()).unwrap();
        if let Some(result_pb::results::Inner::Record(record_pb)) = result.inner {
            let mut record = Record::default();
            for column in record_pb.columns {
                let tag: Option<KeyId> = if let Some(tag) = column.name_or_id {
                    match tag.item.unwrap() {
                        common_pb::name_or_id::Item::Name(name) => Some(
                            name.parse::<KeyId>()
                                .unwrap_or(KeyId::max_value()),
                        ),
                        common_pb::name_or_id::Item::Id(id) => Some(id),
                    }
                } else {
                    None
                };
                let entry = column.entry.unwrap();
                // append entry without moving head
                if let Some(tag) = tag {
                    let columns = record.get_columns_mut();
                    columns.insert(tag as usize, Arc::new(Entry::try_from(entry).unwrap()));
                } else {
                    record.append(Entry::try_from(entry).unwrap(), None);
                }
            }
            Some(record)
        } else {
            None
        }
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_basecase1() {
        let ldbc_pattern = build_ldbc_basecase1().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_basecase1() {
        let pattern = build_ldbc_basecase1().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_basecase2() {
        let ldbc_pattern = build_ldbc_basecase2().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_basecase2() {
        let pattern = build_ldbc_basecase2().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_basecase3() {
        let ldbc_pattern = build_ldbc_basecase3().unwrap();
        let mut pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        pb_plan.roots = vec![1];
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_basecase3() {
        let pattern = build_ldbc_basecase3().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_basecase4() {
        let ldbc_pattern = build_ldbc_basecase4().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_basecase4() {
        let pattern = build_ldbc_basecase4().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_basecase5() {
        let ldbc_pattern = build_ldbc_basecase5().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_basecase5() {
        let pattern = build_ldbc_basecase5().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_basecase6() {
        let ldbc_pattern = build_ldbc_basecase6().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();
        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_basecase6() {
        let pattern = build_ldbc_basecase6().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
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
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
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
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_gb9() {
        let ldbc_pattern = build_ldbc_gb9().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_gb9() {
        let pattern = build_ldbc_gb9().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_gb10() {
        let ldbc_pattern = build_ldbc_gb10().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_gb10() {
        let pattern = build_ldbc_gb10().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_gb11() {
        let ldbc_pattern = build_ldbc_gb11().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_gb11() {
        let pattern = build_ldbc_gb11().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_gb14() {
        let ldbc_pattern = build_ldbc_gb14().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_gb14() {
        let pattern = build_ldbc_gb14().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_gb15() {
        let ldbc_pattern = build_ldbc_gb15().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_gb15() {
        let pattern = build_ldbc_gb15().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_gb16() {
        let ldbc_pattern = build_ldbc_gb16().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_gb16() {
        let pattern = build_ldbc_gb16().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_gb17() {
        let ldbc_pattern = build_ldbc_gb17().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_gb17() {
        let pattern = build_ldbc_gb17().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_gb18() {
        let ldbc_pattern = build_ldbc_gb18().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_gb18() {
        let pattern = build_ldbc_gb18().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_naive_pattern_match_plan_for_ldbc_gb20() {
        let ldbc_pattern = build_ldbc_gb20().unwrap();
        let pb_plan = ldbc_pattern
            .generate_simple_extend_match_plan(&get_ldbc_pattern_meta(), false)
            .unwrap();

        print_pb_logical_plan(&pb_plan);
        execute_pb_logical_plan(pb_plan);
    }

    #[test]
    fn generate_optimized_pattern_match_plan_for_ldbc_gb20() {
        let pattern = build_ldbc_gb20().unwrap();
        // Naive Extend-Based Plan
        println!("Extend-Based Plan:");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::ExtendWithIntersection);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
        println!("\n\n");

        // Naive Hybrid Plan
        println!("Hybrid Plan: ");
        let catalogue = Catalogue::build_from_pattern(&pattern, PatMatPlanSpace::Hybrid);
        let pb_plan = generate_pattern_match_plan(&pattern, &catalogue, false)
            .expect("Failed to generate pattern match plan");
        execute_pb_logical_plan(pb_plan);
    }
}
