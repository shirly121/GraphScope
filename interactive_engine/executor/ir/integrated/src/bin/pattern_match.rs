//
//! Copyright 2022 Alibaba Group Holding Limited.
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

use graph_proxy::create_exp_store;
use ir_common::generated::algebra as pb;
use ir_core::plan::logical::LogicalPlan;
use ir_core::plan::physical::AsPhysical;
use pegasus::{Configuration, JobConf};
use pegasus_client::builder::JobBuilder;
use runtime_integration::*;
use std::convert::TryInto;
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;
use structopt::StructOpt;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(StructOpt)]
pub struct Config {
    #[structopt(short = "c", long = "config_dir")]
    config_dir: Option<PathBuf>,
    #[structopt(short = "w", long = "workers", default_value = "1")]
    workers: u32,
    #[structopt(short = "d", long = "is_distributed")]
    is_distributed: bool,
    #[structopt(short = "p", long = "print_intermediate_result")]
    print_intermediate_result: bool,
    #[structopt(short = "n", long = "no_query_execution")]
    no_query_execution: bool,
}

// lazy_static! {
//     static ref FACTORY: IRJobAssembly = initialize_job_assembly();
// }

fn print_config(config: &Config) {
    println!("Configuration:");
    println!("  config_dir: {:?}", config.config_dir);
    println!("  num_workers: {}", config.workers);
    println!("  is_distributed: {}", config.is_distributed);
    println!("  print_intermediate_result: {}", config.print_intermediate_result);
    println!(" no_query_execution: {}", config.no_query_execution);
    println!("");
}

fn split_intermediate_pb_logical_plan(pb_plan: &pb::LogicalPlan) -> Vec<pb::LogicalPlan> {
    use pb::logical_plan::operator::Opr;
    let mut intermediate_pb_plans: Vec<pb::LogicalPlan> = vec![];
    let mut intermediate_pb_plan: pb::LogicalPlan = pb::LogicalPlan::default();
    intermediate_pb_plan.roots = vec![];
    let pb_plan_len = pb_plan.nodes.len();
    for node_idx in 0..pb_plan_len {
        let pb_node = &pb_plan.nodes[node_idx];
        let opr = pb_node
            .opr
            .as_ref()
            .unwrap()
            .opr
            .as_ref()
            .unwrap();
        match opr {
            Opr::Edge(_opr) => {
                intermediate_pb_plans.push(intermediate_pb_plan.clone());
            }
            Opr::ExpandIntersect(_opr) => {
                intermediate_pb_plans.push(intermediate_pb_plan.clone());
            }
            _ => {}
        };
        intermediate_pb_plan.nodes.push(pb_node.clone());
    }

    return intermediate_pb_plans;
}

fn main() -> Result<(), Box<dyn Error>> {
    pegasus_common::logs::init_log();
    create_exp_store();
    let config = Config::from_args();
    print_config(&config);
    let server_config = if let Some(config_dir) = config.config_dir {
        pegasus_server::config::load_configs(config_dir)?.0
    } else {
        Configuration::singleton()
    };
    let mut conf = JobConf::new("GLogue Universal Test");
    conf.set_workers(config.workers);
    conf.reset_servers(pegasus::ServerConf::All);
    pegasus::startup(server_config)?;
    pegasus::wait_servers_ready(&conf.servers());
    let pattern_meta = read_pattern_meta()?;
    let pattern = read_pattern()?;
    let mut catalog = read_catalogue()?;
    println!("############ Plan Generation ############");
    println!("start generating plan...");
    let plan_generation_start_time = Instant::now();
    let mut pb_plan = pattern.generate_optimized_match_plan_recursively(
        &mut catalog,
        &pattern_meta,
        config.is_distributed,
    )?;
    println!("generating plan time cost is: {:?} ms", plan_generation_start_time.elapsed().as_millis());
    if config.print_intermediate_result {
        // split the original logical plan into plans of intermediate results
        println!("############ Print Intermediate Results ############");
        let mut intermediate_pb_plans: Vec<pb::LogicalPlan> = split_intermediate_pb_logical_plan(&pb_plan);
        intermediate_pb_plans
            .iter_mut()
            .for_each(|intermediate_pb_plan| {
                match_pb_plan_add_source(intermediate_pb_plan);
                pb_plan_add_count_sink_operator(intermediate_pb_plan);
                print_pb_logical_plan(&intermediate_pb_plan);
                // Submit Query to get the intermediate results
                let plan: LogicalPlan = intermediate_pb_plan
                    .clone()
                    .try_into()
                    .expect("Failed to convert to logical plan");
                let mut job_builder = JobBuilder::default();
                let mut plan_meta = plan.get_meta().clone().with_partition();
                plan.add_job_builder(&mut job_builder, &mut plan_meta)
                    .unwrap();
                let request = job_builder
                    .build()
                    .expect("Failed at job builder");
                println!("start executing query...");
                submit_query(request, conf.clone()).expect("Failed to submit query");
                println!("\n\n");
            });
    }

    match_pb_plan_add_source(&mut pb_plan);
    pb_plan_add_count_sink_operator(&mut pb_plan);
    println!("Final pb logical plan:");
    print_pb_logical_plan(&pb_plan);

    if !config.no_query_execution {
        println!("############ Query Execution ############");
        let plan: LogicalPlan = pb_plan
            .try_into()
            .expect("Failed to convert to logical plan");
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.get_meta().clone().with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();
        let request = job_builder
            .build()
            .expect("Failed at job builder");
        println!("start executing query...");
        submit_query(request, conf.clone()).expect("Failed to submit query");
    }
    Ok(())
}
