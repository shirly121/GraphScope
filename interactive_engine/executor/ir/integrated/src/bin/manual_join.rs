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

use std::collections::HashMap;
use std::convert::TryInto;
use std::error::Error;
use std::path::PathBuf;
use std::time::Instant;
use structopt::StructOpt;

use graph_proxy::create_exp_store;
use ir_common::generated::algebra as pb;
use ir_core::catalogue::catalog::{Catalogue, PatMatPlanSpace};
use ir_core::catalogue::pattern::Pattern;
use ir_core::catalogue::plan::PlanGenerator;
use ir_core::catalogue::join_step::BinaryJoinPlan;
use ir_core::error::IrResult;
use ir_core::plan::logical::LogicalPlan;
use ir_core::plan::physical::AsPhysical;
use pegasus::{Configuration, JobConf};
use pegasus_client::builder::JobBuilder;
use runtime_integration::*;

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
    #[structopt(required = true, long = "build_pattern_path")]
    build_pattern_path: String,
    #[structopt(required = true, long = "probe_pattern_path")]
    probe_pattern_path: String,
    #[structopt(long = "batch_size", default_value = "1024")]
    batch_size: u32,
    #[structopt(long = "batch_capacity", default_value = "64")]
    batch_capacity: u32,
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
    println!("  no_query_execution: {}", config.no_query_execution);
    println!("  build pattern path: {}", config.build_pattern_path);
    println!("  probe pattern path: {}", config.probe_pattern_path);
    println!("");
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
    conf.batch_size = config.batch_size;
    conf.batch_capacity = config.batch_capacity;
    pegasus::startup(server_config)?;
    pegasus::wait_servers_ready(&conf.servers());
    
    println!("############ Plan Generation ############");
    println!("start generating plan...");
    let build_pattern = read_pattern_from_path(config.build_pattern_path)
        .expect("Failed to read pattern from path");
    let probe_pattern = read_pattern_from_path(config.probe_pattern_path)
        .expect("Failed to read pattern from path");
    let plan_generation_start_time = Instant::now();
    let pb_plan = generate_manual_join_plan(build_pattern, probe_pattern, config.is_distributed)
        .expect("Failed to generate manual join plan");
    println!("generating plan time cost is: {:?} ms", plan_generation_start_time.elapsed().as_millis());
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

fn generate_manual_join_plan(build_pattern: Pattern, probe_pattern: Pattern, is_distributed: bool) -> Result<pb::LogicalPlan, Box<dyn Error>> {
    let join_plan = BinaryJoinPlan::init(build_pattern, probe_pattern);
    let pattern_meta = read_pattern_meta()?;
    let mut catalog = read_catalogue()?;
    catalog.set_plan_space(PatMatPlanSpace::ExtendWithIntersection);
    // Set best approach for build and probe patterns
    catalog.set_best_approach_by_pattern(join_plan.get_build_pattern());
    catalog.set_best_approach_by_pattern(join_plan.get_probe_pattern());
    // Generate plan for build pattern
    let mut build_pattern_plan_generator =
        PlanGenerator::new(join_plan.get_build_pattern(), &catalog, &pattern_meta, is_distributed);
    build_pattern_plan_generator
        .generate_pattern_match_plan_recursively(join_plan.get_build_pattern())
        .expect("Failed to generate optimized pattern match plan recursively");;
    // Generate plan for probe pattterns
    let mut probe_pattern_plan_generator =
        PlanGenerator::new(join_plan.get_probe_pattern(), &catalog, &pattern_meta, is_distributed);
    probe_pattern_plan_generator
        .generate_pattern_match_plan_recursively(join_plan.get_probe_pattern())
        .expect("Failed to generate optimized pattern match plan recursively");
    // Append binary join operator to join two plans
    let join_keys = join_plan.generate_join_keys();
    build_pattern_plan_generator.join(probe_pattern_plan_generator, join_keys)
        .expect("Failed to join two logical plans");
    // Append scan and count/sink nodes in logical plan
    build_pattern_plan_generator.match_pb_plan_add_source();
    build_pattern_plan_generator.pb_plan_add_count_sink_operator();
    Ok(build_pattern_plan_generator.get_pb_plan())
}
