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
use ir_core::catalogue::extend_step::DefiniteExtendStep;
use ir_core::catalogue::pattern::Pattern;
use ir_core::catalogue::plan::PlanPath;
use ir_core::catalogue::PatternId;
use ir_core::plan::logical::{match_pb_plan_add_source, LogicalPlan};
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
    #[structopt(short = "o", long = "order")]
    order: String,
    #[structopt(short = "s", long = "split", default_value = " ")]
    split: String,
    #[structopt(short = "w", long = "workers", default_value = "1")]
    workers: u32,
    #[structopt(short = "d", long = "is_distributed")]
    is_distributed: bool,
    #[structopt(long = "batch_size", default_value = "1024")]
    batch_size: u32,
    #[structopt(long = "batch_capacity", default_value = "64")]
    batch_capacity: u32,
}

fn main() -> Result<(), Box<dyn Error>> {
    pegasus_common::logs::init_log();
    create_exp_store();
    let config = Config::from_args();
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
    let pattern_meta = read_pattern_meta()?;
    let pattern = read_pattern()?;
    let order = get_pattern_extend_order(config.order, &config.split)?;
    println!("start generating plan...");
    let plan_generation_start_time = Instant::now();
    let mut plan_path = PlanPath::default();
    for extend_step in get_definite_extend_steps_from_pattern_with_order(&pattern, order) {
        plan_path.add_plan_step_front(extend_step.into());
    }
    let mut pb_plan = plan_path.to_match_plan(config.is_distributed, &pattern, &pattern_meta)?;
    println!("generating plan time cost is: {:?} ms", plan_generation_start_time.elapsed().as_millis());
    match_pb_plan_add_source(&mut pb_plan);
    pb_plan_add_count_sink_operator(&mut pb_plan);
    print_pb_logical_plan(&pb_plan);
    let plan: LogicalPlan = pb_plan.try_into()?;
    let mut job_builder = JobBuilder::default();
    let mut plan_meta = plan.get_meta().clone().with_partition();
    plan.add_job_builder(&mut job_builder, &mut plan_meta)
        .unwrap();
    let request = job_builder.build()?;
    println!("start executing query...");
    submit_query(request, conf)?;
    Ok(())
}

pub fn get_pattern_extend_order(order_str: String, split: &str) -> Result<Vec<PatternId>, Box<dyn Error>> {
    let order: Vec<PatternId> = order_str
        .split(split)
        .into_iter()
        .map(|s| {
            s.parse()
                .expect("element of order should be a number")
        })
        .collect();
    Ok(order)
}

fn get_definite_extend_steps_from_pattern_with_order(
    pattern: &Pattern, mut order: Vec<PatternId>,
) -> Vec<DefiniteExtendStep> {
    let mut trace_pattern = pattern.clone();
    let mut extend_steps = vec![];
    while let Some(vertex_id) = order.pop() {
        let extend_step = DefiniteExtendStep::from_target_pattern(&trace_pattern, vertex_id).unwrap();
        extend_steps.push(extend_step);
        if trace_pattern.get_vertices_num() > 1 {
            trace_pattern = trace_pattern.remove_vertex(vertex_id).unwrap();
        }
    }
    extend_steps
}
