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

use ir_core::catalogue::catalog::{Catalogue, PatMatPlanSpace};
use runtime_integration::{read_pattern, read_pattern_meta, read_patterns, read_sample_graph};
use ir_core::catalogue::sparsify::read_sparsify_config;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;
use structopt::StructOpt;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(StructOpt)]
pub struct Config {
    #[structopt(short = "m", long = "catalog_mode", default_value = "from_pattern")]
    catalog_mode: String,
    #[structopt(short = "d", long = "catalog_depth", default_value = "3")]
    catalog_depth: usize,
    #[structopt(short = "p", long = "export_path")]
    export_path: String,
    #[structopt(short = "s", long = "sparsify_rate_path", default_value = "sparsify_rate.json")]
    sparsify_rate_path: String,
    #[structopt(short = "t", long = "thread_num", default_value = "1")]
    thread_num: usize,
    #[structopt(short = "r", long = "sample_rate", default_value = "1.0")]
    sample_rate: f64,
    #[structopt(short = "l", long = "medium_results_limit")]
    limit: Option<usize>,
    #[structopt(short = "s", long = "plan_space", default_value = "hybrid")]
    plan_space: String,
}

fn print_config(config: &Config) {
    println!("Configuration:");
    println!("  Catalog mode: {}", config.catalog_mode);
    println!("  Catalog depth: {}", config.catalog_depth);
    println!("  export path: {}", config.export_path);
    println!("  sparsify_rate_path: {}", config.sparsify_rate_path);
    println!("  Num threads: {}", config.thread_num);
    println!("  Sample Rate: {}", config.sample_rate);
    println!("  Medium results limit: {:?}", config.limit);
    println!("  plan space: {}", config.plan_space);
    println!("");
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args();
    print_config(&config);
    let sample_graph = Arc::new(read_sample_graph()?);
    println!("start building catalog...");
    let plan_space: PatMatPlanSpace = match config.plan_space.as_str() {
        "extend" => PatMatPlanSpace::ExtendWithIntersection,
        "hybrid" => PatMatPlanSpace::Hybrid,
        _ => unreachable!(),
    };
    let catalog_build_start_time = Instant::now();
    let mut catalog = match config.catalog_mode.as_str() {
        "from_pattern" => {
            let pattern = read_pattern()?;
            Catalogue::build_from_pattern(&pattern, plan_space)
        }
        "from_patterns" => {
            let patterns = read_patterns()?;
            let mut catalog = Catalogue::default();
            catalog.set_plan_space(plan_space);
            for pattern in patterns {
                catalog.update_catalog_by_pattern(&pattern)
            }
            catalog
        }
        "from_meta" => {
            let pattern_meta = read_pattern_meta()?;
            Catalogue::build_from_meta(&pattern_meta, config.catalog_depth, config.catalog_depth)
        }
        "mix_mode" => {
            let pattern_meta = read_pattern_meta()?;
            let patterns = read_patterns()?;
            let mut catalog =
                Catalogue::build_from_meta(&pattern_meta, config.catalog_depth, config.catalog_depth);
            catalog.set_plan_space(plan_space);
            for pattern in patterns {
                catalog.update_catalog_by_pattern(&pattern)
            }
            catalog
        }
        _ => unreachable!(),
    };
    let sparsify_rate = read_sparsify_config(&config.sparsify_rate_path);
    catalog.estimate_graph(
        sample_graph,
        config.sample_rate,
        sparsify_rate,
        config.limit,
        config.thread_num,
    );
    println!("building catalog time cost is: {:?} s", catalog_build_start_time.elapsed().as_secs());
    catalog.export(config.export_path)?;
    Ok(())
}
