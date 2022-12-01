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

use ir_core::catalogue::sparsify::{
    create_sparsified_graph, dump_edge_info, get_edge_distribution, read_sparsify_config,
};
use runtime_integration::read_graph;
use std::collections::HashMap;
use std::error::Error;
use std::process::Command;
use structopt::StructOpt;
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(StructOpt)]
pub struct Config {
    #[structopt(short = "p", long = "export_path")]
    export_path: String,
    #[structopt(short = "r", long = "sample_rate", default_value = "0.1")]
    sample_rate: f64,
    #[structopt(short = "l", long = "low_order_path", default_value = "edge_sta.json")]
    low_order_path: String,
    #[structopt(short = "s", long = "sparsify_rate_path", default_value = "sparsify_rate.json")]
    sparsify_rate_path: String,
    #[structopt(short = "t", long = "optimizer_tools", default_value = "Sparsify.py")]
    optimizer_tools: String,
    #[structopt(short = "m", long = "rate_mod", default_value = "unique")]
    is_unique_rate: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args();
    let graph = read_graph()?;
    let graph2 = read_graph()?;
    dump_edge_info(get_edge_distribution(graph2), &config.low_order_path);
    let executed_command = "SPARSE_RATE=".to_string()
        + &config.sample_rate.to_string()
        + " SPARSE_STATISTIC_PATH="
        + &config.low_order_path
        + " SPARSE_RATE_PATH="
        + &config.sparsify_rate_path
        + " python3 "
        + config.optimizer_tools.as_str();
    let mut generating_sparsify_rate = Command::new("sh");
    generating_sparsify_rate
        .arg("-c")
        .arg(executed_command);
    let _optimization_program = generating_sparsify_rate
        .output()
        .expect("failed to execute process");
    // println!("{:?}",_optimization_program);
    let sparsify_rate = read_sparsify_config(&config.sparsify_rate_path);
    let mut naive_sparsify_rate = HashMap::new();
    for (key, value) in sparsify_rate.clone() {
        naive_sparsify_rate.insert(key, value);
    }
    dump_edge_info(sparsify_rate.clone(), &config.sparsify_rate_path);
    if config.is_unique_rate == "unique" {
        create_sparsified_graph(graph, sparsify_rate, config.export_path);
    } else {
        dump_edge_info(naive_sparsify_rate.clone(), &config.sparsify_rate_path);
        create_sparsified_graph(graph, naive_sparsify_rate, config.export_path);
    }
    Ok(())
}
