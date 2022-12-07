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

use ir_core::catalogue::catalog::TableLogue;
use ir_core::catalogue::PatternLabelId;
use runtime_integration::{read_pattern_meta, read_sample_graph};
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;
use structopt::StructOpt;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(StructOpt)]
pub struct Config {
    #[structopt(short = "d", long = "catalog_depth", default_value = "3")]
    table_log_depth: usize,
    #[structopt(short = "m", long = "table_log_mode", default_value = "from_meta")]
    table_log_mode: String,
    #[structopt(short = "t", long = "thread_num", default_value = "1")]
    thread_num: usize,
    #[structopt(short = "r", long = "sample_rate", default_value = "1.0")]
    sample_rate: f64,
    #[structopt(short = "l", long = "medium_results_limit")]
    limit: Option<usize>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args();
    let sample_graph = Arc::new(read_sample_graph()?);
    let pattern_meta = read_pattern_meta()?;
    println!("start building table log...");
    let table_log_build_start_time = Instant::now();
    let mut table_log = match config.table_log_mode.as_str() {
        "from_meta" => TableLogue::from_meta(&pattern_meta, config.table_log_depth),
        "naive" => {
            let vertex_labels: Vec<PatternLabelId> = pattern_meta.vertex_label_ids_iter().collect();
            let edge_labels: Vec<PatternLabelId> = pattern_meta.edge_label_ids_iter().collect();
            TableLogue::naive_init(vertex_labels, edge_labels, config.table_log_depth)
        }
        _ => unreachable!(),
    };
    table_log.estimate_graph(sample_graph, config.sample_rate, config.limit, config.thread_num);
    println!("building table log time cost is: {:?} s", table_log_build_start_time.elapsed().as_secs());
    println!("{:?}", table_log.iter().count());
    Ok(())
}
