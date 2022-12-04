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
    catalog_depth: usize,
    #[structopt(short = "t", long = "thread_num", default_value = "1")]
    thread_num: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args();
    let sample_graph = Arc::new(read_sample_graph()?);
    let pattern_meta = read_pattern_meta()?;
    println!("start building table log...");
    let table_log_build_start_time = Instant::now();
    let mut table_log = TableLogue::from_meta(&pattern_meta, config.catalog_depth);
    table_log.estimate_graph(sample_graph, 1.0, None, config.thread_num);
    println!("building table log time cost is: {:?} s", table_log_build_start_time.elapsed().as_secs());
    Ok(())
}
