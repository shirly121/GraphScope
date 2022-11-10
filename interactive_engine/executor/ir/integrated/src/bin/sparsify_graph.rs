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

use ir_core::catalogue::sparsify::create_sparsified_graph;
use runtime_integration::read_graph;
use std::error::Error;
use structopt::StructOpt;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(StructOpt)]
pub struct Config {
    #[structopt(short = "p", long = "export_path")]
    export_path: String,
    #[structopt(short = "r", long = "sample_rate", default_value = "0.1")]
    sample_rate: f64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args();
    let graph = read_graph()?;
    create_sparsified_graph(graph, config.sample_rate, config.export_path);
    Ok(())
}
