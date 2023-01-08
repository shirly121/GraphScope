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

use ir_core::catalogue::PatternId;
use std::error::Error;
use structopt::StructOpt;

use runtime_integration::{read_catalogue, read_pattern};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(StructOpt)]
pub struct Config {
    #[structopt(short = "o", long = "order")]
    order: String,
    #[structopt(short = "s", long = "split", default_value = " ")]
    split: String,
    #[structopt(short = "p", long = "export_path")]
    export_path: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let config = Config::from_args();
    let catalog = read_catalogue()?;
    let pattern = read_pattern()?;
    let mut order = get_pattern_extend_order(config.order, &config.split)?;
    let mut trace_pattern = pattern.clone();
    let trace_pattern_id = catalog
        .get_pattern_index(&trace_pattern.encode_to())
        .unwrap();
    let mut pattern_indices = vec![trace_pattern_id];
    while let Some(vertex_id) = order.pop() {
        if trace_pattern.get_vertices_num() > 1 {
            trace_pattern = trace_pattern.remove_vertex(vertex_id).unwrap();
            let trace_pattern_id = catalog
                .get_pattern_index(&trace_pattern.encode_to())
                .unwrap();
            pattern_indices.push(trace_pattern_id);
        }
    }
    pattern_indices.reverse();
    let picked_catalog = catalog.pick_catalog(pattern_indices);
    picked_catalog.export(config.export_path)?;
    Ok(())
}

fn get_pattern_extend_order(order_str: String, split: &str) -> Result<Vec<PatternId>, Box<dyn Error>> {
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
