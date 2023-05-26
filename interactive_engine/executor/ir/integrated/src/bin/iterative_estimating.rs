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

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::error::Error;
use std::fs::File;
use std::{num, cmp};
use std::io::BufRead;
use std::path::{PathBuf};
use std::process::Command;
use std::time::Instant;
use graph_store::prelude::GlobalStoreTrait;
use structopt::StructOpt;

use crate::read_graph;
use graph_proxy::create_exp_store;
use ir_core::catalogue::catalog::PatMatPlanSpace;
use ir_core::catalogue::plan::{set_alpha, set_beta, set_w1, set_w2};
use ir_core::plan::logical::LogicalPlan;
use ir_core::plan::physical::AsPhysical;
use pegasus::{Configuration, JobConf};
use pegasus_client::builder::JobBuilder;
use runtime_integration::*;
use std::{thread, time, io};
// use ir_core::catalogue::sparsify::{
//     create_sparsified_graph, dump_edge_info, get_edge_distribution, read_sparsify_config, create_sparsified_bloom_filter, create_sparsified_hash_map,
// };
use bloom::{BloomFilter};

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
    #[structopt(short = "i", long = "iterative_time", default_value = "5")]
    iterative_time: u64,
    #[structopt(short = "f", long = "bloom_filter")]
    is_bloom_filter: bool
}

fn main() -> Result<(), Box<dyn Error>> {
    // let config = Config::from_args();
    // let graph = read_graph()?;
    // let graph2 = read_graph()?;
    // dump_edge_info(get_edge_distribution(graph2), &config.low_order_path);
    // let executed_command = "SPARSE_RATE=".to_string()
    //     + &config.sample_rate.to_string()
    //     + " SPARSE_STATISTIC_PATH="
    //     + &config.low_order_path
    //     + " SPARSE_RATE_PATH="
    //     + &config.sparsify_rate_path
    //     + " python3 "
    //     + config.optimizer_tools.as_str();
    // let mut generating_sparsify_rate = Command::new("sh");
    // generating_sparsify_rate
    //     .arg("-c")
    //     .arg(executed_command);
    // let _optimization_program = generating_sparsify_rate
    //     .output()
    //     .expect("failed to execute process");
    // // println!("{:?}",_optimization_program);
    // let mut sparsify_rate = read_sparsify_config(&config.sparsify_rate_path);
    // let mut naive_sparsify_rate = HashMap::new();
    // for (key, value) in sparsify_rate.clone() {
    //     naive_sparsify_rate.insert(key, config.sample_rate);
    // }
    // dump_edge_info(sparsify_rate.clone(), &config.sparsify_rate_path);
    // // println!("{:?}",config.is_unique_rate);
    // if config.is_unique_rate != "unique"{
    //     // println!("naive {:?}", naive_sparsify_rate);
    //     dump_edge_info(naive_sparsify_rate.clone(), &config.sparsify_rate_path);
    //     sparsify_rate = read_sparsify_config(&config.sparsify_rate_path)
    // }
    // // println!("{:?}", sparsify_rate);
    // let vertex_size = read_graph()?.count_all_edges(None) as u64;
    // let false_positive_rate = 0.01;

    // let mut b_filter = BloomFilter::with_rate(false_positive_rate,vertex_size as u32);
    
    // let mut h_filter = HashSet::new();
    // // let hashes = filter.num_hashes() as u64;
    // // println!("iter stddev fp sum rate");
    // for i in 0..config.iterative_time {

    //     let src_graph = read_graph()?;
    //     if config.is_bloom_filter {
    //         create_sparsified_bloom_filter(src_graph, sparsify_rate.clone(), config.export_path.clone(), &mut b_filter);
    //     }
    //     else{
    //         create_sparsified_hash_map(src_graph, sparsify_rate.clone(), config.export_path.clone(), &mut h_filter);
    //     }
    //     let pattern_path = std::env::var("PATTERN_PATH")?;
    //     let start = std::env::var("START")?;
    //     let end = std::env::var("END")?;
    //     let env_parameter = "PATTERN_PATH=".to_string()+&pattern_path+" START="+&start+" END="+&end;
    //     let pattern_match_command = "bash /Users/yanglaoyuan/Documents/GitHub/new_GraphScope/GraphScope/interactive_engine/executor/ir/Experiment/query_sparsify.sh";
    //     let mut interative_match_command = Command::new("sh");
    //     let output = interative_match_command
    //         .arg("-c")
    //         .arg(pattern_match_command)
    //         .output()
    //         .expect("executing sub-pattern match error");
        
    //     // println!("{:?}",output);
    //     assert!(output.status.success());
    //     // shell for running pattern match with sparsify graph as graph_path. 
    //     let duration = time::Duration::new(10,0);
    //     thread::sleep(duration);
    //     let mut sum = 0.0;
    //     let mut file = File::open("/Users/yanglaoyuan/Documents/GitHub/new_GraphScope/GraphScope/interactive_engine/executor/ir/Experiment/iterative_record.txt")?;
    //     let lines = io::BufReader::new(file).lines();
    //     let mut n = 0;
    //     for j in lines{
    //         let res = j?;
    //         let idx = res.parse::<u64>()?;
    //         if config.is_bloom_filter{
    //             if b_filter.contains(&idx){
    //                 sum += 1.0;
    //             }
    //             n += 1;
    //         }
    //         else {
    //             if h_filter.contains(&idx){
    //                 sum += 1.0;
    //             }
    //             n += 1;
    //         }
    //     }
    //     let edge_rate = config.sample_rate;
    //     let global_sample_rate = 1.0-(1.0-edge_rate).powi(i as i32 + 1i32);
    //     let probability_rate =edge_rate.powi(2)*global_sample_rate;
    //     let dynamic_fp_rate = 0;//(1.0-(1.0-false_positive_rate.powf(1.0/hashes as f32)).powf(global_sample_rate as f32)).powf(hashes as f32) as f64;
    //     let fp_revise_result = sum;//(sum - n as f64*dynamic_fp_rate as f64)/(1.0-dynamic_fp_rate as f64);
    //     let stddev = fp_revise_result/probability_rate-387573.0f64;
    //     // println!("{:?} ",stddev);
    //     println!("{:?}", sum);
    // }
    
    Ok(())
}
