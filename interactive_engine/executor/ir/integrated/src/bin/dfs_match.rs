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
use std::fs;
use std::convert::TryInto;
use std::error::Error;

use std::mem::size_of;
use std::path::PathBuf;
use std::process::Command;
use std::time::Instant;
use bloom::BloomFilter;
use rand::{thread_rng, Rng};
use graph_store::prelude::GlobalStoreTrait;
use structopt::StructOpt;

use graph_proxy::create_exp_store;

use ir_core::catalogue::sparsify::{
    create_sparsified_graph, dump_edge_info, get_edge_distribution, read_sparsify_config,create_sparsified_bloom_filter, create_sparsified_hash_set, create_sparsified_intersect
};
use ir_core::plan::logical::LogicalPlan;
use ir_core::plan::physical::AsPhysical;
use ir_core::catalogue::dfs_match::{dfs_match_bf, dfs_match_hs, dfs_match_ih, dfs_match_ig, gen_plan};
use pegasus::{Configuration, JobConf};
use pegasus_client::builder::JobBuilder;
use runtime_integration::*;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(StructOpt)]
pub struct Config {
    #[structopt(long = "mode", default_value = "single")]
    mode: String,
    #[structopt(short = "p", long = "export_path")]
    export_path: String,
    #[structopt(short = "l", long = "low_order_path", default_value = "edge_sta.json")]
    low_order_path: String,
    #[structopt(short = "s", long = "sparsify_rate_path", default_value = "sparsify_rate.json")]
    sparsify_rate_path: String,
    #[structopt(short = "t", long = "optimizer_tools", default_value = "Sparsify.py")]
    optimizer_tools: String,
    #[structopt(long = "hash", default_value = "3")]
    hash: u32,
    #[structopt(short = "a", long = "auto_hash_num")]
    auto_hash: bool,
    #[structopt(short = "i",long = "iterative_run", default_value = "1")]
    iterative_run: i32,
    #[structopt(short = "b", long = "bound_type", default_value = "mem")]
    bound_type: String,
    #[structopt(short = "r", long = "sample_rate", default_value = "0.1")]
    sample_rate: f64,
    #[structopt(short = "m", long = "rate_mod")]
    is_unique_rate: bool,
    #[structopt( long = "ground_truth")]
    ground_truth: bool,
    #[structopt(short = "x", long = "intersect_mode", default_value = "")]
    intersect_mode: String,
    #[structopt(short = "g",long = "global_size", default_value = "1073741824")]
    global_size: usize,
    #[structopt(long = "baseline", default_value = "1")]
    baseline: f64,
    #[structopt(long = "bm_rate", default_value = "0.5")]
    bm_rate: f64,
}

fn pre_process_bf(config: &Config, bf: & mut BloomFilter) ->HashMap<(u8, u8, u8), f64>{

    let org_graph=read_graph().unwrap();
    let edge_bytes = org_graph.count_all_edges(None) * size_of::<&usize>();
    let mut sparse_rate = config.global_size as f64/edge_bytes as f64 * (1.0-config.bm_rate);
    let v_size = org_graph.count_all_vertices(None);
    dump_edge_info(get_edge_distribution(&org_graph), &config.low_order_path);
    if config.bound_type == "mem" {
        sparse_rate = sparse_rate.min(1.0);
    }
    else if config.bound_type == "rate" {
        sparse_rate = config.sample_rate;
    }
    
    if config.is_unique_rate {
        let executed_command = "SPARSE_RATE=".to_string()
            + &sparse_rate.to_string()
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

    }
    else {
        let edge_list = read_sparsify_config(&config.low_order_path);
        let mut naive_sparsify_rate = HashMap::new();
        for (key, value) in edge_list.clone() {
            naive_sparsify_rate.insert(key, sparse_rate);
        }
        dump_edge_info(naive_sparsify_rate.clone(), &config.sparsify_rate_path);
    }
    let sparsify_rate = read_sparsify_config(&config.sparsify_rate_path);
    create_sparsified_bloom_filter(&org_graph, &sparsify_rate, &config.export_path, bf);
    sparsify_rate
}

fn pre_process_both(config: &Config, bf2: & mut BloomFilter, bf: & mut HashSet<usize>) ->HashMap<(u8, u8, u8), f64>{

    let org_graph=read_graph().unwrap();
    let edge_bytes = org_graph.count_all_edges(None) * size_of::<&usize>();
    let mut sparse_rate = config.global_size as f64/edge_bytes as f64 * (1.0-config.bm_rate);
    let v_size = org_graph.count_all_vertices(None);
    dump_edge_info(get_edge_distribution(&org_graph), &config.low_order_path);
    if config.bound_type == "mem" {
        sparse_rate = sparse_rate.min(1.0);
    }
    else if config.bound_type == "rate" {
        sparse_rate = config.sample_rate;
    }
    
    if config.is_unique_rate {
        let executed_command = "SPARSE_RATE=".to_string()
            + &sparse_rate.to_string()
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

    }
    else {
        let edge_list = read_sparsify_config(&config.low_order_path);
        let mut naive_sparsify_rate = HashMap::new();
        for (key, value) in edge_list.clone() {
            naive_sparsify_rate.insert(key, sparse_rate);
        }
        dump_edge_info(naive_sparsify_rate.clone(), &config.sparsify_rate_path);
    }
    let sparsify_rate = read_sparsify_config(&config.sparsify_rate_path);
    create_sparsified_bloom_filter(&org_graph, &sparsify_rate, &config.export_path, bf2);
    create_sparsified_hash_set(&org_graph, &sparsify_rate, &config.export_path, bf);
    sparsify_rate
}


fn pre_process_hs(config: &Config, bf: & mut HashSet<usize>) ->HashMap<(u8, u8, u8), f64>{

    let org_graph=read_graph().unwrap();
    let edge_bytes = org_graph.count_all_edges(None) * size_of::<&usize>();
    let mut sparse_rate = config.global_size as f64/edge_bytes as f64 * 0.5;
    let v_size = org_graph.count_all_vertices(None);
    dump_edge_info(get_edge_distribution(&org_graph), &config.low_order_path);
    if config.bound_type == "mem" {
        sparse_rate = sparse_rate.min(1.0);
    }
    else if config.bound_type == "rate" {
        sparse_rate = config.sample_rate;
    }
    
    if config.is_unique_rate {
        let executed_command = "SPARSE_RATE=".to_string()
            + &sparse_rate.to_string()
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

    }
    else {
        let edge_list = read_sparsify_config(&config.low_order_path);
        let mut naive_sparsify_rate = HashMap::new();
        for (key, value) in edge_list.clone() {
            naive_sparsify_rate.insert(key, sparse_rate);
        }
        dump_edge_info(naive_sparsify_rate.clone(), &config.sparsify_rate_path);
    }
    let sparsify_rate = read_sparsify_config(&config.sparsify_rate_path);
    create_sparsified_hash_set(&org_graph, &sparsify_rate, &config.export_path, bf);
    sparsify_rate
}


fn pre_process_i(config: &Config) ->HashMap<(u8, u8, u8), f64>{

    let org_graph=read_graph().unwrap();
    let edge_bytes = org_graph.count_all_edges(None) * size_of::<&usize>();
    let mut sparse_rate = config.global_size as f64/edge_bytes as f64;
    let v_size = org_graph.count_all_vertices(None);
    dump_edge_info(get_edge_distribution(&org_graph), &config.low_order_path);
    if config.bound_type == "mem" {
        sparse_rate = sparse_rate.min(1.0);
    }
    else if config.bound_type == "rate" {
        sparse_rate = config.sample_rate;
    }
    
    if config.is_unique_rate {
        let executed_command = "SPARSE_RATE=".to_string()
            + &sparse_rate.to_string()
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

    }
    else {
        let edge_list = read_sparsify_config(&config.low_order_path);
        let mut naive_sparsify_rate = HashMap::new();
        for (key, value) in edge_list.clone() {
            naive_sparsify_rate.insert(key, sparse_rate);
        }
        dump_edge_info(naive_sparsify_rate.clone(), &config.sparsify_rate_path);
    }
    let sparsify_rate = read_sparsify_config(&config.sparsify_rate_path);
    create_sparsified_intersect(&org_graph, &sparsify_rate, &config.export_path);
    sparsify_rate
}

fn test_fp_rate(config: &Config, i: i32) -> f64{

    let e_size = read_graph().unwrap().count_all_edges(None);
    let e_byte = e_size* size_of::<&usize>();
    let sparse_rate;
    if config.bound_type == "mem" {
        sparse_rate = (config.global_size as f64/e_byte as f64 * (1.0-config.bm_rate) as f64).min(1.0);
    }
    else if config.bound_type == "rate" {
        sparse_rate = config.sample_rate;
    }
    else{
        sparse_rate=0.0;
    }
    let bm_size;
    if config.bound_type== "rate" {
        bm_size = config.global_size;
    }
    else {
        bm_size = (config.global_size as f64 * config.bm_rate) as usize;
    }
    let hash;
    if config.auto_hash {

        let optimal_hash = bm_size as f64/ (e_size as f64 * sparse_rate) * 2.0_f64.ln() as f64;
        hash = optimal_hash.ceil() as u32;
    }
    else {
        hash = config.hash;
    }
    let mut bf2 = bloom::BloomFilter::with_size(bm_size * size_of::<&usize>() ,hash);
    let mut bf = HashSet::new();
    for j in 0..i{
        pre_process_both(config, &mut bf2,&mut bf);
    } 
    let g = read_sparsify_graph().unwrap();
    let v_size = read_graph().unwrap().count_all_vertices(None);
    let v_l = g.get_all_vertices(None).map(|f| f.get_id()).collect::<Vec<usize>>();
    let mut rng = thread_rng();
    let mut fp_count = 0;
    for i in 0..10000 {
        let v1 = v_l[rng.gen_range(0..v_l.len())];
        let v2 = v_l[rng.gen_range(0..v_l.len())];
        let e_id = v1*v_size+v2;
        if !bf.contains(&e_id) && bf2.contains(&e_id) {
            fp_count += 1;
        }
    }
    let inner_term = -1.0 * (hash as f64 / bm_size as f64*(e_size as f64 * sparse_rate) as f64);
    let fp_rate = (1.0-inner_term.exp()).powi(hash as i32);
    // println!("fp count {} {}", fp_count as f64 / 10000.0, fp_rate);
    fp_count as f64 / 10000.0
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args();
    let pattern_meta = read_pattern_meta()?;
    let patterns = match config.mode.as_str() {
        "single" => vec![read_pattern()?],
        "multiple" => read_patterns()?,
        _ => unreachable!(),
    };
    let e_size = read_graph()?.count_all_edges(None);
    let e_byte = e_size* size_of::<&usize>();
    if config.intersect_mode == "bf" {

        let sparse_rate;
        if config.bound_type == "mem" {
            sparse_rate = (config.global_size as f64/e_byte as f64 * (1.0-config.bm_rate) as f64).min(1.0);
        }
        else if config.bound_type == "rate" {
            sparse_rate = config.sample_rate;
        }
        else{
            sparse_rate=0.0;
        }
        let bm_size;
        if config.bound_type== "rate" {
            bm_size = config.global_size;
        }
        else {
            bm_size = (config.global_size as f64 * config.bm_rate) as usize;
        }
        let hash;
        if config.auto_hash {

            let optimal_hash = bm_size as f64/ (e_size as f64 * sparse_rate) * 2.0_f64.ln() as f64;
            hash = optimal_hash.ceil() as u32;
        }
        else {
            hash = config.hash;
        }
        // p = (1 - e(-(k * n/m)))^k
        // let mut inner_term;
        // let mut fp_rate;
        let mut bf = bloom::BloomFilter::with_size(bm_size * size_of::<&usize>() ,hash);
        for i in 1..config.iterative_run+1 {
            // inner_term = -1.0 * (hash as f64 / bm_size as f64*(e_size as f64 * (1f64 -(1f64 - sparse_rate).powi(i))) as f64);
            // fp_rate = (1.0-inner_term.exp()).powi(hash as i32);
            let fp_rate = test_fp_rate(&config, i);
            if !config.ground_truth {
                print!("bloom_filter {:?} {:?} {:?} {:?}", config.global_size/1024, fp_rate, i, config.bm_rate);
                print!(" {:?}",sparse_rate);
            }
            let sparsify_rate = pre_process_bf(&config, & mut bf);
            let sparse_graph =read_sparsify_graph().unwrap();
            let v_size = read_graph()?.count_all_vertices(None);
            for pattern in patterns.clone() {
        
                // println!("start executing bm...");
                let mut match_num = dfs_match_bf(&pattern, &sparse_graph, &bf, v_size, fp_rate, !config.ground_truth) as f64;
                // if !config.ground_truth {
                //     print!(" {:?} ",match_num);
                // }

                let mut edge_table = HashMap::new();
                let plan_info = gen_plan(&pattern, &mut edge_table);

                for (src_id,parents) in plan_info {
                    if parents.len()==0 {
                        continue;
                    }
                    let l1=pattern.get_vertex(src_id as usize).unwrap().get_label();
                    let l3 = pattern.get_vertex(parents[0]).unwrap().get_label();
                    let l2 = edge_table[&(src_id as u32, parents[0] as u32)];
                    match_num /= sparsify_rate[&(l1 as u8, l2 as u8, l3 as u8)];
                    if parents.len()>1 {
                        for j in 1..parents.len() {
                            let l3 = pattern.get_vertex(parents[j]).unwrap().get_label();
                            let l2 = edge_table[&(src_id as u32, parents[j] as u32)];
                            let sample_rate_global = 1f64-(1f64-sparsify_rate[&(l1 as u8, l2 as u8, l3 as u8)]).powi(i);

                            match_num /= sample_rate_global;
                        }
                    }
                }
                if !config.ground_truth{
                    println!(" {:?}",(match_num as f64 / config.baseline as f64 -1.0).abs().min((config.baseline as f64 / (match_num + 1.0) as f64 -1.0).abs()));
                    // println!("{:?}",match_num);
                }
                else {
                    println!(" {:?}",(match_num as f64 / config.baseline as f64));
                }
            }
        }
    }
    else if config.intersect_mode == "hs" {
        let mut sparse_rate = config.global_size as f64/e_byte as f64 * (1.0-0.5);
        if config.bound_type == "mem" {
            sparse_rate = sparse_rate.min(1.0);
        }
        else if config.bound_type == "rate" {
            sparse_rate = config.sample_rate;
        }
        let mut bf = HashSet::new();
        for i in 1..config.iterative_run+1 {
            if !config.ground_truth {
                print!("hashset {:?} - {:?} -", config.global_size/1024,i);
                print!(" {:?}",sparse_rate);
            }
            let sparsify_rate = pre_process_hs(&config, & mut bf);
            let sparse_graph =read_sparsify_graph().unwrap();
            let v_size = read_graph()?.count_all_vertices(None);
            for pattern in patterns.clone() {
    
                // println!("start executing hash...");
                let mut match_num = dfs_match_hs(&pattern, &sparse_graph, &bf, v_size, !config.ground_truth) as f64;
                // if !config.ground_truth {
                //     println!("match {:?}",match_num);
                // }
                let mut edge_table = HashMap::new();
                let plan_info = gen_plan(&pattern, &mut edge_table);

                for (src_id,parents) in plan_info {
                    if parents.len()==0 {
                        continue;
                    }
                    let l1=pattern.get_vertex(src_id as usize).unwrap().get_label();
                    let l3 = pattern.get_vertex(parents[0]).unwrap().get_label();
                    let l2 = edge_table[&(src_id as u32, parents[0] as u32)];
                    match_num /= sparsify_rate[&(l1 as u8, l2 as u8, l3 as u8)];
                    if parents.len()>1 {
                        for j in 1..parents.len() {
                            let l3 = pattern.get_vertex(parents[j]).unwrap().get_label();
                            let l2 = edge_table[&(src_id as u32, parents[j] as u32)];
                            let sample_rate_global = 1f64-(1f64-sparsify_rate[&(l1 as u8, l2 as u8, l3 as u8)]).powi(i);

                            match_num /= sample_rate_global;
                        }
                    }
                }
                if !config.ground_truth{
                    println!(" {:?}",(match_num as f64 / config.baseline as f64 -1.0).abs().min((config.baseline as f64 / (match_num + 1.0) as f64 -1.0).abs()));
                }
                else {
                    println!(" {:?}",(match_num as f64 / config.baseline as f64));
                }
            }
        }
    }
    else if config.intersect_mode == "ih" {
        let mut sparse_rate = config.global_size as f64/e_byte as f64;
        if config.bound_type == "mem" {
            sparse_rate = sparse_rate.min(1.0);
        }
        for i in 1..config.iterative_run+1 {
            if !config.ground_truth {
                print!("intersect_by_hash {:?} - - -", config.global_size/1024);
                print!(" {:?}",sparse_rate);
            }
            let sparsify_rate = pre_process_i(&config);
            let sparse_graph =read_sparsify_graph().unwrap();
            for pattern in patterns.clone() {
    
                // println!("start executing hash...");
                let mut match_num = dfs_match_ih(&pattern, &sparse_graph, !config.ground_truth) as f64;
                // println!("match num {:?}",match_num);
                let mut edge_table = HashMap::new();

                let plan_info = gen_plan(&pattern, &mut edge_table);
                // println!("edge_table {:?}", edge_table);
                for (src_id,parents) in plan_info {
                    if parents.len()==0 {
                        continue;
                    }
                    let l1=pattern.get_vertex(src_id as usize).unwrap().get_label();
                    let l3 = pattern.get_vertex(parents[0]).unwrap().get_label();
                    let l2 = edge_table[&(src_id as u32, parents[0] as u32)];
                    match_num /= sparsify_rate[&(l1 as u8, l2 as u8, l3 as u8)];
                    if parents.len()>1 {
                        for j in 1..parents.len() {
                            let l3 = pattern.get_vertex(parents[j]).unwrap().get_label();
                            let l2 = edge_table[&(src_id as u32, parents[j] as u32)];
                            let sample_rate_global = 1f64-(1f64-sparsify_rate[&(l1 as u8, l2 as u8, l3 as u8)]).powi(i);

                            match_num /= sample_rate_global;
                        }
                    }
                }
                if !config.ground_truth{
                    println!(" {:?}",(match_num as f64 / config.baseline as f64 -1.0).abs().min((config.baseline as f64 / (match_num + 1.0) as f64 -1.0).abs()));
                }
                else {
                    println!(" {:?}",(match_num as f64 / config.baseline as f64));
                }
            }
        }
    }
    else if config.intersect_mode == "ig" {
        for i in 1..config.iterative_run+1 {
            let mut sparse_rate = config.global_size as f64/e_byte as f64;
            if config.bound_type == "mem" {
                sparse_rate = sparse_rate.min(1.0);
            }
            if !config.ground_truth {
                print!("intersect_by_gallop {:?} - - -", config.global_size/1024);
                print!(" {:?}",sparse_rate);
            }
            let sparsify_rate = pre_process_i(&config);
            let sparse_graph =read_sparsify_graph().unwrap();
            for pattern in patterns.clone() {
        
                // println!("start executing hash...");
                let mut match_num = dfs_match_ig(&pattern, &sparse_graph, !config.ground_truth) as f64;
                // println!("match num {:?}",match_num);
                let mut edge_table = HashMap::new();

                let plan_info = gen_plan(&pattern, &mut edge_table);
                // println!("edge_table {:?}", edge_table);
                for (src_id,parents) in plan_info {
                    if parents.len()==0 {
                        continue;
                    }
                    let l1=pattern.get_vertex(src_id as usize).unwrap().get_label();
                    let l3 = pattern.get_vertex(parents[0]).unwrap().get_label();
                    let l2 = edge_table[&(src_id as u32, parents[0] as u32)];
                    match_num /= sparsify_rate[&(l1 as u8, l2 as u8, l3 as u8)];
                    if parents.len()>1 {
                        for j in 1..parents.len() {
                            let l3 = pattern.get_vertex(parents[j]).unwrap().get_label();
                            let l2 = edge_table[&(src_id as u32, parents[j] as u32)];
                            let sample_rate_global = 1f64-(1f64-sparsify_rate[&(l1 as u8, l2 as u8, l3 as u8)]).powi(i);

                            match_num /= sample_rate_global;
                        }
                    }
                }
                if !config.ground_truth{
                    println!(" {:?}",(match_num as f64 / config.baseline as f64 -1.0).abs().min((config.baseline as f64 / (match_num + 1.0) as f64 -1.0).abs()));
                }
                else {
                    println!(" {:?}",(match_num as f64 / config.baseline as f64));
                }
                
            }
        }
    }
    else {
        let a =test_fp_rate(&config,1);
    }
    Ok(())
}
