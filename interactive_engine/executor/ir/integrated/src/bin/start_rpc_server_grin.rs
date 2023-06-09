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

use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::path::PathBuf;
use std::sync::Arc;

use graph_proxy::apis::PegasusClusterInfo;
use graph_proxy::GrinPartition;
use graph_proxy::{create_grin_store, GrinGraphProxy};
use grin::grin_v6d::*;
use grin::{string_c2rust, string_rust2c};
use log::info;
use runtime::initialize_job_assembly;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "EchoServer", about = "example of rpc service")]
struct Config {
    #[structopt(long = "config", parse(from_os_str))]
    config_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("starting rpc server grin...");
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let (server_config, rpc_config) = pegasus_server::config::load_configs(config.config_dir).unwrap();
    println!("load config success {:?}", server_config);
    let rust_str_1 = "/home/graphscope/gie-grin/v6d/build/tmp.sock";
    let rust_str_2 = "136746074498397120";
    unsafe {
        let mut arr: [*const c_char; 2] = [string_rust2c(rust_str_1), string_rust2c(rust_str_2)];
        // let mut arr: [*const c_char; 2] = [c_str_ptr_1, c_str_ptr_2];
        let arr_mut_ptr: *mut *mut c_char = arr.as_mut_ptr() as *mut *mut c_char;
        let pg = grin_get_partitioned_graph_from_storage(2, arr_mut_ptr);

        let mut partition_server_index_mapping = HashMap::new();
        let fake_num_partitions = 4;
        for partition_id in 0..fake_num_partitions {
            partition_server_index_mapping.insert(partition_id, 0);
        }

        let cluster_info = Arc::new(PegasusClusterInfo::default());

        let partitioned_graph = Arc::new(GrinGraphProxy::new(pg).unwrap());

        let partitioner =
            GrinPartition::new(partitioned_graph.clone(), partition_server_index_mapping.clone());
        let grin_graph =
            create_grin_store(partitioned_graph.clone(), vec![0, 1, 2, 3], cluster_info.clone());
        let job_assembly = initialize_job_assembly(grin_graph, Arc::new(partitioner), cluster_info);

        info!("try to start rpc server;");

        pegasus_server::cluster::standalone::start(rpc_config, server_config, job_assembly).await?;

        Ok(())
    }
}
