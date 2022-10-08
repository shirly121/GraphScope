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

use std::env;

use log::info;
use runtime_integration::{InitializeJobAssembly, QueryExpGraph};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let id = get_id();
    let servers_size = get_servers_size();
    let server_config_string = generate_server_config_string(id, servers_size);
    let rpc_config_string = generate_rpc_config_string(id);
    let server_config = pegasus::Configuration::parse(&server_config_string)?;
    let rpc_config = pegasus_server::rpc::RPCServerConfig::parse(&rpc_config_string)?;

    let num_servers = server_config.servers_size();
    let query_exp_graph = QueryExpGraph::new(num_servers);
    let job_assembly = query_exp_graph.initialize_job_assembly();
    info!("try to start rpc server;");

    pegasus_server::cluster::k8s::start(rpc_config, server_config, job_assembly).await?;

    Ok(())
}

fn get_id() -> u64 {
    env::var("HOSTNAME")
        .expect("k8s cluster should set HOSTNAME env variable")
        .split("-")
        .last()
        .expect("invalid HOSTNAME format")
        .parse()
        .expect("server id should be a number")
}

fn get_servers_size() -> usize {
    env::var("SERVERSSIZE")
        .expect("k8s cluster should set SERVERSSIZE env variable")
        .parse()
        .expect("servers size should be a number")
}

fn generate_server_config_string(id: u64, servers_size: usize) -> String {
    let mut server_config = format!(
        "[network]\n\
        server_id = {}\n\
        servers_size = {}\n",
        id, servers_size
    );
    for i in 0..servers_size {
        server_config.push_str(&format!(
            "[[network.servers]]\n\
            hostname = 'gaia-ir-rpc-{}.gaia-ir-rpc-hs.default.svc.cluster.local'\n\
            port = 11234\n",
            i
        ))
    }
    server_config
}

fn generate_rpc_config_string(id: u64) -> String {
    format!(
        "rpc_host = 'gaia-ir-rpc-{}.gaia-ir-rpc-hs.default.svc.cluster.local'\n\
        rpc_port = 1234",
        id
    )
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_generate_server_config_string() {
        let id = 0;
        let servers_size = 3;
        let server_config_string = generate_server_config_string(id, servers_size);
        let server_config = pegasus::Configuration::parse(&server_config_string).unwrap();
        let network_config = server_config.network.unwrap();
        assert_eq!(network_config.server_id, id);
        assert_eq!(network_config.servers_size, servers_size);
    }

    #[test]
    fn test_generate_rpc_config_string() {
        let id = 0;
        let rpc_config_string = generate_rpc_config_string(id);
        let rpc_config = pegasus_server::rpc::RPCServerConfig::parse(&rpc_config_string).unwrap();
        let rpc_host = rpc_config.rpc_host.unwrap();
        let rpc_port = rpc_config.rpc_port.unwrap();
        assert_eq!(rpc_host, "gaia-ir-rpc-0.gaia-ir-rpc-hs.default.svc.cluster.local".to_string());
        assert_eq!(rpc_port, 1234)
    }
}
