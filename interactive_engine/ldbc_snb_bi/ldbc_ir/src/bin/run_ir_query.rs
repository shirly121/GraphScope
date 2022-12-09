use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::time::Instant;

use graph_proxy::{create_exp_store, SimplePartition};
use graph_store::prelude::*;
use itertools::__std_iter::Iterator;
use mcsr::graph_db::GlobalCsrTrait;
use pegasus::result::{ResultSink, ResultStream};
use pegasus::{run_opt, Configuration, JobConf, ServerConf};
use pegasus_server::job::JobAssembly;
use pegasus_server::job::JobDesc;
use runtime::IRJobAssembly;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "w", long = "workers", default_value = "2")]
    workers: u32,
    #[structopt(short = "q", long = "query")]
    query_path: String,
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
}

fn main() {
    println!("Start run ir queries");

    let config: Config = Config::from_args();

    pegasus_common::logs::init_log();

    create_exp_store();

    let server_conf = if let Some(ref servers) = config.servers {
        let servers = std::fs::read_to_string(servers).unwrap();
        Configuration::parse(&servers).unwrap()
    } else {
        Configuration::singleton()
    };
    let server_num = server_conf.servers_size();
    pegasus::startup(server_conf).ok();
    pegasus::wait_servers_ready(&ServerConf::All);

    let query_path = config.query_path;
    let mut queries_binary_paths = vec![];
    let file = File::open(query_path).unwrap();
    let lines = io::BufReader::new(file).lines();
    for line in lines {
        queries_binary_paths.push(line.unwrap());
    }
    let query_start = Instant::now();
    let mut index = 0i32;
    for query_binary_path in queries_binary_paths {
        let job_name = "job_ir_plan_".to_owned() + &index.to_string();
        let mut conf = JobConf::new(job_name);
        conf.set_workers(config.workers);
        conf.reset_servers(ServerConf::All);

        let paths = query_binary_path
            .trim()
            .split("|")
            .collect::<Vec<&str>>();
        let source_file_name = paths[0];
        let plan_file_name = paths[1];
        let resource_file_name = paths[2];

        let source = std::fs::read(source_file_name).expect("read source failed");
        let plan = std::fs::read(plan_file_name).expect("read plan failed");
        let resource = std::fs::read(resource_file_name).expect("read resource failed");

        let job = JobDesc { input: source, plan, resource };

        let partitioner = SimplePartition { num_servers: server_num };
        let assemble = IRJobAssembly::new(partitioner);
        let (tx, rx) = crossbeam_channel::unbounded();
        let sink = ResultSink::new(tx);
        let cancel_hook = sink.get_cancel_hook().clone();
        let mut results = ResultStream::new(conf.job_id, cancel_hook, rx);
        run_opt(conf, sink, move |worker| assemble.assemble(&job, worker)).expect("Submit job failed");
        while let Some(res) = results.next() {
            println!("Query result is  {:?}", res);
        }
    }
}
