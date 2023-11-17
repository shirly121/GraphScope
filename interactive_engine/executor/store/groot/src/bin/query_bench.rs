use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use env_logger;

use groot_store::api::Vertex;
use groot_store::db::api::multi_version_graph::MultiVersionGraph;
use groot_store::db::api::GraphConfigBuilder;
use groot_store::db::graph::store::GraphStore;
use groot_store::db::util::time;

use log::info;

fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    let thread_count = &args[1];
    let loop_count:i64 = args[2].parse().unwrap();
    // "./resource/groot_data_bin/data/0"
    let data_path = &args[3];
    // "./resource/groot_data_bin/meta"
    let meta_path = &args[4];
    println!("thread_count {}, query loop_count {}, data_path {:?}, meta_path {:?}", thread_count, loop_count, data_path, meta_path);

    let mut builder = GraphConfigBuilder::new();
    builder.set_storage_engine("rocksdb");
    builder.add_storage_option("file.meta.store.path", &meta_path);
    builder.add_storage_option("store.data.path", &data_path);

    let config = builder.build();
    let store = Arc::new(GraphStore::open(&config, &data_path).unwrap());
    println!("store opened.");
    let timer = time::Timer::new();
    let mut handles = Vec::new();
    let total_count = Arc::new(AtomicU64::new(0));
    let snapshot_id: i64 = i64::MAX - 1;

    for i in 0..thread_count.parse().unwrap() {
        let task_id = i;
        let counter = total_count.clone();
        let store = store.clone();
        println!("task {} starting", task_id);
        let handle = std::thread::spawn(move || {
            for _times in 0..loop_count {
                let label_id = None;
                let property_ids = None;
                let condition = None;
                // 1. graph query related apis
                // scan vertex/edge
                let res = store.scan_vertex(snapshot_id,  label_id, condition, property_ids).unwrap();
                for vertex in res {
                    let vertex = vertex.unwrap();
                    info!("scan vertex {:?}", vertex);
                    let vertex_id = vertex.get_id();
                    // explore neighbors
                    let res = store.get_out_edges(snapshot_id, vertex_id, label_id, condition, property_ids).unwrap();
                    for neighbor in res {
                        let neighbor = neighbor.unwrap();
                        info!("get out edge {:?}", neighbor); 
                    }
                
                    let res = store.get_in_edges(snapshot_id, vertex_id, label_id, condition, property_ids).unwrap();
                    for neighbor in res {
                        let neighbor = neighbor.unwrap();
                        info!("get in edge {:?}", neighbor); 
                    }
                    // get vertex/edge
                    let res = store.get_vertex(snapshot_id, vertex_id, label_id, property_ids).unwrap();
                    info!("get vertex {:?}", res);   
                }
                let  res = store.scan_edge(snapshot_id, label_id, condition, property_ids).unwrap();
                info!("scan edge count {}", res.count());
             
                // 2. graph schema related apis
                // todo
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }
  
    for handle in handles {
        handle.join().unwrap();
    }
    let total_query = total_count.load(Ordering::Relaxed);
    let total_time = timer.elasped_secs();
    println!("{}\t{}", "time(sec)", "speed(record/s)");
    println!("{:.0}\t{:.2}", total_time,  total_query);
    
    std::thread::sleep(Duration::from_secs(1000000000));
}
