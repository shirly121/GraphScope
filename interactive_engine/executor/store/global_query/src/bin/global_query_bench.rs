use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use env_logger;

use global_query::{GlobalGraph, GlobalGraphQuery, PartitionVertexIds, GraphPartitionManager, PartitionLabeledVertexIds};
use groot_store::api::{Vertex, LabelId, VertexId, PartitionId};
use groot_store::db::api::GraphConfigBuilder;
use groot_store::db::graph::store::GraphStore;
use groot_store::db::util::time;

use log::info;

fn build_partition_vertex_id(
    id: VertexId, store: Arc<GlobalGraph>,
) -> Vec<PartitionVertexIds> {
    let partition_id = store.get_partition_id(id as VertexId) as PartitionId;
    vec![(partition_id, vec![id as VertexId])]
}

fn build_partition_label_vertex_ids(
    vid: VertexId, store: Arc<GlobalGraph>,
) -> Vec<PartitionLabeledVertexIds> {
    let mut partition_label_vid_map = HashMap::new();
  
    let partition_id = store.get_partition_id(vid) as PartitionId;
    let label_vid_list = partition_label_vid_map
        .entry(partition_id)
        .or_insert(HashMap::new());
    label_vid_list
        .entry(None)
        .or_insert(vec![])
        .push(vid);

    partition_label_vid_map
        .into_iter()
        .map(|(pid, label_vid_map)| (pid, label_vid_map.into_iter().collect()))
        .collect()
}
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
    let store = GraphStore::open(&config, &data_path).unwrap();
    let mut global_graph = GlobalGraph::empty(1);
    global_graph.add_partition(0, Arc::new(store));
    let global_graph_arc = Arc::new(global_graph);

    println!("store opened.");
    // std::thread::sleep(Duration::from_secs(30));
    let timer = time::Timer::new();
    let mut handles = Vec::new();
    let total_count = Arc::new(AtomicU64::new(0));
    let si: i64 = i64::MAX - 1;

    for i in 0..thread_count.parse().unwrap() {
        let task_id = i;
        let counter = total_count.clone();
        let store = global_graph_arc.clone();
        println!("task {} starting", task_id);
        let handle = std::thread::spawn(move || {
            for _times in 0..loop_count {
                let labels: &Vec<LabelId> = &vec![];
                let condition = None;
                let dedup_prop_ids = None;
                let output_prop_ids = None;
                let limit = usize::MAX;
                let partition_ids = &store.get_process_partition_list();
                let edge_labels: &Vec<LabelId> = &vec![];
                // 1. graph query related apis
                // scan vertex/edge
                let res = store.get_all_vertices(si, labels, condition, dedup_prop_ids, output_prop_ids, limit, partition_ids);
                for vertex in res {
                    info!("scan vertex {:?}", vertex);
                    let vertex_id = vertex.get_id();
                    let src_ids = build_partition_vertex_id(vertex_id, store.clone());
                    info!("start explore neighbors {:?}", src_ids);
                    // explore neighbors
                    let res = store.get_out_edges(si, src_ids.clone(), edge_labels, condition, dedup_prop_ids, output_prop_ids, limit);
                    for (_src_id, neighbors) in res {
                        for neighbor in neighbors {
                            info!("get out edge {:?}", neighbor); 
                        }
                    }
                    let res = store.get_in_edges(si, src_ids, edge_labels, condition, dedup_prop_ids, output_prop_ids, limit);
                    for (_src_id, neighbors) in res {
                        for neighbor in neighbors {
                            info!("get in edge {:?}", neighbor); 
                        }
                    }
                    // get vertex/edge
                    let ids = build_partition_label_vertex_ids(vertex_id, store.clone());
                    info!("start get vertex {:?}", ids);
                    let mut res = store.get_vertex_properties(si, ids, output_prop_ids);
                    info!("get vertex {:?}", res.next());   
                }
                let res = store.get_all_edges(si, labels, condition, dedup_prop_ids, output_prop_ids, limit, partition_ids);
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
    println!("{}\t{}", "time(sec)", "query_num");
    println!("{:.0}\t{:.2}", total_time,  total_query);
}
