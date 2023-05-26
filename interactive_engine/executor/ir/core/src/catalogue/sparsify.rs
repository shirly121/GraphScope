//
//! Copyright 2020 Alibaba Group Holding Limited.
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
//!

use bloom::BloomFilter;
use graph_store::prelude::{
    DefaultId, GlobalStoreTrait, GlobalStoreUpdate, GraphDBConfig, InternalId, ItemType, LargeGraphDB,
    MutableGraphDB, Row,
};
use rand::rngs::StdRng;
use std::time::{SystemTime};
use rand::{Rng, SeedableRng, thread_rng};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::fs::{self, File};
use std::{collections::HashMap, path::Path};

// pub fn generate_sparsify_rate(rate: f64, edge_distribution: HashMap<(u8,u8,u8), f64>) -> HashMap<(u8,u8,u8),f64> {
//     let mut sparsify_rate = HashMap::new();
//     let function = NumericalDifferentiation::new(Func(|x: &[f64;2]| {
//         (1.0 - x[0]).powi(2) + 100.0*(x[1] - x[0].powi(2)).powi(2)
//     }));
//     sparsify_rate
// }

pub fn dump_edge_info(edges: HashMap<(u8, u8, u8), f64>, path: &str) {
    let mut string_table = HashMap::new();
    for (key, value) in edges {
        let new_key = key.0.to_string() + "_" + &key.1.to_string() + "_" + &key.2.to_string();
        string_table.insert(new_key, value);
    }
    let data = serde_json::to_value(&string_table).unwrap();
    serde_json::to_writer(&File::create(path).unwrap(), &data).unwrap();
}

pub fn read_sparsify_config(path: &str) -> HashMap<(u8, u8, u8), f64> {
    let config = fs::read_to_string(path).unwrap();
    let parsed: Value = serde_json::from_str(&config).unwrap();
    let obj: Map<String, Value> = parsed.as_object().unwrap().clone();
    let mut sparsify_rate = HashMap::new();
    for (key, value) in obj {
        let labels: Vec<&str> = key.split('_').collect();
        let label1 = labels[0].parse::<u8>().unwrap();
        let label2 = labels[1].parse::<u8>().unwrap();
        let label3 = labels[2].parse::<u8>().unwrap();
        sparsify_rate.insert((label1, label2, label3), value.as_f64().unwrap());
    }
    sparsify_rate
}

pub fn get_edge_distribution(src_graph: &LargeGraphDB) -> HashMap<(u8, u8, u8), f64> {
    let mut edge_distribution = HashMap::new();
    for j in src_graph.get_all_edges(None) {
        let src_label = src_graph
            .get_vertex(j.get_src_id())
            .unwrap()
            .get_label();
        let dst_label = src_graph
            .get_vertex(j.get_dst_id())
            .unwrap()
            .get_label();
        let edge_label = j.get_label();
        // As current Pattern only use hyper label, here only use hyper label [0]
        let src_filter_label = src_label[0];
        let dst_filter_label = dst_label[0];
        // if src_label[1] == INVALID_LABEL_ID {
        //     src_filter_label = src_label[0];
        // }
        // if dst_label[1] == INVALID_LABEL_ID {
        //     dst_filter_label = dst_label[0];
        // }
        let relation_key = (src_filter_label, edge_label, dst_filter_label);
        *edge_distribution
            .entry(relation_key)
            .or_insert(0.0) += 1.0;
            }
    edge_distribution
}

pub fn create_sparsified_graph<P: AsRef<Path>>(
    src_graph: &LargeGraphDB, sparsify_rate: HashMap<(u8, u8, u8), f64>, path: P,
) {
    let mut mut_graph: MutableGraphDB<DefaultId, InternalId> =
        GraphDBConfig::default().root_dir(path).new();
    // random edge
    let all_vertex: Vec<usize> = src_graph
        .get_all_vertices(None)
        .map(|v| v.get_id())
        .filter(|id| src_graph.get_both_edges(*id, None).count() != 0)
        .collect();
    for i in all_vertex {
        let label = src_graph.get_vertex(i).unwrap().get_label();
        mut_graph.add_vertex(i, label);
    }
    let mut rng = thread_rng();
    let times = rng.gen_range(20..100);
    for i in 0..times {
        rng.gen_bool(0.5);
    }
    for j in src_graph.get_all_edges(None) {
        let src_label = src_graph
            .get_vertex(j.get_src_id())
            .unwrap()
            .get_label();

        let dst_label = src_graph
            .get_vertex(j.get_dst_id())
            .unwrap()
            .get_label();
        let edge_label = j.get_label();
        let src_filter_label = src_label[0];
        let dst_filter_label = dst_label[0];
        // if src_label[1] == INVALID_LABEL_ID {
        //     src_filter_label = src_label[0];
        // }
        // if dst_label[1] == INVALID_LABEL_ID {
        //     dst_filter_label = dst_label[0];
        // }
        let relation_key = (src_filter_label, edge_label, dst_filter_label);
        let rate = sparsify_rate[&relation_key];
        if rng.gen_bool(rate) {
            mut_graph.add_edge(j.get_src_id(), j.get_dst_id(), j.get_label());
        }
    }
    // end random edge
    mut_graph.export().unwrap();
}

pub fn create_sparsified_bloom_filter<P: AsRef<Path>>(
    src_graph: &LargeGraphDB, sparsify_rate: &HashMap<(u8, u8, u8), f64>, path: &P, filter:& mut BloomFilter
) {
    let mut mut_graph: MutableGraphDB<DefaultId, InternalId> =
        GraphDBConfig::default().root_dir(path).new();
    // random edge
    let v_size = src_graph.count_all_vertices(None);
    let mut v_set = HashSet::new();
    let mut e_set = HashSet::new();
    let mut rng = thread_rng();
    
    let times = rng.gen_range(20..100);
    for i in 0..times {
        rng.gen_bool(0.5);
    }
    for j in src_graph.get_all_edges(None) {
        if e_set.contains(&(j.get_src_id(),j.get_dst_id())) || e_set.contains(&(j.get_dst_id(),j.get_src_id())) {
            continue;
        }
        e_set.insert((j.get_src_id(),j.get_dst_id()));
        e_set.insert((j.get_dst_id(),j.get_src_id()));
        let src_label = src_graph
            .get_vertex(j.get_src_id())
            .unwrap()
            .get_label();

        let dst_label = src_graph
            .get_vertex(j.get_dst_id())
            .unwrap()
            .get_label();
        let edge_label = j.get_label();
        let src_filter_label = src_label[0];
        let dst_filter_label = dst_label[0];
        // if src_label[1] == INVALID_LABEL_ID {
        //     src_filter_label = src_label[0];
        // }
        // if dst_label[1] == INVALID_LABEL_ID {
        //     dst_filter_label = dst_label[0];
        // }
        let relation_key = (src_filter_label, edge_label, dst_filter_label);
        let rate = sparsify_rate[&relation_key];
        if rng.gen_bool(rate) {
            let hash_edge_id = j.get_src_id()as u64*v_size as u64 +j.get_dst_id() as u64;
            let hash_edge_id_inv = j.get_dst_id()as u64*v_size as u64 +j.get_src_id() as u64;
            if !filter.contains(&hash_edge_id){
                filter.insert(&hash_edge_id);
            }
            if !filter.contains(&hash_edge_id_inv){
                filter.insert(&hash_edge_id_inv);
            }
            if !v_set.contains(&j.get_src_id()) {
                v_set.insert(j.get_src_id());
                mut_graph.add_vertex(j.get_src_id(), src_graph.get_vertex(j.get_src_id()).unwrap().get_label());
            }
            if !v_set.contains(&j.get_dst_id()) {
                v_set.insert(j.get_dst_id());
                mut_graph.add_vertex(j.get_dst_id(), src_graph.get_vertex(j.get_dst_id()).unwrap().get_label());
            }
            mut_graph.add_edge(j.get_src_id(), j.get_dst_id(), j.get_label());
            mut_graph.add_edge(j.get_dst_id(), j.get_src_id(), j.get_label());
        }
    }
    // end random edge
    mut_graph.export().unwrap();
}

pub fn create_sparsified_hash_set<P: AsRef<Path>>(
    src_graph: &LargeGraphDB, sparsify_rate: &HashMap<(u8, u8, u8), f64>, path: &P, filter:& mut HashSet<usize>
) {
    let mut mut_graph: MutableGraphDB<DefaultId, InternalId> =
        GraphDBConfig::default().root_dir(path).new();
    // random edge
    let v_size = src_graph.count_all_vertices(None);
    let mut v_set = HashSet::new();
    let mut e_set = HashSet::new();
    
    let mut rng = thread_rng();
    
    let times = rng.gen_range(20..100);
    for i in 0..times {
        rng.gen_bool(0.5);
    }
    for j in src_graph.get_all_edges(None) {
        if e_set.contains(&(j.get_src_id(),j.get_dst_id())) || e_set.contains(&(j.get_dst_id(),j.get_src_id())) {
            continue;
        }

        e_set.insert((j.get_src_id(),j.get_dst_id()));
        e_set.insert((j.get_dst_id(),j.get_src_id()));
        let src_label = src_graph
            .get_vertex(j.get_src_id())
            .unwrap()
            .get_label();

        let dst_label = src_graph
            .get_vertex(j.get_dst_id())
            .unwrap()
            .get_label();
        let edge_label = j.get_label();
        let src_filter_label = src_label[0];
        let dst_filter_label = dst_label[0];
        // if src_label[1] == INVALID_LABEL_ID {
        //     src_filter_label = src_label[0];
        // }
        // if dst_label[1] == INVALID_LABEL_ID {
        //     dst_filter_label = dst_label[0];
        // }
        let relation_key = (src_filter_label, edge_label, dst_filter_label);
        let rate = sparsify_rate[&relation_key];
        if rng.gen_bool(rate) {
            let hash_edge_id = j.get_src_id()as u64*v_size as u64 +j.get_dst_id() as u64;
            let hash_edge_id_inv = j.get_dst_id()as u64*v_size as u64 +j.get_src_id() as u64;
            if !filter.contains(&(hash_edge_id as usize)){
                filter.insert(hash_edge_id as usize);
            }
            if !filter.contains(&(hash_edge_id_inv as usize)){
                filter.insert(hash_edge_id_inv as usize);
            }
            if !v_set.contains(&j.get_src_id()) {
                v_set.insert(j.get_src_id());
                mut_graph.add_vertex(j.get_src_id(), src_graph.get_vertex(j.get_src_id()).unwrap().get_label());
            }
            if !v_set.contains(&j.get_dst_id()) {
                v_set.insert(j.get_dst_id());
                mut_graph.add_vertex(j.get_dst_id(), src_graph.get_vertex(j.get_dst_id()).unwrap().get_label());
            }
            mut_graph.add_edge(j.get_src_id(), j.get_dst_id(), j.get_label());
            mut_graph.add_edge(j.get_dst_id(), j.get_src_id(), j.get_label());
        }
    }
    // end random edge
    mut_graph.export().unwrap();
}

pub fn create_sparsified_both_set<P: AsRef<Path>>(
    src_graph: &LargeGraphDB, sparsify_rate: &HashMap<(u8, u8, u8), f64>, path: &P, filter:& mut HashSet<usize>, filter2:& mut BloomFilter
) {
    let mut mut_graph: MutableGraphDB<DefaultId, InternalId> =
        GraphDBConfig::default().root_dir(path).new();
    // random edge
    let v_size = src_graph.count_all_vertices(None);
    let mut v_set = HashSet::new();
    let mut e_set = HashSet::new();
    
    let mut rng = thread_rng();
    
    let times = rng.gen_range(20..100);
    for i in 0..times {
        rng.gen_bool(0.5);
    }
    for j in src_graph.get_all_edges(None) {
        if e_set.contains(&(j.get_src_id(),j.get_dst_id())) || e_set.contains(&(j.get_dst_id(),j.get_src_id())) {
            continue;
        }

        e_set.insert((j.get_src_id(),j.get_dst_id()));
        e_set.insert((j.get_dst_id(),j.get_src_id()));
        let src_label = src_graph
            .get_vertex(j.get_src_id())
            .unwrap()
            .get_label();

        let dst_label = src_graph
            .get_vertex(j.get_dst_id())
            .unwrap()
            .get_label();
        let edge_label = j.get_label();
        let src_filter_label = src_label[0];
        let dst_filter_label = dst_label[0];
        // if src_label[1] == INVALID_LABEL_ID {
        //     src_filter_label = src_label[0];
        // }
        // if dst_label[1] == INVALID_LABEL_ID {
        //     dst_filter_label = dst_label[0];
        // }
        let relation_key = (src_filter_label, edge_label, dst_filter_label);
        let rate = sparsify_rate[&relation_key];
        if rng.gen_bool(rate) {
            let hash_edge_id = j.get_src_id()as u64*v_size as u64 +j.get_dst_id() as u64;
            let hash_edge_id_inv = j.get_dst_id()as u64*v_size as u64 +j.get_src_id() as u64;
            if !filter.contains(&(hash_edge_id as usize)){
                filter.insert(hash_edge_id as usize);
            }
            if !filter.contains(&(hash_edge_id_inv as usize)){
                filter.insert(hash_edge_id_inv as usize);
            }
            if !filter2.contains(&(hash_edge_id as usize)){
                filter2.insert(&(hash_edge_id as usize));
            }
            if !filter2.contains(&(hash_edge_id_inv as usize)){
                filter2.insert(&(hash_edge_id as usize));
            }
            if !v_set.contains(&j.get_src_id()) {
                v_set.insert(j.get_src_id());
                mut_graph.add_vertex(j.get_src_id(), src_graph.get_vertex(j.get_src_id()).unwrap().get_label());
            }
            if !v_set.contains(&j.get_dst_id()) {
                v_set.insert(j.get_dst_id());
                mut_graph.add_vertex(j.get_dst_id(), src_graph.get_vertex(j.get_dst_id()).unwrap().get_label());
            }
            mut_graph.add_edge(j.get_src_id(), j.get_dst_id(), j.get_label());
            mut_graph.add_edge(j.get_dst_id(), j.get_src_id(), j.get_label());
        }
    }
    // end random edge
    mut_graph.export().unwrap();
}


pub fn create_sparsified_intersect<P: AsRef<Path>>(
    src_graph: &LargeGraphDB, sparsify_rate: &HashMap<(u8, u8, u8), f64>, path: &P
) {
    let mut mut_graph: MutableGraphDB<DefaultId, InternalId> =
        GraphDBConfig::default().root_dir(path).new();
    // random edge
    let v_size = src_graph.count_all_vertices(None);
    let mut v_set = HashSet::new();
    let mut e_set = HashSet::new();
    
    let mut rng = thread_rng();
    let times = rng.gen_range(20..100);
    for i in 0..times {
        rng.gen_bool(0.5);
    }
    for j in src_graph.get_all_edges(None) {
        if e_set.contains(&(j.get_src_id(),j.get_dst_id())) || e_set.contains(&(j.get_dst_id(),j.get_src_id())) {
            continue;
        }
        e_set.insert((j.get_src_id(),j.get_dst_id()));
        e_set.insert((j.get_dst_id(),j.get_src_id()));
        let src_label = src_graph
            .get_vertex(j.get_src_id())
            .unwrap()
            .get_label();

        let dst_label = src_graph
            .get_vertex(j.get_dst_id())
            .unwrap()
            .get_label();
        let edge_label = j.get_label();
        let src_filter_label = src_label[0];
        let dst_filter_label = dst_label[0];
        // if src_label[1] == INVALID_LABEL_ID {
        //     src_filter_label = src_label[0];
        // }
        // if dst_label[1] == INVALID_LABEL_ID {
        //     dst_filter_label = dst_label[0];
        // }
        let relation_key = (src_filter_label, edge_label, dst_filter_label);
        let rate = sparsify_rate[&relation_key];

        if rng.gen_bool(rate) {

            if !v_set.contains(&j.get_src_id()) {
                v_set.insert(j.get_src_id());
                mut_graph.add_vertex(j.get_src_id(), src_graph.get_vertex(j.get_src_id()).unwrap().get_label());
            }
            if !v_set.contains(&j.get_dst_id()) {
                v_set.insert(j.get_dst_id());
                mut_graph.add_vertex(j.get_dst_id(), src_graph.get_vertex(j.get_dst_id()).unwrap().get_label());
            }
            mut_graph.add_edge(j.get_src_id(), j.get_dst_id(), j.get_label());
            mut_graph.add_edge(j.get_dst_id(), j.get_src_id(), j.get_label());
        }
    }
    // end random edge
    mut_graph.export().unwrap();
}


pub fn switch_undirected_graph<P: AsRef<Path>>(
    src_graph: LargeGraphDB, path: P
) {
    let mut mut_graph: MutableGraphDB<DefaultId, InternalId> =
        GraphDBConfig::default().root_dir(path).new();
    for v in src_graph.get_all_vertices(Some(&vec![1])) {
        mut_graph.add_vertex(v.get_id(), v.get_label());
    }
    for edge in src_graph.get_all_edges(Some(&vec![12])) {
        mut_graph.add_edge(edge.get_src_id(), edge.get_dst_id(), edge.get_label());
        mut_graph.add_edge(edge.get_dst_id(), edge.get_src_id(), edge.get_label());
    }
    mut_graph.export().unwrap();
}

pub fn aggregate_graphs(src_graphs: Vec<LargeGraphDB>, root_dir: &str) -> MutableGraphDB {
    let mut mut_graph: MutableGraphDB<DefaultId, InternalId> = GraphDBConfig::default()
        .root_dir(root_dir)
        .new();
    for src_graph in src_graphs {
        for vertex in src_graph.get_all_vertices(None) {
            if let Some(vertex_properties) = vertex.clone_all_properties() {
                let row = Row::from(
                    vertex_properties
                        .into_iter()
                        .map(|(_, obj)| obj)
                        .collect::<Vec<ItemType>>(),
                );
                mut_graph
                    .add_vertex_with_properties(vertex.get_id(), vertex.get_label(), row)
                    .unwrap();
            } else {
                mut_graph.add_vertex(vertex.get_id(), vertex.get_label());
            }
        }
        for edge in src_graph.get_all_edges(None) {
            if let Some(edge_properties) = edge.clone_all_properties() {
                let row = Row::from(
                    edge_properties
                        .into_iter()
                        .map(|(_, obj)| obj)
                        .collect::<Vec<ItemType>>(),
                );
                mut_graph
                    .add_edge_with_properties(edge.get_src_id(), edge.get_dst_id(), edge.get_label(), row)
                    .unwrap();
            } else {
                mut_graph.add_edge(edge.get_src_id(), edge.get_dst_id(), edge.get_label());
            }
        }
    }
    mut_graph
}