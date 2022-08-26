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
use graph_store::prelude::{
    DefaultId, GlobalStoreTrait, GlobalStoreUpdate, GraphDBConfig, InternalId, ItemType, LargeGraphDB,
    MutableGraphDB, Row,
};
use rand::{thread_rng, Rng};

pub fn create_sparsified_graph(src_graph: LargeGraphDB, rate: i32) {
    let mut mut_graph: MutableGraphDB<DefaultId, InternalId> = GraphDBConfig::default()
        .root_dir("/Users/meloyang/opt/Graphs/ldbc_sparsification/new_graph")
        .new();
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
    for j in src_graph.get_all_edges(None) {
        let mut rng = thread_rng();
        let ran = rng.gen_range(0..100);
        if ran <= rate {
            mut_graph.add_edge(j.get_src_id(), j.get_dst_id(), j.get_label());
        }
    }
    // end random edge
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use graph_store::ldbc::GraphLoader;
//     #[test]
//     fn test_create_sparsified_graph() {
//         let data_dir = "/Users/meloyang/opt/Graphs/csr_ldbc_graph/scale_1/csv";
//         let root_dir = "/Users/meloyang/opt/Graphs/csr_ldbc_graph/scale_1/csv";
//         let schema_file = "/Users/meloyang/opt/Graphs/csr_ldbc_graph/schema.json";
//         let mut loader =
//             GraphLoader::<DefaultId, InternalId>::new(data_dir, root_dir, schema_file, 20, 0, 1);
//         loader.load().expect("Load ldbc data error!");
//         let src_graph = loader.into_graph();
//         create_sparsified_graph(src_graph, 30);
//     }
// }
