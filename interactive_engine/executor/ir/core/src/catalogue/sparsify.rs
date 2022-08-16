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
    DefaultId, GlobalStoreTrait, GlobalStoreUpdate, GraphDBConfig, InternalId, LargeGraphDB, MutableGraphDB,
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
