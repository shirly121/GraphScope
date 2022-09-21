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

mod common;

#[cfg(test)]
mod tests {
    use ir_core::catalogue::pattern::{PatternEdge, PatternVertex};
    use ir_core::catalogue::PatternDirection;

    use crate::common::pattern_cases::*;

    /// Test whether the structure of pattern_case1 is the same as our previous description
    #[test]
    fn test_pattern_case1_structure() {
        let pattern_case1 = build_pattern_case1();
        let edges_num = pattern_case1.get_edges_num();
        assert_eq!(edges_num, 2);
        let vertices_num = pattern_case1.get_vertices_num();
        assert_eq!(vertices_num, 2);
        let edges_with_label_0: Vec<&PatternEdge> = pattern_case1.edges_iter_by_label(0).collect();
        assert_eq!(edges_with_label_0.len(), 2);
        let vertices_with_label_0: Vec<&PatternVertex> = pattern_case1
            .vertices_iter_by_label(0)
            .collect();
        assert_eq!(vertices_with_label_0.len(), 2);
        let edge_0 = pattern_case1.get_edge(0).unwrap();
        assert_eq!(edge_0.get_id(), 0);
        assert_eq!(edge_0.get_label(), 0);
        assert_eq!(edge_0.get_start_vertex().get_id(), 0);
        assert_eq!(edge_0.get_end_vertex().get_id(), 1);
        assert_eq!(edge_0.get_start_vertex().get_label(), 0);
        assert_eq!(edge_0.get_end_vertex().get_label(), 0);
        let edge_1 = pattern_case1.get_edge(1).unwrap();
        assert_eq!(edge_1.get_id(), 1);
        assert_eq!(edge_1.get_label(), 0);
        assert_eq!(edge_1.get_start_vertex().get_id(), 1);
        assert_eq!(edge_1.get_end_vertex().get_id(), 0);
        assert_eq!(edge_1.get_start_vertex().get_label(), 0);
        assert_eq!(edge_1.get_end_vertex().get_label(), 0);
        let vertex_0 = pattern_case1.get_vertex(0).unwrap();
        assert_eq!(vertex_0.get_id(), 0);
        assert_eq!(vertex_0.get_label(), 0);
        assert_eq!(pattern_case1.get_vertex_degree(0), 2);
        let mut vertex_0_adjacent_edges_iter = pattern_case1.adjacencies_iter(0);
        let vertex_0_adjacency_0 = vertex_0_adjacent_edges_iter.next().unwrap();
        assert_eq!(vertex_0_adjacency_0.get_edge_id(), 0);
        assert_eq!(vertex_0_adjacency_0.get_adj_vertex().get_id(), 1);
        assert_eq!(vertex_0_adjacency_0.get_direction(), PatternDirection::Out);
        let vertex_0_adjacency_1 = vertex_0_adjacent_edges_iter.next().unwrap();
        assert_eq!(vertex_0_adjacency_1.get_edge_id(), 1);
        assert_eq!(vertex_0_adjacency_1.get_adj_vertex().get_id(), 1);
        assert_eq!(vertex_0_adjacency_1.get_direction(), PatternDirection::In);
        let vertex_1 = pattern_case1.get_vertex(1).unwrap();
        assert_eq!(vertex_1.get_id(), 1);
        assert_eq!(vertex_1.get_label(), 0);
        assert_eq!(pattern_case1.get_vertex_degree(1), 2);
        let mut vertex_1_adjacent_edges_iter = pattern_case1.adjacencies_iter(1);
        let vertex_1_adjacency_0 = vertex_1_adjacent_edges_iter.next().unwrap();
        assert_eq!(vertex_1_adjacency_0.get_edge_id(), 1);
        assert_eq!(vertex_1_adjacency_0.get_adj_vertex().get_id(), 0);
        assert_eq!(vertex_1_adjacency_0.get_direction(), PatternDirection::Out);
        let vertex_1_adjacency_1 = vertex_1_adjacent_edges_iter.next().unwrap();
        assert_eq!(vertex_1_adjacency_1.get_edge_id(), 0);
        assert_eq!(vertex_1_adjacency_1.get_adj_vertex().get_id(), 0);
        assert_eq!(vertex_1_adjacency_1.get_direction(), PatternDirection::In);
    }
}
