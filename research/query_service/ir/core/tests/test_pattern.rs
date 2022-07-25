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
    use std::convert::TryFrom;

    use ir_core::catalogue::codec::*;
    use ir_core::catalogue::pattern::{Pattern, PatternEdge, PatternVertex};
    use ir_core::catalogue::{PatternDirection, PatternId};
    use ir_core::plan::meta::TagId;

    use crate::common::pattern_cases::*;

    const TAG_A: TagId = 0;
    const TAG_B: TagId = 1;
    const TAG_C: TagId = 2;
    const TAG_D: TagId = 3;

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

    #[test]
    fn test_ldbc_pattern_from_pb_case1_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case1();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.get_vertices_num(), 3);
            assert_eq!(pattern.get_edges_num(), 3);
            // 3 Person vertices
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                3
            );
            // 3 knows edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(12)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                3
            );
            // check structure
            // build identical pattern for comparison
            let pattern_vertex1 = PatternVertex::new(0, 1);
            let pattern_vertex2 = PatternVertex::new(1, 1);
            let pattern_vertex3 = PatternVertex::new(2, 1);
            let pattern_edge1 = PatternEdge::new(0, 12, pattern_vertex1, pattern_vertex2);
            let pattern_edge2 = PatternEdge::new(1, 12, pattern_vertex1, pattern_vertex3);
            let pattern_edge3 = PatternEdge::new(2, 12, pattern_vertex2, pattern_vertex3);
            let pattern_for_comparison =
                Pattern::try_from(vec![pattern_edge1, pattern_edge2, pattern_edge3]).unwrap();
            // check whether the two pattern has the same code
            let encoder = Encoder::init(4, 4, 1, 2);
            let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
            let pattern_for_comparison_code: Vec<u8> = pattern_for_comparison.encode_to(&encoder);
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_A)
                    .unwrap()
                    .get_id(),
                0
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_B)
                    .unwrap()
                    .get_id(),
                1
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_C)
                    .unwrap()
                    .get_id(),
                2
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case2_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case2();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.get_vertices_num(), 3);
            assert_eq!(pattern.get_edges_num(), 3);
            // 2 Person vertices
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                2
            );
            // 1 University vertex
            assert_eq!(
                pattern
                    .vertices_iter_by_label(12)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                1
            );
            // 1 knows edge
            assert_eq!(
                pattern
                    .edges_iter_by_label(12)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                1
            );
            // 2 studyat edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(15)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                2
            );
            // check structure
            let pattern_vertex1 = PatternVertex::new(0, 12);
            let pattern_vertex2 = PatternVertex::new(1, 1);
            let pattern_vertex3 = PatternVertex::new(2, 1);
            let pattern_edge1 = PatternEdge::new(0, 15, pattern_vertex2, pattern_vertex1);
            let pattern_edge2 = PatternEdge::new(1, 15, pattern_vertex3, pattern_vertex1);
            let pattern_edge3 = PatternEdge::new(2, 12, pattern_vertex2, pattern_vertex3);
            let pattern_for_comparison =
                Pattern::try_from(vec![pattern_edge1, pattern_edge2, pattern_edge3]).unwrap();
            // check whether the two pattern has the same code
            let encoder = Encoder::init(4, 4, 1, 2);
            let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
            let pattern_for_comparison_code: Vec<u8> = pattern_for_comparison.encode_to(&encoder);
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_A)
                    .unwrap()
                    .get_id(),
                0
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_B)
                    .unwrap()
                    .get_id(),
                1
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_C)
                    .unwrap()
                    .get_id(),
                2
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case3_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case3();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.get_vertices_num(), 4);
            assert_eq!(pattern.get_edges_num(), 6);
            // 4 Person vertices
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                4
            );
            // 6 knows edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(12)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                6
            );
            // check structure
            // build identical pattern for comparison
            let pattern_vertex1 = PatternVertex::new(0, 1);
            let pattern_vertex2 = PatternVertex::new(1, 1);
            let pattern_vertex3 = PatternVertex::new(2, 1);
            let pattern_vertex4 = PatternVertex::new(3, 1);
            let pattern_edge1 = PatternEdge::new(0, 12, pattern_vertex1, pattern_vertex2);
            let pattern_edge2 = PatternEdge::new(1, 12, pattern_vertex1, pattern_vertex3);
            let pattern_edge3 = PatternEdge::new(2, 12, pattern_vertex2, pattern_vertex3);
            let pattern_edge4 = PatternEdge::new(3, 12, pattern_vertex1, pattern_vertex4);
            let pattern_edge5 = PatternEdge::new(4, 12, pattern_vertex2, pattern_vertex4);
            let pattern_edge6 = PatternEdge::new(5, 12, pattern_vertex3, pattern_vertex4);
            let pattern_for_comparison = Pattern::try_from(vec![
                pattern_edge1,
                pattern_edge2,
                pattern_edge3,
                pattern_edge4,
                pattern_edge5,
                pattern_edge6,
            ])
            .unwrap();
            // check whether the two pattern has the same code
            let encoder = Encoder::init(4, 4, 1, 3);
            let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
            let pattern_for_comparison_code: Vec<u8> = pattern_for_comparison.encode_to(&encoder);
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_A)
                    .unwrap()
                    .get_id(),
                0
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_B)
                    .unwrap()
                    .get_id(),
                1
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_C)
                    .unwrap()
                    .get_id(),
                2
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_D)
                    .unwrap()
                    .get_id(),
                3
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case4_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case4();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.get_vertices_num(), 4);
            assert_eq!(pattern.get_edges_num(), 4);
            // 2 Person vertices
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                2
            );
            // 1 City vertex
            assert_eq!(
                pattern
                    .vertices_iter_by_label(9)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                1
            );
            // 1 Comment vertex
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                2
            );
            // 1 has creator edge
            assert_eq!(
                pattern
                    .edges_iter_by_label(0)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                1
            );
            // 1 likes edge
            assert_eq!(
                pattern
                    .edges_iter_by_label(13)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                1
            );
            // 2 islocated edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(11)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                2
            );
            // check structure
            // build identical pattern for comparison
            let pattern_vertex1 = PatternVertex::new(0, 1);
            let pattern_vertex2 = PatternVertex::new(1, 9);
            let pattern_vertex3 = PatternVertex::new(2, 1);
            let pattern_vertex4 = PatternVertex::new(3, 2);
            let pattern_edge1 = PatternEdge::new(0, 11, pattern_vertex1, pattern_vertex2);
            let pattern_edge2 = PatternEdge::new(1, 11, pattern_vertex3, pattern_vertex2);
            let pattern_edge3 = PatternEdge::new(2, 13, pattern_vertex1, pattern_vertex4);
            let pattern_edge4 = PatternEdge::new(3, 0, pattern_vertex4, pattern_vertex3);
            let pattern_for_comparison =
                Pattern::try_from(vec![pattern_edge1, pattern_edge2, pattern_edge3, pattern_edge4])
                    .unwrap();
            // check whether the two pattern has the same code
            let encoder = Encoder::init(4, 4, 1, 2);
            let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
            let pattern_for_comparison_code: Vec<u8> = pattern_for_comparison.encode_to(&encoder);
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_A)
                    .unwrap()
                    .get_id(),
                0
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_B)
                    .unwrap()
                    .get_id(),
                2
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_C)
                    .unwrap()
                    .get_id(),
                1
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_D)
                    .unwrap()
                    .get_id(),
                3
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case5_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case5();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.get_vertices_num(), 6);
            assert_eq!(pattern.get_edges_num(), 6);
            // 6 Person vertices
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                6
            );
            // 6 knows edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(12)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                6
            );
            // check structure
            // build identical pattern for comparison
            let pattern_vertex1 = PatternVertex::new(0, 1);
            let pattern_vertex2 = PatternVertex::new(1, 1);
            let pattern_vertex3 = PatternVertex::new(2, 1);
            let pattern_vertex4 = PatternVertex::new(3, 1);
            let pattern_vertex5 = PatternVertex::new(4, 1);
            let pattern_vertex6 = PatternVertex::new(5, 1);
            let pattern_edge1 = PatternEdge::new(0, 12, pattern_vertex1, pattern_vertex2);
            let pattern_edge2 = PatternEdge::new(1, 12, pattern_vertex3, pattern_vertex2);
            let pattern_edge3 = PatternEdge::new(2, 12, pattern_vertex3, pattern_vertex4);
            let pattern_edge4 = PatternEdge::new(3, 12, pattern_vertex5, pattern_vertex4);
            let pattern_edge5 = PatternEdge::new(4, 12, pattern_vertex5, pattern_vertex6);
            let pattern_edge6 = PatternEdge::new(5, 12, pattern_vertex1, pattern_vertex6);
            let pattern_for_comparison = Pattern::try_from(vec![
                pattern_edge1,
                pattern_edge2,
                pattern_edge3,
                pattern_edge4,
                pattern_edge5,
                pattern_edge6,
            ])
            .unwrap();
            // check whether the two pattern has the same code
            let encoder = Encoder::init(4, 4, 1, 3);
            let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
            let pattern_for_comparison_code: Vec<u8> = pattern_for_comparison.encode_to(&encoder);
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_A)
                    .unwrap()
                    .get_id(),
                0
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_B)
                    .unwrap()
                    .get_id(),
                1
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_C)
                    .unwrap()
                    .get_id(),
                3
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn test_ldbc_pattern_from_pb_case6_structure() {
        let pattern_result = build_ldbc_pattern_from_pb_case6();
        if let Ok(pattern) = pattern_result {
            assert_eq!(pattern.get_vertices_num(), 6);
            assert_eq!(pattern.get_edges_num(), 6);
            // 4 Persons vertices
            assert_eq!(
                pattern
                    .vertices_iter_by_label(1)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                4
            );
            // 1 City vertex
            assert_eq!(
                pattern
                    .vertices_iter_by_label(9)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                1
            );
            // 1 Comment vertex
            assert_eq!(
                pattern
                    .vertices_iter_by_label(2)
                    .collect::<Vec<&PatternVertex>>()
                    .len(),
                1
            );
            // 1 has creator edge
            assert_eq!(
                pattern
                    .edges_iter_by_label(0)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                1
            );
            // 1 likes edge
            assert_eq!(
                pattern
                    .edges_iter_by_label(13)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                1
            );
            // 2 islocated edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(11)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                2
            );
            // 2 knows edges
            assert_eq!(
                pattern
                    .edges_iter_by_label(12)
                    .collect::<Vec<&PatternEdge>>()
                    .len(),
                2
            );
            // check structure
            // build identical pattern for comparison
            let pattern_vertex1 = PatternVertex::new(0, 1);
            let pattern_vertex2 = PatternVertex::new(1, 9);
            let pattern_vertex3 = PatternVertex::new(2, 1);
            let pattern_vertex4 = PatternVertex::new(3, 1);
            let pattern_vertex5 = PatternVertex::new(4, 1);
            let pattern_vertex6 = PatternVertex::new(5, 2);
            let pattern_edge1 = PatternEdge::new(0, 11, pattern_vertex1, pattern_vertex2);
            let pattern_edge2 = PatternEdge::new(1, 11, pattern_vertex3, pattern_vertex2);
            let pattern_edge3 = PatternEdge::new(2, 12, pattern_vertex3, pattern_vertex4);
            let pattern_edge4 = PatternEdge::new(3, 12, pattern_vertex4, pattern_vertex5);
            let pattern_edge5 = PatternEdge::new(4, 0, pattern_vertex6, pattern_vertex5);
            let pattern_edge6 = PatternEdge::new(5, 13, pattern_vertex1, pattern_vertex6);
            let pattern_for_comparison = Pattern::try_from(vec![
                pattern_edge1,
                pattern_edge2,
                pattern_edge3,
                pattern_edge4,
                pattern_edge5,
                pattern_edge6,
            ])
            .unwrap();
            // check whether the two pattern has the same code
            let encoder = Encoder::init(4, 4, 1, 3);
            let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
            let pattern_for_comparison_code: Vec<u8> = pattern_for_comparison.encode_to(&encoder);
            assert_eq!(pattern_code, pattern_for_comparison_code);
            // check Tag
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_A)
                    .unwrap()
                    .get_id(),
                0
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_B)
                    .unwrap()
                    .get_id(),
                1
            );
            assert_eq!(
                pattern
                    .get_vertex_from_tag(TAG_C)
                    .unwrap()
                    .get_id(),
                4
            );
        } else if let Err(error) = pattern_result {
            panic!("Build pattern from pb message failed: {:?}", error)
        }
    }

    #[test]
    fn set_accurate_rank_case1() {
        let pattern = build_pattern_case1();
        assert_eq!(pattern.get_vertex_rank(0).unwrap(), 0);
        assert_eq!(pattern.get_vertex_rank(1).unwrap(), 0);
    }

    #[test]
    fn set_accurate_rank_case2() {
        let pattern = build_pattern_case2();
        assert_eq!(pattern.get_vertex_rank(0).unwrap(), 0);
        assert_eq!(pattern.get_vertex_rank(1).unwrap(), 0);
        assert_eq!(pattern.get_vertex_rank(2).unwrap(), 0);
    }

    #[test]
    fn set_accurate_rank_case3() {
        let pattern = build_pattern_case3();
        assert_eq!(pattern.get_vertex_rank(0).unwrap(), 1);
        assert_eq!(pattern.get_vertex_rank(1).unwrap(), 0);
        assert_eq!(pattern.get_vertex_rank(2).unwrap(), 1);
        assert_eq!(pattern.get_vertex_rank(3).unwrap(), 0);
    }

    #[test]
    fn set_accurate_rank_case4() {
        let pattern = build_pattern_case4();
        assert_eq!(pattern.get_vertex_rank(0).unwrap(), 1);
        assert_eq!(pattern.get_vertex_rank(1).unwrap(), 0);
        assert_eq!(pattern.get_vertex_rank(2).unwrap(), 1);
        assert_eq!(pattern.get_vertex_rank(3).unwrap(), 0);
    }

    #[test]
    fn set_accurate_rank_case5() {
        let pattern = build_pattern_case5();
        let id_vec_a: Vec<PatternId> = vec![100, 200, 300, 400];
        let id_vec_b: Vec<PatternId> = vec![10, 20, 30];
        let id_vec_c: Vec<PatternId> = vec![1, 2, 3];
        let id_vec_d: Vec<PatternId> = vec![1000];
        // A
        assert_eq!(pattern.get_vertex_rank(id_vec_a[0]).unwrap(), 1);
        assert_eq!(pattern.get_vertex_rank(id_vec_a[1]).unwrap(), 3);
        assert_eq!(pattern.get_vertex_rank(id_vec_a[2]).unwrap(), 0);
        assert_eq!(pattern.get_vertex_rank(id_vec_a[3]).unwrap(), 2);
        // B
        assert_eq!(pattern.get_vertex_rank(id_vec_b[0]).unwrap(), 0);
        assert_eq!(pattern.get_vertex_rank(id_vec_b[1]).unwrap(), 2);
        assert_eq!(pattern.get_vertex_rank(id_vec_b[2]).unwrap(), 1);
        // C
        assert_eq!(pattern.get_vertex_rank(id_vec_c[0]).unwrap(), 0);
        assert_eq!(pattern.get_vertex_rank(id_vec_c[1]).unwrap(), 2);
        assert_eq!(pattern.get_vertex_rank(id_vec_c[2]).unwrap(), 0);
        // D
        assert_eq!(pattern.get_vertex_rank(id_vec_d[0]).unwrap(), 0);
    }

    #[test]
    fn rank_ranking_case1() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case1();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case2() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case2();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case3() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case3();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case4() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case4();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            1
        );
    }

    #[test]
    fn rank_ranking_case5() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case5();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case6() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case6();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case7() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case7();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case8() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case8();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case9() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case9();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case10() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case10();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case11() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case11();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B2").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case12() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case12();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B2").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B3").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case13() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case13();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B2").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case14() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case14();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B2").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B3").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("C0").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case15() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case15();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B2").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("C0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("C1").unwrap())
                .unwrap(),
            1
        );
    }

    #[test]
    fn rank_ranking_case16() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case16();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B2").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("C0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("C1").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("D0").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case17() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case17();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            4
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A4").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A5").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case17_even_num_chain() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case17_even_num_chain();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            6
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A4").unwrap())
                .unwrap(),
            4
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A5").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A6").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn rank_ranking_case17_long_chain() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case17_long_chain();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            7
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A4").unwrap())
                .unwrap(),
            9
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A5").unwrap())
                .unwrap(),
            10
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A6").unwrap())
                .unwrap(),
            8
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A7").unwrap())
                .unwrap(),
            6
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A8").unwrap())
                .unwrap(),
            4
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A9").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A10").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn vertex_ranking_case17_special_id_situation_1() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case17_special_id_situation_1();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            4
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A4").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A5").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn vertex_ranking_case17_special_id_situation_2() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case17_special_id_situation_2();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            4
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A4").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A5").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn vertex_ranking_case18() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case18();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A4").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A5").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn vertex_ranking_case19() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case19();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            8
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            9
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A4").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A5").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A6").unwrap())
                .unwrap(),
            4
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A7").unwrap())
                .unwrap(),
            6
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A8").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A9").unwrap())
                .unwrap(),
            7
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("C0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("D0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("E0").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn vertex_ranking_case20() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case20();
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            pattern
                .get_vertex_rank(*vertex_id_map.get("A4").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn dfs_sorting_case1() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case1();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn dfs_sorting_case3() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case3();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            2
        );
    }

    #[test]
    fn dfs_sorting_case4() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case4();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            2
        );
    }

    #[test]
    fn dfs_sorting_case6() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case6();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            2
        );
    }

    #[test]
    fn dfs_sorting_case9() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case9();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            2
        );
    }

    #[test]
    fn dfs_sorting_case11() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case11();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B2").unwrap())
                .unwrap(),
            4
        );
    }

    #[test]
    fn dfs_sorting_case13() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case13();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            4
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B2").unwrap())
                .unwrap(),
            3
        );
    }

    #[test]
    fn dfs_sorting_case14() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case14();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            4
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B2").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B3").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("C0").unwrap())
                .unwrap(),
            6
        );
    }

    #[test]
    fn dfs_sorting_case15() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case15();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            4
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            7
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B2").unwrap())
                .unwrap(),
            8
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("C0").unwrap())
                .unwrap(),
            6
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("C1").unwrap())
                .unwrap(),
            3
        );
    }

    #[test]
    fn dfs_sorting_case16() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case16();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            8
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            6
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B1").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B2").unwrap())
                .unwrap(),
            9
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("C0").unwrap())
                .unwrap(),
            7
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("C1").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("D0").unwrap())
                .unwrap(),
            4
        );
    }

    #[test]
    fn dfs_sorting_case17() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case17();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            4
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A4").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A5").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn dfs_sorting_case17_variant_long_chain() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case17_long_chain();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            10
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            9
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            8
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            7
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A4").unwrap())
                .unwrap(),
            6
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A5").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A6").unwrap())
                .unwrap(),
            4
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A7").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A8").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A9").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A10").unwrap())
                .unwrap(),
            0
        );
    }

    #[test]
    fn dfs_sorting_case19() {
        let (pattern, vertex_id_map) = build_pattern_rank_ranking_case19();
        let (_, vertex_dfs_id_map) = pattern.get_dfs_edge_sequence().unwrap();
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A0").unwrap())
                .unwrap(),
            3
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A1").unwrap())
                .unwrap(),
            7
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A2").unwrap())
                .unwrap(),
            0
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A3").unwrap())
                .unwrap(),
            4
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A4").unwrap())
                .unwrap(),
            8
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A5").unwrap())
                .unwrap(),
            11
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A6").unwrap())
                .unwrap(),
            1
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A7").unwrap())
                .unwrap(),
            5
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A8").unwrap())
                .unwrap(),
            9
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("A9").unwrap())
                .unwrap(),
            12
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("B0").unwrap())
                .unwrap(),
            2
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("C0").unwrap())
                .unwrap(),
            10
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("D0").unwrap())
                .unwrap(),
            6
        );
        assert_eq!(
            *vertex_dfs_id_map
                .get(*vertex_id_map.get("E0").unwrap())
                .unwrap(),
            13
        );
    }
}
