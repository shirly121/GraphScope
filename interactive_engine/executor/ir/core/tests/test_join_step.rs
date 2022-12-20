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
    use ir_core::catalogue::pattern::Pattern;
    use ir_core::catalogue::PatternLabelId;

    use crate::common::canonical_label_cases::*;
    use crate::common::join_step_cases::*;
    use crate::common::pattern_cases::*;

    fn check_decomposition_plan(pattern: &Pattern, num_plans: usize) {
        let pattern_code: Vec<u8> = pattern.encode_to();
        let decomposition_plans = pattern.binary_join_decomposition().unwrap();
        // --debug--
        decomposition_plans
            .iter()
            .for_each(|binary_join_plan| {
                println!("[!!Test Plan!!]");
                let build_pattern_vertices: Vec<(PatternLabelId, usize, usize)> = binary_join_plan
                    .get_build_pattern()
                    .vertices_iter()
                    .map(|vertex| {
                        let v_id = vertex.get_id();
                        let v_label = vertex.get_label();
                        let v_group = pattern.get_vertex_group(v_id).unwrap();
                        let v_rank = pattern.get_vertex_rank(v_id).unwrap();
                        (v_label, v_group, v_rank)
                    })
                    .collect();
                println!("Build pattern vertices: {:?}", build_pattern_vertices);
                println!(
                    "Build pattern num edges: {}",
                    binary_join_plan
                        .get_build_pattern()
                        .get_edges_num()
                );
                binary_join_plan
                    .get_build_pattern()
                    .edges_iter()
                    .for_each(|edge| println!("{:?}", edge));
                let probe_pattern_vertices: Vec<(PatternLabelId, usize, usize)> = binary_join_plan
                    .get_probe_pattern()
                    .vertices_iter()
                    .map(|vertex| {
                        let v_id = vertex.get_id();
                        let v_label = vertex.get_label();
                        let v_group = pattern.get_vertex_group(v_id).unwrap();
                        let v_rank = pattern.get_vertex_rank(v_id).unwrap();
                        (v_label, v_group, v_rank)
                    })
                    .collect();
                println!("Probe pattern vertices: {:?}", probe_pattern_vertices);
                println!(
                    "Probe pattern num edges: {}",
                    binary_join_plan
                        .get_probe_pattern()
                        .get_edges_num()
                );
                binary_join_plan
                    .get_probe_pattern()
                    .edges_iter()
                    .for_each(|edge| println!("{:?}", edge));
                let shared_vertices: Vec<(PatternLabelId, usize, usize)> = binary_join_plan
                    .get_shared_vertices()
                    .iter()
                    .map(|&shared_v_id| {
                        let v_label = pattern
                            .get_vertex(shared_v_id)
                            .expect("Failed to get vertex from id")
                            .get_label();
                        let v_group = pattern.get_vertex_group(shared_v_id).unwrap();
                        let v_rank = pattern.get_vertex_rank(shared_v_id).unwrap();
                        (v_label, v_group, v_rank)
                    })
                    .collect();
                println!("Shared vertices info: {:?}", shared_vertices);
            });
        // --debug--
        assert_eq!(decomposition_plans.len(), num_plans);
        for binary_join_plan in decomposition_plans {
            let joined_pattern = binary_join_plan
                .join_with_raw_id()
                .expect("Failed to join two patterns");
            assert_eq!(joined_pattern.get_vertices_num(), pattern.get_vertices_num());
            assert_eq!(joined_pattern.get_edges_num(), pattern.get_edges_num());
            let joined_pattern_code: Vec<u8> = joined_pattern.encode_to();
            assert_eq!(pattern_code, joined_pattern_code);
        }
    }

    #[test]
    fn binary_join_decomposition_ldbc_bi3() {
        let pattern = build_ldbc_bi3().unwrap();
        check_decomposition_plan(&pattern, 4);
    }

    #[test]
    fn join_decomposition_gb1() {
        let pattern = build_gb_query_1().unwrap();
        check_decomposition_plan(&pattern, 0);
    }

    #[test]
    fn join_decomposition_gb6() {
        let pattern: Pattern = build_gb_query_6().unwrap();
        check_decomposition_plan(&pattern, 0);
    }

    #[test]
    fn join_decomposition_gb9() {
        let pattern: Pattern = build_gb_query_9().unwrap();
        check_decomposition_plan(&pattern, 0);
    }

    #[test]
    fn join_decomposition_gb12() {
        let pattern: Pattern = build_gb_query_12().unwrap();
        check_decomposition_plan(&pattern, 0);
    }

    #[test]
    fn join_decomposition_gb14() {
        let pattern: Pattern = build_gb_query_14().unwrap();
        check_decomposition_plan(&pattern, 3);
    }

    // #[test]
    // fn binary_join_decomposition_ldbc_bi11() {
    //     let pattern = build_ldbc_bi11().unwrap();
    //     check_decomposition_plan(&pattern, 6);
    // }

    #[test]
    fn binary_join_decomposition_vertex_ranking_case11() {
        let (pattern, _vertex_id_map) = build_pattern_rank_ranking_case11();
        let decomposition_plans = pattern.binary_join_decomposition().unwrap();
        assert_eq!(decomposition_plans.len(), 0);
    }

    #[test]
    fn binary_join_decomposition_vertex_ranking_case12() {
        let (pattern, _vertex_id_map) = build_pattern_rank_ranking_case12();
        check_decomposition_plan(&pattern, 2);
    }

    #[test]
    fn binary_join_decomposition_vertex_ranking_case13() {
        let (pattern, _vertex_id_map) = build_pattern_rank_ranking_case13();
        check_decomposition_plan(&pattern, 3);
    }

    // #[test]
    // fn binary_join_decomposition_vertex_ranking_case14() {
    //     let (pattern, _vertex_id_map) = build_pattern_rank_ranking_case14();
    //     check_decomposition_plan(&pattern, 4);
    // }

    #[test]
    fn binary_join_decomposition_vertex_ranking_case15() {
        let (pattern, _vertex_id_map) = build_pattern_rank_ranking_case15();
        check_decomposition_plan(&pattern, 5);
    }

    #[test]
    fn binary_join_decomposition_vertex_ranking_case16() {
        let (pattern, _vertex_id_map) = build_pattern_rank_ranking_case16();
        check_decomposition_plan(&pattern, 6);
    }

    #[test]
    fn binary_join_decomposition_vertex_ranking_case17() {
        let (pattern, _vertex_id_map) = build_pattern_rank_ranking_case17();
        check_decomposition_plan(&pattern, 1);
    }

    #[test]
    fn binary_join_decomposition_vertex_ranking_case18() {
        let (pattern, _vertex_id_map) = build_pattern_rank_ranking_case18();
        check_decomposition_plan(&pattern, 1);
    }

    // #[test]
    // fn binary_join_decomposition_vertex_ranking_case19() {
    //     let (pattern, _vertex_id_map) = build_pattern_rank_ranking_case19();
    //     check_decomposition_plan(&pattern, 10);
    // }

    #[test]
    fn binary_join_decomposition_vertex_ranking_case20() {
        let (pattern, _vertex_id_map) = build_pattern_rank_ranking_case20();
        check_decomposition_plan(&pattern, 0);
    }
}
