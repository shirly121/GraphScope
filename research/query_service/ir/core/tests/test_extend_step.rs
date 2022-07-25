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
    use ir_core::catalogue::codec::*;
    use ir_core::catalogue::PatternDirection;

    use crate::common::{extend_step_cases::*, pattern_cases::*, pattern_meta_cases::*};

    /// Test whether pattern1 + extend_step = pattern2
    #[test]
    fn test_pattern_case1_case2_extend_de_extend() {
        let pattern1 = build_pattern_case1();
        let encoder1 = Encoder::init_by_pattern(&pattern1, 2);
        let pattern1_code: Vec<u8> = Cipher::encode_to(&pattern1, &encoder1);
        let extend_step = build_extend_step_case1();
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_pattern_case2();
        let encoder2 = Encoder::init_by_pattern(&pattern2, 2);
        let pattern2_code: Vec<u8> = Cipher::encode_to(&pattern2, &encoder2);
        let pattern_after_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_extend, &encoder2);
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code, &encoder1)
            .unwrap();
        let pattern_after_de_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_de_extend, &encoder1);
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_pattern_case8_case9_extend_de_extend() {
        let pattern1 = build_pattern_case8();
        let encoder1 = Encoder::init_by_pattern(&pattern1, 2);
        let pattern1_code: Vec<u8> = Cipher::encode_to(&pattern1, &encoder1);
        let extend_step = build_extend_step_case2();
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_pattern_case9();
        let encoder2 = Encoder::init_by_pattern(&pattern2, 2);
        let pattern2_code: Vec<u8> = Cipher::encode_to(&pattern2, &encoder2);
        let pattern_after_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_extend, &encoder2);
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code, &encoder1)
            .unwrap();
        let pattern_after_de_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_de_extend, &encoder1);
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_modern_case1_case3_extend_de_extend_1() {
        let pattern1 = build_modern_pattern_case1();
        let encoder1 = Encoder::init_by_pattern(&pattern1, 2);
        let pattern1_code: Vec<u8> = Cipher::encode_to(&pattern1, &encoder1);
        let extend_step = build_modern_extend_step_case1();
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_modern_pattern_case3();
        let encoder2 = Encoder::init_by_pattern(&pattern2, 2);
        let pattern2_code: Vec<u8> = Cipher::encode_to(&pattern2, &encoder2);
        let pattern_after_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_extend, &encoder2);
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code, &encoder1)
            .unwrap();
        let pattern_after_de_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_de_extend, &encoder1);
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_modern_case1_case3_extend_de_extend_2() {
        let pattern1 = build_modern_pattern_case1();
        let encoder1 = Encoder::init_by_pattern(&pattern1, 2);
        let pattern1_code: Vec<u8> = Cipher::encode_to(&pattern1, &encoder1);
        let extend_step = build_modern_extend_step_case2();
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_modern_pattern_case3();
        let encoder2 = Encoder::init_by_pattern(&pattern2, 2);
        let pattern2_code: Vec<u8> = Cipher::encode_to(&pattern2, &encoder2);
        let pattern_after_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_extend, &encoder2);
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code, &encoder1)
            .unwrap();
        let pattern_after_de_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_de_extend, &encoder1);
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_modern_case1_case4_extend_de_extend() {
        let pattern1 = build_modern_pattern_case1();
        let encoder1 = Encoder::init_by_pattern(&pattern1, 2);
        let pattern1_code: Vec<u8> = Cipher::encode_to(&pattern1, &encoder1);
        let extend_step = build_modern_extend_step_case3();
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_modern_pattern_case4();
        let encoder2 = Encoder::init_by_pattern(&pattern2, 2);
        let pattern2_code: Vec<u8> = Cipher::encode_to(&pattern2, &encoder2);
        let pattern_after_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_extend, &encoder2);
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code, &encoder1)
            .unwrap();
        let pattern_after_de_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_de_extend, &encoder1);
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_modern_case2_case4_extend_de_extend() {
        let pattern1 = build_modern_pattern_case2();
        let encoder1 = Encoder::init_by_pattern(&pattern1, 2);
        let pattern1_code: Vec<u8> = Cipher::encode_to(&pattern1, &encoder1);
        let extend_step = build_modern_extend_step_case4();
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_modern_pattern_case4();
        let encoder2 = Encoder::init_by_pattern(&pattern2, 2);
        let pattern2_code: Vec<u8> = Cipher::encode_to(&pattern2, &encoder2);
        let pattern_after_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_extend, &encoder2);
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code, &encoder1)
            .unwrap();
        let pattern_after_de_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_de_extend, &encoder1);
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_modern_case3_case5_extend_de_extend() {
        let pattern1 = build_modern_pattern_case3();
        let encoder1 = Encoder::init_by_pattern(&pattern1, 2);
        let pattern1_code: Vec<u8> = Cipher::encode_to(&pattern1, &encoder1);
        let extend_step = build_modern_extend_step_case6();
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_modern_pattern_case5();
        let encoder2 = Encoder::init_by_pattern(&pattern2, 2);
        let pattern2_code: Vec<u8> = Cipher::encode_to(&pattern2, &encoder2);
        let pattern_after_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_extend, &encoder2);
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code, &encoder1)
            .unwrap();
        let pattern_after_de_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_de_extend, &encoder1);
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_modern_case4_case5_extend_de_extend() {
        let pattern1 = build_modern_pattern_case4();
        let encoder1 = Encoder::init_by_pattern(&pattern1, 2);
        let pattern1_code: Vec<u8> = Cipher::encode_to(&pattern1, &encoder1);
        let extend_step = build_modern_extend_step_case5();
        let pattern_after_extend = pattern1.extend(&extend_step).unwrap();
        // Pattern after extend should be exactly the same as pattern2
        let pattern2 = build_modern_pattern_case5();
        let encoder2 = Encoder::init_by_pattern(&pattern2, 2);
        let pattern2_code: Vec<u8> = Cipher::encode_to(&pattern2, &encoder2);
        let pattern_after_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_extend, &encoder2);
        // Pattern after de_extend should be exactly the same as pattern1
        let pattern_after_de_extend = pattern_after_extend
            .de_extend(&extend_step, &pattern1_code, &encoder1)
            .unwrap();
        let pattern_after_de_extend_code: Vec<u8> = Cipher::encode_to(&pattern_after_de_extend, &encoder1);
        assert_eq!(pattern_after_extend_code, pattern2_code);
        assert_eq!(pattern_after_de_extend_code, pattern1_code);
    }

    #[test]
    fn test_get_extend_steps_of_modern_case1() {
        let modern_pattern_meta = get_modern_pattern_meta();
        let person_only_pattern = build_modern_pattern_case1();
        let all_extend_steps = person_only_pattern.get_extend_steps(&modern_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 3);
        let mut out_0_0_0 = 0;
        let mut incoming_0_0_0 = 0;
        let mut out_0_0_1 = 0;
        for extend_step in all_extend_steps {
            let extend_edges = extend_step
                .get_extend_edges_by_start_v(0, 0)
                .unwrap();
            assert_eq!(extend_edges.len(), 1);
            let extend_edge = extend_edges[0];
            assert_eq!(extend_edge.get_start_vertex_label(), 0);
            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
            if extend_step.get_target_v_label() == 0 {
                if extend_edge.get_direction() == PatternDirection::Out {
                    out_0_0_0 += 1;
                }
                if extend_edge.get_direction() == PatternDirection::In {
                    incoming_0_0_0 += 1;
                }
            }
            if extend_step.get_target_v_label() == 1 && extend_edge.get_direction() == PatternDirection::Out
            {
                out_0_0_1 += 1;
            }
        }
        assert_eq!(out_0_0_0, 1);
        assert_eq!(incoming_0_0_0, 1);
        assert_eq!(out_0_0_1, 1);
    }

    #[test]
    fn test_get_extend_steps_of_modern_case2() {
        let modern_pattern_meta = get_modern_pattern_meta();
        let person_only_pattern = build_modern_pattern_case2();
        let all_extend_steps = person_only_pattern.get_extend_steps(&modern_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 1);
        assert_eq!(all_extend_steps[0].get_target_v_label(), 0);
        assert_eq!(all_extend_steps[0].get_diff_start_v_num(), 1);
        let extend_edge = all_extend_steps[0]
            .get_extend_edges_by_start_v(1, 0)
            .unwrap()[0];
        assert_eq!(extend_edge.get_start_vertex_label(), 1);
        assert_eq!(extend_edge.get_start_vertex_rank(), 0);
        assert_eq!(extend_edge.get_edge_label(), 1);
        assert_eq!(extend_edge.get_direction(), PatternDirection::In);
    }

    #[test]
    fn test_get_extend_steps_of_modern_case3() {
        let modern_pattern_meta = get_modern_pattern_meta();
        let person_knows_person = build_modern_pattern_case3();
        let all_extend_steps = person_knows_person.get_extend_steps(&modern_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 11);
        let mut extend_steps_with_label_0_count = 0;
        let mut extend_steps_with_label_1_count = 0;
        let mut out_0_0_0_count = 0;
        let mut incoming_0_0_0_count = 0;
        let mut out_0_1_0_count = 0;
        let mut incoming_0_1_0_count = 0;
        let mut out_0_0_1_count = 0;
        let mut out_0_1_1_count = 0;
        let mut out_0_0_0_out_0_1_0_count = 0;
        let mut out_0_0_0_incoming_0_1_0_count = 0;
        let mut incoming_0_0_0_out_0_1_0_count = 0;
        let mut incoming_0_0_0_incoming_0_1_0_count = 0;
        let mut out_0_0_1_out_0_1_1_count = 0;
        for extend_step in all_extend_steps {
            if extend_step.get_target_v_label() == 0 {
                extend_steps_with_label_0_count += 1;
                if extend_step.get_diff_start_v_num() == 1 {
                    if extend_step.has_extend_from_start_v(0, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1
                            }
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_0_1_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_1_1_count += 1;
                            }
                        }
                    } else if extend_step.has_extend_from_start_v(0, 1) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 1)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 1);
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_1_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_1_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_0_1_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_1_1_count += 1;
                            }
                        }
                    }
                } else if extend_step.get_diff_start_v_num() == 2 {
                    let mut found_out_0_0_0 = false;
                    let mut found_incoming_0_0_0 = false;
                    let mut found_out_0_1_0 = false;
                    let mut found_incoming_0_1_0 = false;
                    if extend_step.has_extend_from_start_v(0, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                found_out_0_0_0 = true;
                            } else if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                found_incoming_0_0_0 = true;
                            }
                        }
                    }
                    if extend_step.has_extend_from_start_v(0, 1) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 1)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 1);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                found_out_0_1_0 = true;
                            } else if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                found_incoming_0_1_0 = true;
                            }
                        }
                    }
                    if found_out_0_0_0 && found_out_0_1_0 {
                        out_0_0_0_out_0_1_0_count += 1;
                    } else if found_out_0_0_0 && found_incoming_0_1_0 {
                        out_0_0_0_incoming_0_1_0_count += 1;
                    } else if found_incoming_0_0_0 && found_out_0_1_0 {
                        incoming_0_0_0_out_0_1_0_count += 1;
                    } else if found_incoming_0_0_0 && found_incoming_0_1_0 {
                        incoming_0_0_0_incoming_0_1_0_count += 1;
                    }
                }
            } else if extend_step.get_target_v_label() == 1 {
                extend_steps_with_label_1_count += 1;
                if extend_step.get_diff_start_v_num() == 1 {
                    if extend_step.has_extend_from_start_v(0, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_0_1_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1
                            }
                        }
                    } else if extend_step.has_extend_from_start_v(0, 1) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 1)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 1);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                out_0_1_1_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1
                            }
                        }
                    }
                } else if extend_step.get_diff_start_v_num() == 2 {
                    let mut found_out_0_0_1 = false;
                    let mut found_out_0_1_1 = false;
                    if extend_step.has_extend_from_start_v(0, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                found_out_0_0_1 = true;
                            }
                        }
                    }
                    if extend_step.has_extend_from_start_v(0, 1) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 1)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 1);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 1
                            {
                                found_out_0_1_1 = true;
                            }
                        }
                    }
                    if found_out_0_0_1 && found_out_0_1_1 {
                        out_0_0_1_out_0_1_1_count += 1;
                    }
                }
            }
        }
        assert_eq!(extend_steps_with_label_0_count, 8);
        assert_eq!(extend_steps_with_label_1_count, 3);
        assert_eq!(out_0_0_0_count, 1);
        assert_eq!(incoming_0_0_0_count, 1);
        assert_eq!(out_0_1_0_count, 1);
        assert_eq!(incoming_0_1_0_count, 1);
        assert_eq!(out_0_0_1_count, 1);
        assert_eq!(out_0_1_1_count, 1);
        assert_eq!(out_0_0_0_out_0_1_0_count, 1);
        assert_eq!(out_0_0_0_incoming_0_1_0_count, 1);
        assert_eq!(incoming_0_0_0_out_0_1_0_count, 1);
        assert_eq!(incoming_0_0_0_incoming_0_1_0_count, 1);
        assert_eq!(out_0_0_1_out_0_1_1_count, 1);
    }

    #[test]
    fn test_get_extend_steps_of_modern_case4() {
        let modern_pattern_meta = get_modern_pattern_meta();
        let person_created_software = build_modern_pattern_case4();
        let all_extend_steps = person_created_software.get_extend_steps(&modern_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 6);
        let mut extend_steps_with_label_0_count = 0;
        let mut extend_steps_with_label_1_count = 0;
        let mut out_0_0_0_count = 0;
        let mut incoming_0_0_0_count = 0;
        let mut incoming_1_0_1_count = 0;
        let mut out_0_0_0_incoming_1_0_1_count = 0;
        let mut incoming_0_0_0_incoming_1_0_1_count = 0;
        for extend_step in all_extend_steps {
            if extend_step.get_target_v_label() == 0 {
                extend_steps_with_label_0_count += 1;
                if extend_step.get_diff_start_v_num() == 1 {
                    if extend_step.has_extend_from_start_v(0, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 1
                            {
                                incoming_1_0_1_count += 1;
                            }
                        }
                    } else if extend_step.has_extend_from_start_v(1, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(1, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 1);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                out_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                incoming_0_0_0_count += 1;
                            }
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 1
                            {
                                incoming_1_0_1_count += 1;
                            }
                        }
                    }
                } else if extend_step.get_diff_start_v_num() == 2 {
                    let mut found_out_0_0_0 = false;
                    let mut found_incoming_1_0_1 = false;
                    let mut found_incoming_0_0_0 = false;
                    if extend_step.has_extend_from_start_v(0, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(0, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 0);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::Out
                                && extend_edge.get_edge_label() == 0
                            {
                                found_out_0_0_0 = true;
                            } else if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 0
                            {
                                found_incoming_0_0_0 = true;
                            }
                        }
                    }
                    if extend_step.has_extend_from_start_v(1, 0) {
                        let extend_edges = extend_step
                            .get_extend_edges_by_start_v(1, 0)
                            .unwrap();
                        for extend_edge in extend_edges {
                            assert_eq!(extend_edge.get_start_vertex_label(), 1);
                            assert_eq!(extend_edge.get_start_vertex_rank(), 0);
                            if extend_edge.get_direction() == PatternDirection::In
                                && extend_edge.get_edge_label() == 1
                            {
                                found_incoming_1_0_1 = true;
                            }
                        }
                    }
                    if found_out_0_0_0 && found_incoming_1_0_1 {
                        out_0_0_0_incoming_1_0_1_count += 1;
                    } else if found_incoming_0_0_0 && found_incoming_1_0_1 {
                        incoming_0_0_0_incoming_1_0_1_count += 1;
                    }
                }
            } else if extend_step.get_target_v_label() == 1 {
                extend_steps_with_label_1_count += 1;
            }
        }
        assert_eq!(extend_steps_with_label_0_count, 5);
        assert_eq!(extend_steps_with_label_1_count, 1);
        assert_eq!(out_0_0_0_count, 1);
        assert_eq!(incoming_0_0_0_count, 1);
        assert_eq!(incoming_1_0_1_count, 1);
        assert_eq!(out_0_0_0_incoming_1_0_1_count, 1);
        assert_eq!(incoming_0_0_0_incoming_1_0_1_count, 1);
    }

    #[test]
    fn test_get_extend_steps_of_ldbc_case1() {
        let ldbc_pattern_meta = get_ldbc_pattern_meta();
        let person_knows_person = build_ldbc_pattern_case1();
        let all_extend_steps = person_knows_person.get_extend_steps(&ldbc_pattern_meta, 10);
        assert_eq!(all_extend_steps.len(), 44);
    }
}
