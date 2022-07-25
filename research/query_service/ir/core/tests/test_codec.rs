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
    // use ascii::{self, AsciiString, ToAsciiChar};
    use crate::common::extend_step_cases::*;
    use crate::common::pattern_cases::*;
    use ascii::{self, AsciiString};
    use ir_core::catalogue::codec::*;
    use ir_core::catalogue::extend_step::*;
    use ir_core::catalogue::pattern::*;

    /// ### Generate AsciiString from Vector
    // fn generate_asciistring_from_vec(vec: &[u8]) -> AsciiString {
    //     let mut output = AsciiString::new();
    //     for value in vec {
    //         output.push(value.to_ascii_char().unwrap());
    //     }
    //     output
    // }

    // #[test]
    // fn test_create_encode_unit_from_edge() {
    //     let pattern = build_pattern_case6();
    //     let edge1 = pattern.get_edge_from_id(0).unwrap();
    //     let edge2 = pattern.get_edge_from_id(1).unwrap();
    //     let encoder = Encoder::init_by_pattern(&pattern, 5);
    //     let encode_unit_1 = EncodeUnit::from_pattern_edge(&pattern, edge1, &encoder);
    //     assert_eq!(encode_unit_1.values[4], 1);
    //     assert_eq!(encode_unit_1.values[3], 1);
    //     assert_eq!(encode_unit_1.values[2], 2);
    //     assert_eq!(encode_unit_1.values[1], 0);
    //     assert_eq!(encode_unit_1.values[0], 0);
    //     let encode_unit_2 = EncodeUnit::from_pattern_edge(&pattern, edge2, &encoder);
    //     assert_eq!(encode_unit_2.values[4], 2);
    //     assert_eq!(encode_unit_2.values[3], 1);
    //     assert_eq!(encode_unit_2.values[2], 3);
    //     assert_eq!(encode_unit_2.values[1], 0);
    //     assert_eq!(encode_unit_2.values[0], 0);
    // }

    #[test]
    fn test_initialize_encoder_from_parameter_case1() {
        let encoder = Encoder::init(2, 3, 4, 5);
        assert_eq!(encoder.get_edge_label_bit_num(), 2);
        assert_eq!(encoder.get_vertex_label_bit_num(), 3);
        assert_eq!(encoder.get_direction_bit_num(), 4);
        assert_eq!(encoder.get_vertex_rank_bit_num(), 5);
    }

    #[test]
    fn test_initialize_encoder_from_parameter_case2() {
        let encoder = Encoder::init(2, 2, 2, 2);
        assert_eq!(encoder.get_edge_label_bit_num(), 2);
        assert_eq!(encoder.get_vertex_label_bit_num(), 2);
        assert_eq!(encoder.get_direction_bit_num(), 2);
        assert_eq!(encoder.get_vertex_rank_bit_num(), 2);
    }

    #[test]
    fn test_initialize_encoder_from_pattern_case6() {
        let pattern = build_pattern_case6();
        let default_vertex_rank_bit_num = 0;
        let encoder = Encoder::init_by_pattern(&pattern, default_vertex_rank_bit_num);
        assert_eq!(encoder.get_edge_label_bit_num(), 2);
        assert_eq!(encoder.get_vertex_label_bit_num(), 2);
        assert_eq!(encoder.get_direction_bit_num(), 2);
        assert_eq!(encoder.get_vertex_rank_bit_num(), 2);
    }

    #[test]
    fn test_initialize_encoder_from_pattern_case7() {
        let pattern = build_pattern_case7();
        let default_vertex_rank_bit_num = 2;
        let encoder = Encoder::init_by_pattern(&pattern, default_vertex_rank_bit_num);
        assert_eq!(encoder.get_edge_label_bit_num(), 3);
        assert_eq!(encoder.get_vertex_label_bit_num(), 3);
        assert_eq!(encoder.get_direction_bit_num(), 2);
        assert_eq!(encoder.get_vertex_rank_bit_num(), 3);
    }

    // #[test]
    // fn encode_unit_to_ascii_string() {
    //     let pattern = build_pattern_case6();
    //     let edge1 = pattern.get_edge_from_id(0).unwrap();
    //     let edge2 = pattern.get_edge_from_id(1).unwrap();
    //     let encoder = Encoder::init(2, 2, 2, 2);
    //     let encode_unit_1 = EncodeUnit::from_pattern_edge(&pattern, edge1, &encoder);
    //     let encode_string_1 = encode_unit_1.to_ascii_string();
    //     let expected_encode_string_1: AsciiString = generate_asciistring_from_vec(&vec![10, 96]);
    //     assert_eq!(encode_string_1.len(), 2);
    //     assert_eq!(encode_string_1, expected_encode_string_1);
    //     let encode_unit_2 = EncodeUnit::from_pattern_edge(&pattern, edge2, &encoder);
    //     let encode_string_2 = encode_unit_2.to_ascii_string();
    //     let expected_encode_string_2: AsciiString = generate_asciistring_from_vec(&vec![12, 112]);
    //     assert_eq!(encode_string_2.len(), 2);
    //     assert_eq!(encode_string_2, expected_encode_string_2);
    // }

    // #[test]
    // fn encode_unit_to_vec_u8() {
    //     let pattern = build_pattern_case6();
    //     let edge1 = pattern.get_edge_from_id(0).unwrap();
    //     let edge2 = pattern.get_edge_from_id(1).unwrap();
    //     let encoder = Encoder::init(2, 2, 2, 2);
    //     let encode_unit_1 = EncodeUnit::from_pattern_edge(&pattern, edge1, &encoder);
    //     let encode_vec_1 = encode_unit_1.to_vec_u8(8);
    //     let expected_encode_vec_1: Vec<u8> = vec![5, 96];
    //     assert_eq!(encode_vec_1.len(), 2);
    //     assert_eq!(encode_vec_1, expected_encode_vec_1);
    //     let encode_unit_2 = EncodeUnit::from_pattern_edge(&pattern, edge2, &encoder);
    //     let encode_vec_2 = encode_unit_2.to_vec_u8(8);
    //     let expected_encode_vec_2: Vec<u8> = vec![6, 112];
    //     assert_eq!(encode_vec_2.len(), 2);
    //     assert_eq!(encode_vec_2, expected_encode_vec_2);
    // }

    #[test]
    fn test_get_decode_value_by_head_tail_vec8() {
        let src_code: Vec<u8> = vec![95, 115, 87];
        // inside one unit
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 0, 0, 8);
        assert_eq!(picked_value, 1);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 15, 15, 8);
        assert_eq!(picked_value, 0);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 1, 0, 8);
        assert_eq!(picked_value, 3);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 2, 1, 8);
        assert_eq!(picked_value, 3);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 7, 0, 8);
        assert_eq!(picked_value, 87);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 15, 8, 8);
        assert_eq!(picked_value, 115);
        // neighboring units
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 8, 7, 8);
        assert_eq!(picked_value, 2);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 9, 6, 8);
        assert_eq!(picked_value, 13);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 8, 4, 8);
        assert_eq!(picked_value, 21);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 13, 4, 8);
        assert_eq!(picked_value, 821);
        // crossing one unit
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 16, 7, 8);
        assert_eq!(picked_value, 742);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 16, 6, 8);
        assert_eq!(picked_value, 1485);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 18, 2, 8);
        assert_eq!(picked_value, 122069);
    }

    #[test]
    fn test_get_decode_value_by_head_tail_asciistring() {
        let src_code: Vec<u8> = vec![95, 115, 87];
        // inside one unit
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 0, 0, 7);
        assert_eq!(picked_value, 1);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 13, 13, 7);
        assert_eq!(picked_value, 1);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 1, 0, 7);
        assert_eq!(picked_value, 3);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 2, 1, 7);
        assert_eq!(picked_value, 3);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 6, 0, 7);
        assert_eq!(picked_value, 87);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 13, 7, 7);
        assert_eq!(picked_value, 115);
        // neighboring units
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 7, 6, 7);
        assert_eq!(picked_value, 3);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 9, 6, 7);
        assert_eq!(picked_value, 7);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 8, 4, 7);
        assert_eq!(picked_value, 29);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 13, 4, 7);
        assert_eq!(picked_value, 925);
        // crossing one unit
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 14, 6, 7);
        assert_eq!(picked_value, 487);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 16, 6, 7);
        assert_eq!(picked_value, 2023);
        let picked_value = Encoder::get_decode_value_by_head_tail(&src_code, 18, 2, 7);
        assert_eq!(picked_value, 130677);
    }

    // #[test]
    // fn test_decode_from_encode_unit_to_vec_u8() {
    //     let pattern = build_pattern_case6();
    //     let edge1 = pattern.get_edge_from_id(0).unwrap();
    //     let edge2 = pattern.get_edge_from_id(1).unwrap();
    //     let encoder = Encoder::init(2, 2, 2, 2);
    //     let encode_unit_1 = EncodeUnit::from_pattern_edge(&pattern, edge1, &encoder);
    //     let encode_vec_1 = encode_unit_1.to_vec_u8(8);
    //     let encode_unit_2 = EncodeUnit::from_pattern_edge(&pattern, edge2, &encoder);
    //     let encode_vec_2 = encode_unit_2.to_vec_u8(8);
    //     assert_eq!(Encoder::get_decode_value_by_head_tail(&encode_vec_1, 9, 8, 8), edge1.get_label());
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_1, 7, 6, 8),
    //         edge1.get_start_vertex_label()
    //     );
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_1, 5, 4, 8),
    //         edge1.get_end_vertex_label()
    //     );
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_1, 3, 2, 8),
    //         pattern.get_vertex_rank(edge1.get_start_vertex_id())
    //     );
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_1, 1, 0, 8),
    //         pattern.get_vertex_rank(edge1.get_end_vertex_id())
    //     );
    //     assert_eq!(Encoder::get_decode_value_by_head_tail(&encode_vec_2, 9, 8, 8), edge2.get_label());
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_2, 7, 6, 8),
    //         edge2.get_start_vertex_label()
    //     );
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_2, 5, 4, 8),
    //         edge2.get_end_vertex_label()
    //     );
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_2, 3, 2, 8),
    //         pattern.get_vertex_rank(edge2.get_start_vertex_id())
    //     );
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_2, 1, 0, 8),
    //         pattern.get_vertex_rank(edge2.get_end_vertex_id())
    //     );
    // }

    // #[test]
    // fn test_decode_from_encode_unit_to_asciistring() {
    //     let pattern = build_pattern_case6();
    //     let edge1 = pattern.get_edge_from_id(0).unwrap();
    //     let edge2 = pattern.get_edge_from_id(1).unwrap();
    //     let encoder = Encoder::init(2, 2, 2, 2);
    //     let encode_unit_1 = EncodeUnit::from_pattern_edge(&pattern, edge1, &encoder);
    //     let encode_string_1 = encode_unit_1.to_ascii_string();
    //     let encode_unit_2 = EncodeUnit::from_pattern_edge(&pattern, edge2, &encoder);
    //     let encode_string_2 = encode_unit_2.to_ascii_string();
    //     let encode_vec_1: Vec<u8> = encode_string_1
    //         .into_iter()
    //         .map(|ch| ch.as_byte())
    //         .collect();
    //     let encode_vec_2: Vec<u8> = encode_string_2
    //         .into_iter()
    //         .map(|ch| ch.as_byte())
    //         .collect();
    //     assert_eq!(Encoder::get_decode_value_by_head_tail(&encode_vec_1, 9, 8, 7), edge1.get_label());
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_1, 7, 6, 7),
    //         edge1.get_start_vertex_label()
    //     );
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_1, 5, 4, 7),
    //         edge1.get_end_vertex_label()
    //     );
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_1, 3, 2, 7),
    //         pattern.get_vertex_rank(edge1.get_start_vertex_id())
    //     );
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_1, 1, 0, 7),
    //         pattern.get_vertex_rank(edge1.get_end_vertex_id())
    //     );
    //     assert_eq!(Encoder::get_decode_value_by_head_tail(&encode_vec_2, 9, 8, 7), edge2.get_label());
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_2, 7, 6, 7),
    //         edge2.get_start_vertex_label()
    //     );
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_2, 5, 4, 7),
    //         edge2.get_end_vertex_label()
    //     );
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_2, 3, 2, 7),
    //         pattern.get_vertex_rank(edge2.get_start_vertex_id())
    //     );
    //     assert_eq!(
    //         Encoder::get_decode_value_by_head_tail(&encode_vec_2, 1, 0, 7),
    //         pattern.get_vertex_rank(edge2.get_end_vertex_id())
    //     );
    // }

    #[test]
    fn test_encode_decode_one_vertex_pattern() {
        // Pattern has label 2
        let pattern = Pattern::from(PatternVertex::new(0, 2));
        let encoder = Encoder::init_by_pattern(&pattern, 1);
        let code1: Vec<u8> = pattern.encode_to(&encoder);
        let pattern: Pattern = Pattern::decode_from(&code1, &encoder).unwrap();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_one_vertex_pattern_ascii_string() {
        // Pattern has label 5
        let pattern = Pattern::from(PatternVertex::new(0, 5));
        let encoder = Encoder::init_by_pattern(&pattern, 1);
        let code1: AsciiString = pattern.encode_to(&encoder);
        let pattern: Pattern = Pattern::decode_from(&code1, &encoder).unwrap();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case1() {
        let pattern = build_pattern_case1();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let code1: Vec<u8> = pattern.encode_to(&encoder);
        let pattern: Pattern = Pattern::decode_from(&code1, &encoder).unwrap();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case2() {
        let pattern = build_pattern_case2();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let code1: Vec<u8> = pattern.encode_to(&encoder);
        let pattern: Pattern = Pattern::decode_from(&code1, &encoder).unwrap();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case3() {
        let pattern = build_pattern_case3();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let code1: Vec<u8> = pattern.encode_to(&encoder);
        let pattern = build_pattern_case3();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case4() {
        let pattern = build_pattern_case4();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let code1: Vec<u8> = pattern.encode_to(&encoder);
        let pattern = build_pattern_case4();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_of_case5() {
        let pattern = build_pattern_case5();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let code1: Vec<u8> = pattern.encode_to(&encoder);
        let pattern = build_pattern_case5();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(code1, code2);
    }

    #[test]
    fn test_encode_decode_pattern_case1_vec_u8() {
        let pattern = build_pattern_case1();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case1_ascii_string() {
        let pattern = build_pattern_case1();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: AsciiString = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case2_vec_u8() {
        let pattern = build_pattern_case2();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case2_ascii_string() {
        let pattern = build_pattern_case2();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: AsciiString = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case3_vec_u8() {
        let pattern = build_pattern_case3();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case3_ascii_string() {
        let pattern = build_pattern_case3();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: AsciiString = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case4_vec_u8() {
        let pattern = build_pattern_case4();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case4_ascii_string() {
        let pattern = build_pattern_case4();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: AsciiString = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case5_vec_u8() {
        let pattern = build_pattern_case5();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case5_ascii_string() {
        let pattern = build_pattern_case5();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: AsciiString = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case6_vec_u8() {
        let pattern = build_pattern_case6();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case6_ascii_string() {
        let pattern = build_pattern_case6();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: AsciiString = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case7_vec_u8() {
        let pattern = build_pattern_case7();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: Vec<u8> = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_pattern_case7_ascii_string() {
        let pattern = build_pattern_case7();
        let encoder = Encoder::init_by_pattern(&pattern, 2);
        let pattern_code: AsciiString = pattern.encode_to(&encoder);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case1_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case1();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case1();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case1_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case1();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case1();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case2_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case2();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case2();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case2_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case2();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case2();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case3_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case3();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case3();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case3_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case3();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case3();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case4_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case4();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case4();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case4_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case4();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case4();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case5_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case5();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case5();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case5_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case5();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case5();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case6_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case6();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case6();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case6_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case6();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case6();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case7_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case7();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case7();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case7_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case7();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case7();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case8_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case8();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case8();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case8_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case8();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case8();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case9_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case9();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case9();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case9_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case9();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case9();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case10_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case10();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case10();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case10_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case10();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case10();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case11_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case11();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case11();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case11_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case11();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case11();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case12_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case12();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case12();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case12_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case12();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case12();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case13_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case13();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case13();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case13_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case13();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case13();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case14_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case14();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case14();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case14_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case14();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case14();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case15_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case15();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case15();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case15_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case15();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case15();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case16_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case16();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case16();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case16_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case16();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case16();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case17_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case17();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case17();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case17_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case17();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case17();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case18_vec_u8() {
        let (pattern, _) = build_pattern_rank_ranking_case18();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: Vec<u8> = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case18();
        let pattern_code2: Vec<u8> = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: Vec<u8> = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_rank_ranking_case18_ascii_string() {
        let (pattern, _) = build_pattern_rank_ranking_case18();
        let encoder = Encoder::init_by_pattern(&pattern, 4);
        let pattern_code1: AsciiString = pattern.encode_to(&encoder);
        let (pattern, _) = build_pattern_rank_ranking_case18();
        let pattern_code2: AsciiString = pattern.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code2);
        let pattern_from_decode: Pattern = Cipher::decode_from(&pattern_code1, &encoder).unwrap();
        let pattern_code_from_decode: AsciiString = pattern_from_decode.encode_to(&encoder);
        assert_eq!(pattern_code1, pattern_code_from_decode);
    }

    #[test]
    fn test_encode_decode_extend_step_case2_vec_u8() {
        let extend_step_1 = build_extend_step_case2();
        let encoder = Encoder::init(2, 2, 2, 2);
        let extend_step_1_code: Vec<u8> = extend_step_1.encode_to(&encoder);
        let extend_step_1_from_decode: ExtendStep =
            Cipher::decode_from(&extend_step_1_code, &encoder).unwrap();
        assert_eq!(extend_step_1.get_target_v_label(), extend_step_1_from_decode.get_target_v_label());
        assert_eq!(extend_step_1.get_extend_edges_num(), extend_step_1_from_decode.get_extend_edges_num());
        assert_eq!(
            extend_step_1
                .get_extend_edges_by_start_v(1, 0)
                .unwrap(),
            extend_step_1_from_decode
                .get_extend_edges_by_start_v(1, 0)
                .unwrap()
        );
        assert_eq!(
            extend_step_1
                .get_extend_edges_by_start_v(1, 1)
                .unwrap(),
            extend_step_1_from_decode
                .get_extend_edges_by_start_v(1, 1)
                .unwrap()
        );
        assert_eq!(
            extend_step_1
                .get_extend_edges_by_start_v(2, 0)
                .unwrap(),
            extend_step_1_from_decode
                .get_extend_edges_by_start_v(2, 0)
                .unwrap()
        );
    }

    #[test]
    fn test_encode_decode_extend_step_case2_ascii_string() {
        let extend_step_1 = build_extend_step_case2();
        let encoder = Encoder::init(2, 2, 2, 2);
        let extend_step_1_code: AsciiString = extend_step_1.encode_to(&encoder);
        let extend_step_1_from_decode: ExtendStep =
            Cipher::decode_from(&extend_step_1_code, &encoder).unwrap();
        assert_eq!(extend_step_1.get_target_v_label(), extend_step_1_from_decode.get_target_v_label());
        assert_eq!(extend_step_1.get_extend_edges_num(), extend_step_1_from_decode.get_extend_edges_num());
        assert_eq!(
            extend_step_1
                .get_extend_edges_by_start_v(1, 0)
                .unwrap(),
            extend_step_1_from_decode
                .get_extend_edges_by_start_v(1, 0)
                .unwrap()
        );
        assert_eq!(
            extend_step_1
                .get_extend_edges_by_start_v(1, 1)
                .unwrap(),
            extend_step_1_from_decode
                .get_extend_edges_by_start_v(1, 1)
                .unwrap()
        );
        assert_eq!(
            extend_step_1
                .get_extend_edges_by_start_v(2, 0)
                .unwrap(),
            extend_step_1_from_decode
                .get_extend_edges_by_start_v(2, 0)
                .unwrap()
        );
    }
}
