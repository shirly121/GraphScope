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
mod test {
    use ir_core::catalogue::catalog::Catalogue;

    use crate::common::pattern_cases::*;
    use crate::common::pattern_meta_cases::*;

    #[test]
    fn test_catalog_for_modern_graph() {
        let modern_graph_meta = get_modern_pattern_meta();
        let catalog = Catalogue::build_from_meta(&modern_graph_meta, 2, 3);
        assert_eq!(4, catalog.get_patterns_num());
        assert_eq!(4, catalog.get_approaches_num());
    }

    #[test]
    fn test_catalog_for_ldbc_graph() {
        let ldbc_graph_meta = get_ldbc_pattern_meta();
        let catalog = Catalogue::build_from_meta(&ldbc_graph_meta, 2, 3);
        assert_eq!(34, catalog.get_patterns_num());
        assert_eq!(42, catalog.get_approaches_num());
    }

    #[test]
    fn test_catalog_for_modern_pattern_case1() {
        let modern_pattern = build_modern_pattern_case1();
        let catalog = Catalogue::build_from_pattern(&modern_pattern);
        assert_eq!(1, catalog.get_patterns_num());
        assert_eq!(0, catalog.get_approaches_num());
    }

    #[test]
    fn test_catalog_for_modern_pattern_case2() {
        let modern_pattern = build_modern_pattern_case2();
        let catalog = Catalogue::build_from_pattern(&modern_pattern);
        assert_eq!(1, catalog.get_patterns_num());
        assert_eq!(0, catalog.get_approaches_num());
    }

    #[test]
    fn test_catalog_for_modern_pattern_case3() {
        let modern_pattern = build_modern_pattern_case3();
        let catalog = Catalogue::build_from_pattern(&modern_pattern);
        assert_eq!(2, catalog.get_patterns_num());
        assert_eq!(2, catalog.get_approaches_num());
    }

    #[test]
    fn test_catalog_for_modern_pattern_case4() {
        let modern_pattern = build_modern_pattern_case4();
        let catalog = Catalogue::build_from_pattern(&modern_pattern);
        assert_eq!(3, catalog.get_patterns_num());
        assert_eq!(2, catalog.get_approaches_num());
    }

    #[test]
    fn test_catalog_for_ldbc_pattern_from_pb_case1() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case1().unwrap();
        let catalog = Catalogue::build_from_pattern(&ldbc_pattern);
        assert_eq!(3, catalog.get_patterns_num());
        assert_eq!(5, catalog.get_approaches_num());
    }

    #[test]
    fn test_catalog_for_ldbc_pattern_from_pb_case2() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case2().unwrap();
        let catalog = Catalogue::build_from_pattern(&ldbc_pattern);
        assert_eq!(5, catalog.get_patterns_num());
        assert_eq!(7, catalog.get_approaches_num());
    }

    #[test]
    fn test_catalog_for_ldbc_pattern_from_pb_case3() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case3().unwrap();
        let catalog = Catalogue::build_from_pattern(&ldbc_pattern);
        assert_eq!(4, catalog.get_patterns_num());
        assert_eq!(9, catalog.get_approaches_num());
    }

    #[test]
    fn test_catalog_for_ldbc_pattern_from_pb_case4() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case4().unwrap();
        let catalog = Catalogue::build_from_pattern(&ldbc_pattern);
        assert_eq!(11, catalog.get_patterns_num());
        assert_eq!(17, catalog.get_approaches_num());
    }
}
