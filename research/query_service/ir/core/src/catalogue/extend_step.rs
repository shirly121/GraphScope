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

use std::collections::{BTreeMap, VecDeque};
use std::convert::TryFrom;
use std::iter::Iterator;

use ir_common::generated::algebra as pb;

use crate::catalogue::pattern::Pattern;
use crate::catalogue::{query_params, PatternId};
use crate::catalogue::{DynIter, PatternDirection, PatternLabelId, PatternRankId};
use crate::error::{IrError, IrResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExtendEdge {
    start_v_label: PatternLabelId,
    start_v_rank: PatternRankId,
    edge_label: PatternLabelId,
    dir: PatternDirection,
}

/// Initializer of ExtendEdge
impl ExtendEdge {
    pub fn new(
        start_v_label: PatternLabelId, start_v_rank: PatternRankId, edge_label: PatternLabelId,
        dir: PatternDirection,
    ) -> ExtendEdge {
        ExtendEdge { start_v_label, start_v_rank, edge_label, dir }
    }
}

/// Methods for access fields of VagueExtendEdge
impl ExtendEdge {
    #[inline]
    pub fn get_start_vertex_label(&self) -> PatternLabelId {
        self.start_v_label
    }

    #[inline]
    pub fn get_start_vertex_rank(&self) -> PatternRankId {
        self.start_v_rank
    }

    #[inline]
    pub fn get_edge_label(&self) -> PatternLabelId {
        self.edge_label
    }

    #[inline]
    pub fn get_direction(&self) -> PatternDirection {
        self.dir
    }
}

#[derive(Debug, Clone)]
pub struct ExtendStep {
    target_v_label: PatternLabelId,
    /// Key: (start vertex label, start vertex rank), Value: Vec<extend edge>
    /// Extend edges are classified by their start_v_labels and start_v_indices
    extend_edges: BTreeMap<(PatternLabelId, PatternRankId), Vec<ExtendEdge>>,
}

/// Initializer of ExtendStep
impl ExtendStep {
    /// Initialization of a ExtendStep needs
    /// 1. a target vertex label
    /// 2. all extend edges connect to the target verex label
    pub fn new(target_v_label: PatternLabelId, extend_edges: Vec<ExtendEdge>) -> ExtendStep {
        let mut new_extend_step = ExtendStep { target_v_label, extend_edges: BTreeMap::new() };
        for edge in extend_edges {
            let edge_vec = new_extend_step
                .extend_edges
                .entry((edge.start_v_label, edge.start_v_rank))
                .or_insert(vec![]);
            edge_vec.push(edge);
        }
        new_extend_step
    }
}

/// Methods for access fileds or get info from ExtendStep
impl ExtendStep {
    /// For the iteration over all the extend edges with the classification by src vertex label and src vertex rank
    pub fn iter(&self) -> DynIter<(&(PatternLabelId, PatternRankId), &Vec<ExtendEdge>)> {
        Box::new(self.extend_edges.iter())
    }

    /// For the iteration over all the extend edges of ExtendStep
    pub fn extend_edges_iter(&self) -> DynIter<&ExtendEdge> {
        Box::new(
            self.extend_edges
                .iter()
                .flat_map(|(_, extend_edges)| extend_edges.iter()),
        )
    }

    #[inline]
    pub fn get_target_v_label(&self) -> PatternLabelId {
        self.target_v_label
    }

    /// Given a source vertex label and rank,
    /// check whether this ExtendStep contains a extend edge from this kind of vertex
    #[inline]
    pub fn has_extend_from_start_v(&self, v_label: PatternLabelId, v_rank: PatternRankId) -> bool {
        self.extend_edges
            .contains_key(&(v_label, v_rank))
    }

    /// Get how many different kind of start vertex this ExtendStep has
    #[inline]
    pub fn get_diff_start_v_num(&self) -> usize {
        self.extend_edges.len()
    }

    #[inline]
    pub fn get_extend_edges_num(&self) -> usize {
        let mut edges_num = 0;
        for (_, edges) in &self.extend_edges {
            edges_num += edges.len()
        }
        edges_num
    }

    /// Given a source vertex label and rank, find all extend edges connect to this kind of vertices
    pub fn get_extend_edges_by_start_v(
        &self, v_label: PatternLabelId, v_rank: PatternRankId,
    ) -> Option<&Vec<ExtendEdge>> {
        self.extend_edges.get(&(v_label, v_rank))
    }
}

/// Given a DefiniteExtendEdge, we can uniquely locate an edge with dir in the pattern
pub struct DefiniteExtendEdge {
    start_v_id: PatternId,
    edge_id: PatternId,
    edge_label: PatternLabelId,
    dir: PatternDirection,
}

/// Initializer of DefiniteExtendEdge
impl DefiniteExtendEdge {
    pub fn new(
        start_v_id: PatternId, edge_id: PatternId, edge_label: PatternLabelId, dir: PatternDirection,
    ) -> DefiniteExtendEdge {
        DefiniteExtendEdge { start_v_id, edge_id, edge_label, dir }
    }
}

/// Given a DefiniteExtendStep, we can uniquely find which part of the pattern to extend
pub struct DefiniteExtendStep {
    target_v_id: PatternId,
    target_v_label: PatternLabelId,
    extend_edges: Vec<DefiniteExtendEdge>,
}

/// Initializer of DefiniteExtendStep
impl DefiniteExtendStep {
    pub fn new(
        target_v_id: PatternId, target_v_label: PatternLabelId, extend_edges: Vec<DefiniteExtendEdge>,
    ) -> DefiniteExtendStep {
        DefiniteExtendStep { target_v_id, target_v_label, extend_edges }
    }
}

/// Transform a one-vertex pattern to DefiniteExtendStep
/// It is usually to use such DefiniteExtendStep to generate Source operator
impl TryFrom<Pattern> for DefiniteExtendStep {
    type Error = IrError;

    fn try_from(pattern: Pattern) -> IrResult<Self> {
        if pattern.get_vertices_num() == 1 {
            let target_vertex = pattern.vertices_iter().last().unwrap();
            let target_v_id = target_vertex.get_id();
            let target_v_label = target_vertex.get_label();
            Ok(DefiniteExtendStep { target_v_id, target_v_label, extend_edges: vec![] })
        } else {
            Err(IrError::Unsupported(
                "Can only convert pattern with one vertex to Definite Extend Step".to_string(),
            ))
        }
    }
}

/// Methods of accessing some fields of DefiniteExtendStep
impl DefiniteExtendStep {
    #[inline]
    pub fn get_target_v_id(&self) -> PatternId {
        self.target_v_id
    }

    #[inline]
    pub fn get_target_v_label(&self) -> PatternLabelId {
        self.target_v_label
    }
}

impl DefiniteExtendStep {
    /// Use the DefiniteExtendStep to generate corresponding edge expand operator
    pub fn generate_expand_operators(&self, origin_pattern: &Pattern) -> Vec<pb::EdgeExpand> {
        let mut expand_operators = vec![];
        let target_v_id = self.get_target_v_id();
        for extend_edge in self.extend_edges.iter() {
            // pick edge's property and predicate from origin pattern
            let edge_id = extend_edge.edge_id;
            let edge_predicate = origin_pattern
                .get_edge_predicate(edge_id)
                .cloned();
            let edge_expand = pb::EdgeExpand {
                // use start vertex id as tag
                v_tag: Some((extend_edge.start_v_id as i32).into()),
                direction: extend_edge.dir as i32,
                params: Some(query_params(vec![extend_edge.edge_label.into()], vec![], edge_predicate)),
                is_edge: false,
                // use target vertex id as alias
                alias: Some((target_v_id as i32).into()),
            };
            expand_operators.push(edge_expand);
        }
        expand_operators
    }

    /// Generate the intersect operator for DefiniteExtendStep;s target vertex
    /// It needs its parent EdgeExpand Operator's node ids
    pub fn generate_intersect_operator(&self, parents: Vec<i32>) -> pb::Intersect {
        pb::Intersect { parents, key: Some((self.target_v_id as i32).into()) }
    }

    /// Generate the filter operator for DefiniteExtendStep;s target vertex
    pub fn generate_vertex_filter_operator(&self, origin_pattern: &Pattern) -> pb::Select {
        // pick target vertex's property and predicate info from origin pattern
        let target_v_id = self.target_v_id;
        let target_v_predicate = origin_pattern
            .get_vertex_predicate(target_v_id)
            .cloned();
        pb::Select { predicate: target_v_predicate }
    }
}
/// Get all the subsets of given Vec<T>
/// The algorithm is BFS
pub fn get_subsets<T, F>(origin_vec: Vec<T>, filter: F) -> Vec<Vec<T>>
where
    T: Clone,
    F: Fn(&T, &Vec<T>) -> bool,
{
    let n = origin_vec.len();
    let mut set_collections = Vec::with_capacity((2 as usize).pow(n as u32));
    let mut queue = VecDeque::new();
    for (i, element) in origin_vec.iter().enumerate() {
        queue.push_back((vec![element.clone()], i + 1));
    }
    while let Some((subset, max_rank)) = queue.pop_front() {
        set_collections.push(subset.clone());
        for i in max_rank..n {
            let mut new_subset = subset.clone();
            if filter(&origin_vec[i], &subset) {
                continue;
            }
            new_subset.push(origin_vec[i].clone());
            queue.push_back((new_subset, i + 1));
        }
    }
    set_collections
}

pub(crate) fn limit_repeated_element_num<'a, T, U>(
    add_element: &'a U, subset_to_be_added: T, limit_num: usize,
) -> bool
where
    T: Iterator<Item = &'a U>,
    U: Eq,
{
    let mut repeaded_num = 0;
    for element in subset_to_be_added {
        if *add_element == *element {
            repeaded_num += 1;
            if repeaded_num >= limit_num {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use crate::catalogue::codec::*;
    use crate::catalogue::extend_step::*;
    use crate::catalogue::test_cases::extend_step_cases::*;
    use crate::catalogue::test_cases::pattern_cases::*;
    use crate::catalogue::test_cases::pattern_meta_cases::*;
    use crate::catalogue::PatternDirection;

    #[test]
    fn test_extend_step_case1_structure() {
        let extend_step1 = build_extend_step_case1();
        assert_eq!(extend_step1.target_v_label, 1);
        assert_eq!(extend_step1.extend_edges.len(), 1);
        assert_eq!(
            extend_step1
                .extend_edges
                .get(&(0, 0))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            extend_step1.extend_edges.get(&(0, 0)).unwrap()[0],
            ExtendEdge { start_v_label: 0, start_v_rank: 0, edge_label: 1, dir: PatternDirection::Out }
        );
        assert_eq!(
            extend_step1.extend_edges.get(&(0, 0)).unwrap()[1],
            ExtendEdge { start_v_label: 0, start_v_rank: 0, edge_label: 1, dir: PatternDirection::Out }
        );
    }

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
