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

use std::collections::VecDeque;
use std::convert::TryFrom;
use std::iter::Iterator;

use ir_common::generated::algebra as pb;

use crate::catalogue::pattern::Pattern;
use crate::catalogue::{query_params, DynIter, PatternDirection, PatternId, PatternLabelId, PatternRankId};
use crate::error::{IrError, IrResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExtendEdge {
    src_vertex_rank: PatternRankId,
    edge_label: PatternLabelId,
    dir: PatternDirection,
}

/// Initializer of ExtendEdge
impl ExtendEdge {
    pub fn new(src_vertex_rank: usize, edge_label: PatternLabelId, dir: PatternDirection) -> ExtendEdge {
        ExtendEdge { src_vertex_rank, edge_label, dir }
    }
}

/// Methods for access fields of VagueExtendEdge
impl ExtendEdge {
    #[inline]
    pub fn get_src_vertex_rank(&self) -> PatternRankId {
        self.src_vertex_rank
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
    target_vertex_label: PatternLabelId,
    extend_edges: Vec<ExtendEdge>,
}

/// Initializer of ExtendStep
impl ExtendStep {
    /// Initialization of a ExtendStep needs
    /// 1. a target vertex label
    /// 2. all extend edges connect to the target verex label
    pub fn new(target_vertex_label: PatternLabelId, extend_edges: Vec<ExtendEdge>) -> ExtendStep {
        ExtendStep { target_vertex_label, extend_edges }
    }
}

/// Methods for access fileds or get info from ExtendStep
impl ExtendStep {
    /// For the iteration over all the extend edges
    pub fn iter(&self) -> DynIter<&ExtendEdge> {
        Box::new(self.extend_edges.iter())
    }

    #[inline]
    pub fn get_target_vertex_label(&self) -> PatternLabelId {
        self.target_vertex_label
    }

    #[inline]
    pub fn get_extend_edges_num(&self) -> usize {
        self.extend_edges.len()
    }
}

/// Given a DefiniteExtendEdge, we can uniquely locate an edge with dir in the pattern
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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
    while let Some((subset, max_index)) = queue.pop_front() {
        set_collections.push(subset.clone());
        for i in max_index..n {
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
    use crate::catalogue::extend_step::*;
    use crate::catalogue::PatternDirection;

    #[test]
    fn test_extend_step_case1_structure() {
        let extend_edge1 = ExtendEdge::new(0, 1, PatternDirection::Out);
        let extend_edge2 = ExtendEdge::new(1, 1, PatternDirection::Out);
        let extend_step1 = ExtendStep::new(1, vec![extend_edge1, extend_edge2]);
        assert_eq!(extend_step1.get_target_vertex_label(), 1);
        assert_eq!(extend_step1.extend_edges.len(), 2);
    }
}
