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
use std::iter::Iterator;

use crate::catalogue::{DynIter, PatternDirection, PatternLabelId, PatternRankId};

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
