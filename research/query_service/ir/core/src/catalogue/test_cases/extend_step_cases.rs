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

use crate::catalogue::extend_step::*;
use crate::catalogue::PatternDirection;

/// The extend step looks like:
///         B
///       /   \
///      A     A
/// The left A has label id 0 and rankId 0
/// The right A also has label id 0 and rankId 0, the two A's are equivalent
/// The target vertex is B with label id 1
/// The two extend edges are both with edge id 1
/// pattern_case1 + extend_step_case1 = pattern_case2
pub fn build_extend_step_case1() -> ExtendStep {
    let extend_edge1 = ExtendEdge::new(0, 0, 1, PatternDirection::Out);
    let extend_edge2 = extend_edge1.clone();
    ExtendStep::new(1, vec![extend_edge1, extend_edge2])
}

/// The extend step looks like:
///       C
///    /  |   \
///  A(0) A(1) B
/// Vertex Label Map:
/// A: 1, B: 2, C: 3
/// Edge Label Map:
/// A->C: 1, B->C: 2
/// The left A has rankId 0 and the middle A has rankId 1
pub fn build_extend_step_case2() -> ExtendStep {
    let target_v_label = 3;
    let extend_edge_1 = ExtendEdge::new(1, 0, 1, PatternDirection::Out);
    let extend_edge_2 = ExtendEdge::new(1, 1, 1, PatternDirection::Out);
    let extend_edge_3 = ExtendEdge::new(2, 0, 2, PatternDirection::Out);
    ExtendStep::new(target_v_label, vec![extend_edge_1, extend_edge_2, extend_edge_3])
}

/// The extend step looks like:
/// Person -> knows -> Person
pub fn build_modern_extend_step_case1() -> ExtendStep {
    let target_v_label = 0;
    let extend_edge_1 = ExtendEdge::new(0, 0, 0, PatternDirection::Out);
    ExtendStep::new(target_v_label, vec![extend_edge_1])
}

/// The extend step looks like:
/// Person <- knows <- Person
pub fn build_modern_extend_step_case2() -> ExtendStep {
    let target_v_label = 0;
    let extend_edge_1 = ExtendEdge::new(0, 0, 0, PatternDirection::In);
    ExtendStep::new(target_v_label, vec![extend_edge_1])
}

/// The extend step looks like:
/// Person -> create -> Software
pub fn build_modern_extend_step_case3() -> ExtendStep {
    let target_v_label = 1;
    let extend_edge_1 = ExtendEdge::new(0, 0, 1, PatternDirection::Out);
    ExtendStep::new(target_v_label, vec![extend_edge_1])
}

/// The extend step looks like:
/// Software <- create <- Person
pub fn build_modern_extend_step_case4() -> ExtendStep {
    let target_v_label = 0;
    let extend_edge_1 = ExtendEdge::new(1, 0, 1, PatternDirection::In);
    ExtendStep::new(target_v_label, vec![extend_edge_1])
}

/// The extend step looks like:
///          Person
///       /        \
///   create       knows
///   /               \
/// Software        Person
pub fn build_modern_extend_step_case5() -> ExtendStep {
    let target_v_label = 0;
    let extend_edge_1 = ExtendEdge::new(1, 0, 1, PatternDirection::In);
    let extend_edge_2 = ExtendEdge::new(0, 0, 0, PatternDirection::Out);
    ExtendStep::new(target_v_label, vec![extend_edge_1, extend_edge_2])
}

/// The extend step looks like:
///          Software
///       /          \
///   create         create
///   /                 \
/// Person            Person
pub fn build_modern_extend_step_case6() -> ExtendStep {
    let target_v_label = 1;
    let extend_edge_1 = ExtendEdge::new(0, 0, 1, PatternDirection::Out);
    let extend_edge_2 = ExtendEdge::new(0, 1, 1, PatternDirection::Out);
    ExtendStep::new(target_v_label, vec![extend_edge_1, extend_edge_2])
}
