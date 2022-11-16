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

use std::cmp::Ordering;
use std::collections::HashSet;
use std::convert::{TryFrom, TryInto};

use ir_common::expr_parse::str_to_expr_pb;
use ir_common::generated::algebra as pb;
use petgraph::graph::NodeIndex;

use crate::catalogue::catalog::{Approach, Catalogue, ExtendWeight, PatternWeight};
use crate::catalogue::extend_step::DefiniteExtendStep;
use crate::catalogue::pattern::Pattern;
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::PatternDirection;
use crate::catalogue::PatternId;
use crate::error::{IrError, IrResult};

static ALPHA: f64 = 0.5;
static BETA: f64 = 0.5;

/// Methods for Pattern to generate pb Logical plan of pattern matching
impl Pattern {
    /// Generate a naive extend based pattern match plan
    pub fn generate_simple_extend_match_plan(
        &self, pattern_meta: &PatternMeta, is_distributed: bool,
    ) -> IrResult<pb::LogicalPlan> {
        let mut trace_pattern = self.clone();
        let mut definite_extend_steps = vec![];
        while trace_pattern.get_vertices_num() > 1 {
            let mut all_vertex_ids: Vec<PatternId> = trace_pattern
                .vertices_iter()
                .map(|v| v.get_id())
                .collect();
            sort_vertex_ids(&mut all_vertex_ids, &trace_pattern);
            let select_vertex_id = *all_vertex_ids.first().unwrap();
            let definite_extend_step =
                DefiniteExtendStep::from_target_pattern(&trace_pattern, select_vertex_id).unwrap();
            definite_extend_steps.push(definite_extend_step);
            trace_pattern.remove_vertex(select_vertex_id);
        }
        definite_extend_steps.push(trace_pattern.try_into()?);
        if is_distributed {
            build_distributed_match_plan(self, definite_extend_steps, pattern_meta)
        } else {
            build_stand_alone_match_plan(self, definite_extend_steps, pattern_meta)
        }
    }

    /// Generate an optimized extend based pattern match plan
    /// Current implementation is Top-Down Greedy method
    pub fn generate_optimized_match_plan_greedily(
        &self, catalog: &Catalogue, pattern_meta: &PatternMeta, is_distributed: bool,
    ) -> IrResult<pb::LogicalPlan> {
        let pattern_code = self.encode_to();
        // locate the pattern node in the catalog graph
        if let Some(node_index) = catalog.get_pattern_index(&pattern_code) {
            let mut trace_pattern = self.clone();
            let mut trace_pattern_index = node_index;
            let mut trace_pattern_weight = catalog
                .get_pattern_weight(trace_pattern_index)
                .unwrap();
            let mut definite_extend_steps = vec![];
            while trace_pattern.get_vertices_num() > 1 {
                let mut all_extend_approaches: Vec<Approach> = catalog
                    .pattern_in_approaches_iter(trace_pattern_index)
                    .filter(|approach| {
                        catalog
                            .get_approach_weight(approach.get_approach_index())
                            .unwrap()
                            .is_extend()
                    })
                    .collect();
                // use the extend step with the lowerst estimated cost
                sort_extend_approaches(
                    &mut all_extend_approaches,
                    catalog,
                    &trace_pattern,
                    trace_pattern_weight,
                );
                let selected_approach = all_extend_approaches[0];
                let (pre_pattern, definite_extend_step, _) =
                    pattern_roll_back(trace_pattern, trace_pattern_index, selected_approach, catalog);
                definite_extend_steps.push(definite_extend_step);
                trace_pattern = pre_pattern;
                trace_pattern_index = selected_approach.get_src_pattern_index();
                trace_pattern_weight = catalog
                    .get_pattern_weight(trace_pattern_index)
                    .unwrap();
            }
            // transform the one-vertex pattern into definite extend step
            definite_extend_steps.push(trace_pattern.try_into()?);
            if is_distributed {
                build_distributed_match_plan(self, definite_extend_steps, pattern_meta)
            } else {
                build_stand_alone_match_plan(self, definite_extend_steps, pattern_meta)
            }
        } else {
            Err(IrError::Unsupported("Cannot Locate Pattern in the Catalogue".to_string()))
        }
    }

    pub fn generate_optimized_match_plan_recursively(
        &self, catalog: &mut Catalogue, pattern_meta: &PatternMeta, is_distributed: bool,
    ) -> IrResult<pb::LogicalPlan> {
        let pattern_code = self.encode_to();
        if let Some(pattern_index) = catalog.get_pattern_index(&pattern_code) {
            // let mut memory_map = HashMap::new();
            let (mut definite_extend_steps, _) =
                get_definite_extend_steps_recursively(catalog, pattern_index, self.clone());
            definite_extend_steps.reverse();
            if is_distributed {
                build_distributed_match_plan(self, definite_extend_steps, pattern_meta)
            } else {
                build_stand_alone_match_plan(self, definite_extend_steps, pattern_meta)
            }
        } else {
            Err(IrError::Unsupported("Cannot Locate Pattern in the Catalogue".to_string()))
        }
    }
}

pub fn get_definite_extend_steps_recursively(
    catalog: &mut Catalogue, pattern_index: NodeIndex, pattern: Pattern,
) -> (Vec<DefiniteExtendStep>, usize) {
    let pattern_weight = catalog
        .get_pattern_weight(pattern_index)
        .unwrap();
    if pattern.get_vertices_num() == 1 {
        let src_definite_extend_step = DefiniteExtendStep::try_from(pattern).unwrap();
        let cost = pattern_weight.get_count();
        return (vec![src_definite_extend_step], cost);
    } else if let Some(best_approach) = pattern_weight.get_best_approach() {
        let (pre_pattern, definite_extend_step, this_step_cost) =
            pattern_roll_back(pattern, pattern_index, best_approach, catalog);
        let pre_pattern_index = best_approach.get_src_pattern_index();
        let (mut definite_extend_steps, mut cost) =
            get_definite_extend_steps_recursively(catalog, pre_pattern_index, pre_pattern);
        definite_extend_steps.push(definite_extend_step);
        cost += this_step_cost;
        return (definite_extend_steps, cost);
    } else {
        let mut definite_extend_steps_with_min_cost = vec![];
        let mut min_cost = usize::MAX;
        let approaches: Vec<Approach> = catalog
            .pattern_in_approaches_iter(pattern_index)
            .collect();
        let mut best_approach = approaches[0];
        for approach in approaches {
            let (pre_pattern, definite_extend_step, this_step_cost) =
                pattern_roll_back(pattern.clone(), pattern_index, approach, catalog);
            let pre_pattern_index = approach.get_src_pattern_index();
            let (mut definite_extend_steps, mut cost) =
                get_definite_extend_steps_recursively(catalog, pre_pattern_index, pre_pattern);
            definite_extend_steps.push(definite_extend_step);
            cost += this_step_cost;
            if cost < min_cost {
                definite_extend_steps_with_min_cost = definite_extend_steps;
                min_cost = cost;
                best_approach = approach;
            }
        }
        catalog.set_pattern_best_approach(pattern_index, best_approach);
        return (definite_extend_steps_with_min_cost, min_cost);
    }
}

fn pattern_roll_back(
    pattern: Pattern, pattern_index: NodeIndex, approach: Approach, catalog: &Catalogue,
) -> (Pattern, DefiniteExtendStep, usize) {
    let pattern_weight = catalog
        .get_pattern_weight(pattern_index)
        .unwrap();
    let extend_weight = catalog
        .get_extend_weight(approach.get_approach_index())
        .unwrap();
    let pre_pattern_index = approach.get_src_pattern_index();
    let pre_pattern_weight = catalog
        .get_pattern_weight(pre_pattern_index)
        .unwrap();
    let this_step_cost = extend_cost_estimate(pre_pattern_weight, pattern_weight, extend_weight);
    let target_vertex_id = pattern
        .get_vertex_from_rank(extend_weight.get_target_vertex_rank())
        .unwrap()
        .get_id();
    let extend_step = extend_weight.get_extend_step();
    let edge_id_map = pattern
        .adjacencies_iter(target_vertex_id)
        .map(|adjacency| {
            (
                (
                    adjacency.get_adj_vertex().get_id(),
                    adjacency.get_edge_label(),
                    adjacency.get_direction().reverse(),
                ),
                adjacency.get_edge_id(),
            )
        })
        .collect();
    let mut pre_pattern = pattern;
    pre_pattern.remove_vertex(target_vertex_id);
    let definite_extend_step =
        DefiniteExtendStep::from_src_pattern(&pre_pattern, &extend_step, target_vertex_id, edge_id_map)
            .unwrap();
    (pre_pattern, definite_extend_step, this_step_cost)
}

fn vertex_has_predicate(pattern: &Pattern, vertex_id: PatternId) -> bool {
    pattern
        .get_vertex_predicate(vertex_id)
        .is_some()
}

fn get_adj_edges_filter_num(pattern: &Pattern, vertex_id: PatternId) -> usize {
    pattern
        .adjacencies_iter(vertex_id)
        .filter(|adj| {
            pattern
                .get_edge_predicate(adj.get_edge_id())
                .is_some()
        })
        .count()
}

fn sort_vertex_ids(vertex_ids: &mut Vec<PatternId>, pattern: &Pattern) {
    vertex_ids.sort_by(|&v1_id, &v2_id| {
        // compare v1 and v2's vertex predicate
        let v1_has_predicate = vertex_has_predicate(pattern, v1_id);
        let v2_has_predicate = vertex_has_predicate(pattern, v2_id);
        // compare v1 and v2's adjacent edges' predicate num
        let v1_edges_predicate_num = get_adj_edges_filter_num(pattern, v1_id);
        let v2_edges_predicate_num = get_adj_edges_filter_num(pattern, v2_id);
        // compare v1 and v2's degree
        let v1_degree = pattern.get_vertex_degree(v1_id);
        let v2_degree = pattern.get_vertex_degree(v2_id);
        // compare v1 and v2's out degree
        let v1_out_degree = pattern.get_vertex_out_degree(v1_id);
        let v2_out_degree = pattern.get_vertex_out_degree(v2_id);
        (v1_has_predicate, v1_edges_predicate_num, v1_degree, v1_out_degree).cmp(&(
            v2_has_predicate,
            v2_edges_predicate_num,
            v2_degree,
            v2_out_degree,
        ))
    });
}

fn sort_extend_approaches(
    approaches: &mut Vec<Approach>, catalog: &Catalogue, pattern: &Pattern, pattern_weight: &PatternWeight,
) {
    approaches.sort_by(|approach1, approach2| {
        let extend_weight1 = catalog
            .get_extend_weight(approach1.get_approach_index())
            .unwrap();
        let extend_weight2 = catalog
            .get_extend_weight(approach2.get_approach_index())
            .unwrap();
        let target_vertex1_has_predicate = pattern
            .get_vertex_predicate(
                pattern
                    .get_vertex_from_rank(extend_weight1.get_target_vertex_rank())
                    .unwrap()
                    .get_id(),
            )
            .is_some();
        let target_vertex2_has_predicate = pattern
            .get_vertex_predicate(
                pattern
                    .get_vertex_from_rank(extend_weight2.get_target_vertex_rank())
                    .unwrap()
                    .get_id(),
            )
            .is_some();
        if target_vertex1_has_predicate && !target_vertex2_has_predicate {
            return Ordering::Greater;
        } else if !target_vertex1_has_predicate && target_vertex2_has_predicate {
            return Ordering::Less;
        }
        let pre_pattern_weight1 = catalog
            .get_pattern_weight(approach1.get_src_pattern_index())
            .unwrap();
        let pre_pattern_weight2 = catalog
            .get_pattern_weight(approach2.get_src_pattern_index())
            .unwrap();
        extend_cost_estimate(pre_pattern_weight1, pattern_weight, extend_weight1)
            .cmp(&extend_cost_estimate(pre_pattern_weight2, pattern_weight, extend_weight2))
    });
}

/// Build logical plan for extend based pattern match plan
///             source
///           /   |    \
///           \   |    /
///            intersect
fn build_distributed_match_plan(
    origin_pattern: &Pattern, mut definite_extend_steps: Vec<DefiniteExtendStep>,
    pattern_meta: &PatternMeta,
) -> IrResult<pb::LogicalPlan> {
    let mut match_plan = pb::LogicalPlan::default();
    let mut child_offset = 1;
    let as_opr = generate_add_start_nodes(
        &mut match_plan,
        origin_pattern,
        &mut definite_extend_steps,
        &mut child_offset,
    )?;
    // pre_node will have some children, need a subplan
    let mut pre_node = pb::logical_plan::Node { opr: Some(as_opr.into()), children: vec![] };
    for definite_extend_step in definite_extend_steps.into_iter().rev() {
        let edge_expands = definite_extend_step.generate_expand_operators(origin_pattern);
        let edge_expands_num = edge_expands.len();
        let edge_expands_ids: Vec<i32> = (0..edge_expands_num as i32)
            .map(|i| i + child_offset)
            .collect();
        for &i in edge_expands_ids.iter() {
            pre_node.children.push(i);
        }
        match_plan.nodes.push(pre_node);
        // if edge expand num > 1, we need a Intersect Operator
        if edge_expands_num > 1 {
            for edge_expand in edge_expands {
                let node = pb::logical_plan::Node {
                    opr: Some(edge_expand.into()),
                    children: vec![child_offset + edge_expands_num as i32],
                };
                match_plan.nodes.push(node);
            }
            let intersect = definite_extend_step.generate_intersect_operator(edge_expands_ids);
            pre_node = pb::logical_plan::Node { opr: Some(intersect.into()), children: vec![] };
            child_offset += (edge_expands_num + 1) as i32;
        } else if edge_expands_num == 1 {
            let edge_expand = edge_expands.into_iter().last().unwrap();
            pre_node = pb::logical_plan::Node { opr: Some(edge_expand.into()), children: vec![] };
            child_offset += 1;
        } else {
            return Err(IrError::InvalidPattern(
                "Build logical plan error: extend step is not source but has 0 edges".to_string(),
            ));
        }
        if check_target_vertex_label_num(&definite_extend_step, pattern_meta) > 1 {
            let target_vertex_label = definite_extend_step.get_target_vertex_label();
            let label_filter = pb::Select {
                predicate: Some(str_to_expr_pb(format!("@.~label == {}", target_vertex_label)).unwrap()),
            };
            let label_filter_id = child_offset;
            pre_node.children.push(label_filter_id);
            match_plan.nodes.push(pre_node);
            pre_node = pb::logical_plan::Node { opr: Some(label_filter.into()), children: vec![] };
            child_offset += 1;
        }
        if let Some(filter) = definite_extend_step.generate_vertex_filter_operator(origin_pattern) {
            let filter_id = child_offset;
            pre_node.children.push(filter_id);
            match_plan.nodes.push(pre_node);
            pre_node = pb::logical_plan::Node { opr: Some(filter.into()), children: vec![] };
            child_offset += 1;
        }
    }
    pre_node.children.push(child_offset);
    match_plan.nodes.push(pre_node);
    Ok(match_plan)
}

fn build_stand_alone_match_plan(
    origin_pattern: &Pattern, mut definite_extend_steps: Vec<DefiniteExtendStep>,
    pattern_meta: &PatternMeta,
) -> IrResult<pb::LogicalPlan> {
    let mut match_plan = pb::LogicalPlan::default();
    let mut child_offset = 1;
    let as_opr = generate_add_start_nodes(
        &mut match_plan,
        origin_pattern,
        &mut definite_extend_steps,
        &mut child_offset,
    )?;
    match_plan
        .nodes
        .push(pb::logical_plan::Node { opr: Some(as_opr.into()), children: vec![child_offset] });
    child_offset += 1;
    for definite_extend_step in definite_extend_steps.into_iter().rev() {
        let mut edge_expands = definite_extend_step.generate_expand_operators(origin_pattern);
        if edge_expands.len() == 0 {
            return Err(IrError::InvalidPattern(
                "Build logical plan error: extend step is not source but has 0 edges".to_string(),
            ));
        } else if edge_expands.len() == 1 {
            match_plan.nodes.push(pb::logical_plan::Node {
                opr: Some(edge_expands.remove(0).into()),
                children: vec![child_offset],
            });
            child_offset += 1;
        } else {
            let expand_intersect_opr = pb::ExpandAndIntersect { edge_expands };
            match_plan.nodes.push(pb::logical_plan::Node {
                opr: Some(expand_intersect_opr.into()),
                children: vec![child_offset],
            });
            child_offset += 1;
        }

        if check_target_vertex_label_num(&definite_extend_step, pattern_meta) > 1 {
            let target_vertex_label = definite_extend_step.get_target_vertex_label();
            let label_filter = pb::Select {
                predicate: Some(str_to_expr_pb(format!("@.~label == {}", target_vertex_label)).unwrap()),
            };
            match_plan.nodes.push(pb::logical_plan::Node {
                opr: Some(label_filter.into()),
                children: vec![child_offset],
            });
            child_offset += 1;
        }
        if let Some(filter) = definite_extend_step.generate_vertex_filter_operator(origin_pattern) {
            match_plan
                .nodes
                .push(pb::logical_plan::Node { opr: Some(filter.into()), children: vec![child_offset] });
            child_offset += 1;
        }
    }
    Ok(match_plan)
}

/// Cost estimation functions
fn extend_cost_estimate(
    pre_pattern_weight: &PatternWeight, pattern_weight: &PatternWeight, extend_weight: &ExtendWeight,
) -> usize {
    pre_pattern_weight.get_count()
        + pattern_weight.get_count()
        + ((extend_weight.get_adjacency_count() as f64) * ALPHA) as usize
        + ((extend_weight.get_intersect_count() as f64) * BETA) as usize
}

fn generate_add_start_nodes(
    match_plan: &mut pb::LogicalPlan, origin_pattern: &Pattern,
    definite_extend_steps: &mut Vec<DefiniteExtendStep>, child_offset: &mut i32,
) -> IrResult<pb::As> {
    let source_extend = definite_extend_steps
        .pop()
        .ok_or(IrError::InvalidPattern("Build logical plan error: from empty extend steps!".to_string()))?;
    let source_vertex_label = source_extend.get_target_vertex_label();
    let source_vertex_id = source_extend.get_target_vertex_id();
    let label_select = pb::Select {
        predicate: Some(str_to_expr_pb(format!("@.~label == {}", source_vertex_label)).unwrap()),
    };
    match_plan
        .nodes
        .push(pb::logical_plan::Node { opr: Some(label_select.into()), children: vec![*child_offset] });
    *child_offset += 1;
    if let Some(filter) = source_extend.generate_vertex_filter_operator(origin_pattern) {
        match_plan
            .nodes
            .push(pb::logical_plan::Node { opr: Some(filter.into()), children: vec![*child_offset] });
        *child_offset += 1;
    }
    let as_opr = pb::As { alias: Some((source_vertex_id as i32).into()) };
    Ok(as_opr)
}

fn check_target_vertex_label_num(extend_step: &DefiniteExtendStep, pattern_meta: &PatternMeta) -> usize {
    let mut target_vertex_labels = HashSet::new();
    for (i, extend_edge) in extend_step.iter().enumerate() {
        let mut target_vertex_label_candis = HashSet::new();
        let src_vertex_label = extend_edge.get_src_vertex().get_label();
        let edge_label = extend_edge.get_edge_label();
        let dir = extend_edge.get_direction();
        for (start_vertex_label, end_vertex_label) in
            pattern_meta.associated_vlabels_iter_by_elabel(edge_label)
        {
            if dir == PatternDirection::Out && src_vertex_label == start_vertex_label {
                target_vertex_label_candis.insert(end_vertex_label);
            } else if dir == PatternDirection::In && src_vertex_label == end_vertex_label {
                target_vertex_label_candis.insert(start_vertex_label);
            }
        }
        if i == 0 {
            target_vertex_labels = target_vertex_label_candis;
        } else {
            target_vertex_labels = target_vertex_labels
                .intersection(&target_vertex_label_candis)
                .cloned()
                .collect();
        }
    }
    target_vertex_labels.len()
}
