//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::process::operator::flatmap::FlatMapFuncGen;
use crate::process::record::{Entry, Record, RecordExpandIter};
use graph_proxy::apis::{get_graph, Direction, GraphElement, QueryParams, Statement, Vertex, ID};
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::KeyId;
use pegasus::api::function::{DynIter, FlatMapFunction, FnResult};
use std::collections::HashSet;
use std::convert::TryInto;

struct ExpandAndIntersect<IE: Into<Entry>> {
    start_v_tags: Vec<KeyId>,
    end_v_tag: KeyId,
    stmts: Vec<Box<dyn Statement<ID, IE>>>,
    len: usize,
}

impl<IE: Into<Entry>> ExpandAndIntersect<IE> {
    fn init() -> Self {
        ExpandAndIntersect { start_v_tags: vec![], end_v_tag: 0, stmts: vec![], len: 0 }
    }
}

impl<IE: Into<Entry> + 'static> FlatMapFunction<Record, Record> for ExpandAndIntersect<IE> {
    type Target = DynIter<Record>;

    fn exec(&self, input: Record) -> FnResult<Self::Target> {
        let mut intersection = HashSet::new();
        let mut is_init = true;
        for i in 0..self.len {
            let start_tag = *self
                .start_v_tags
                .get(i)
                .ok_or(FnExecError::GetTagError("Missing Tag".to_string()))?;
            let stmt = self
                .stmts
                .get(i)
                .ok_or(FnExecError::GetTagError("Missing Statement".to_string()))?;
            let entry = input
                .get(Some(start_tag))
                .ok_or(FnExecError::get_tag_error(&format!(
                    "start_v_tag {:?} in ExpandOrIntersect",
                    start_tag
                )))?;
            let vertex = entry
                .as_graph_vertex()
                .ok_or(FnExecError::UnSupported("Expand and Intersect Only Support Vertex".to_string()))?;
            let v_id = vertex.id();
            let adjacent_vertices_set: HashSet<Vertex> = stmt
                .exec(v_id)?
                .map(|e| {
                    let entry: Entry = e.into();
                    if let Some(v) = entry.as_graph_vertex() {
                        v.clone()
                    } else {
                        unreachable!()
                    }
                })
                .collect();
            if is_init {
                intersection = adjacent_vertices_set;
                is_init = false;
            } else {
                intersection = intersection
                    .intersection(&adjacent_vertices_set)
                    .cloned()
                    .collect();
            }
        }
        Ok(Box::new(RecordExpandIter::new(
            input,
            Some(&self.end_v_tag),
            Box::new(intersection.into_iter()),
        )))
    }
}

impl FlatMapFuncGen for algebra_pb::ExpandAndIntersect {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>> {
        let graph = get_graph().ok_or(FnGenError::NullGraphError)?;
        let mut expand_and_intersect = ExpandAndIntersect::init();
        for (i, edge_expand) in self.edge_expands.into_iter().enumerate() {
            let start_v_tag = edge_expand
                .v_tag
                .ok_or(ParsePbError::from("`EdgeExpand::v_tag` cannot be empty for intersection"))?
                .try_into()?;
            let end_v_tag = edge_expand
                .alias
                .ok_or(ParsePbError::from("`EdgeExpand::alias` cannot be empty for intersection"))?
                .try_into()?;
            if i == 0 {
                expand_and_intersect.end_v_tag = end_v_tag;
            } else {
                if expand_and_intersect.end_v_tag != end_v_tag {
                    return Err(FnGenError::UnSupported("Expand to different end vertex tags".to_string()));
                }
            }
            let direction_pb: algebra_pb::edge_expand::Direction =
                unsafe { ::std::mem::transmute(edge_expand.direction) };
            let direction = Direction::from(direction_pb);
            let query_params: QueryParams = edge_expand.params.try_into()?;
            debug!(
                "Runtime expand collection operator of edge with start_v_tag {:?}, edge_tag {:?}, direction {:?}, query_params {:?}",
                start_v_tag, end_v_tag, direction, query_params
            );
            if edge_expand.expand_opt != algebra_pb::edge_expand::ExpandOpt::Vertex as i32 {
                return Err(FnGenError::unsupported_error("expand edges in ExpandIntersection"));
            }
            if query_params.filter.is_some() {
                // Expand vertices with filters on edges.
                // This can be regarded as a combination of EdgeExpand (with expand_opt as Edge) + GetV
                // ToDo: support it in SimpleEntry
                return Err(FnGenError::unsupported_error("expand vertices with edge filters"));
            }
            let stmt = graph.prepare_explore_vertex(direction, &query_params)?;
            expand_and_intersect
                .start_v_tags
                .push(start_v_tag);
            expand_and_intersect.stmts.push(stmt);
            expand_and_intersect.len += 1;
        }
        Ok(Box::new(expand_and_intersect))
    }
}
