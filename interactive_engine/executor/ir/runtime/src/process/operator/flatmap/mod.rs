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
mod edge_expand;
mod fused;
mod get_v;
mod unfold;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use pegasus::api::function::{DynIter, FlatMapFunction};

use crate::error::FnGenResult;
use crate::process::record::{CompleteEntry, Entry, Record, SimpleEntry};

pub trait FlatMapFuncGen<E: Entry> {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record<E>, Record<E>, Target = DynIter<Record<E>>>>>;
}

impl FlatMapFuncGen<CompleteEntry> for algebra_pb::logical_plan::operator::Opr {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<
        Box<
            dyn FlatMapFunction<
                Record<CompleteEntry>,
                Record<CompleteEntry>,
                Target = DynIter<Record<CompleteEntry>>,
            >,
        >,
    > {
        match self {
            algebra_pb::logical_plan::operator::Opr::Edge(edge_expand) => edge_expand.gen_flat_map(),
            algebra_pb::logical_plan::operator::Opr::Vertex(get_vertex) => get_vertex.gen_flat_map(),
            algebra_pb::logical_plan::operator::Opr::Unfold(unfold) => unfold.gen_flat_map(),
            algebra_pb::logical_plan::operator::Opr::Fused(fused) => fused.gen_flat_map(),
            _ => Err(ParsePbError::ParseError(format!("the operator: {:?} is not a `FlatMap`", self)))?,
        }
    }
}

impl FlatMapFuncGen<SimpleEntry> for algebra_pb::logical_plan::operator::Opr {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<
        Box<
            dyn FlatMapFunction<
                Record<SimpleEntry>,
                Record<SimpleEntry>,
                Target = DynIter<Record<SimpleEntry>>,
            >,
        >,
    > {
        match self {
            algebra_pb::logical_plan::operator::Opr::Edge(edge_expand) => edge_expand.gen_flat_map(),
            // algebra_pb::logical_plan::operator::Opr::Unfold(unfold) => unfold.gen_flat_map(),
            _ => Err(ParsePbError::ParseError(format!("the operator: {:?} is not a `FlatMap`", self)))?,
        }
    }
}
