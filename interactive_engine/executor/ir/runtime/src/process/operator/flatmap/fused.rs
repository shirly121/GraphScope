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

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use pegasus::api::function::{DynIter, FilterMapFunction, FlatMapFunction, FnResult};

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::process::operator::flatmap::FlatMapFuncGen;
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::{Entry, Record};

enum FusedFunc<E: Entry> {
    FilterMap(Box<dyn FilterMapFunction<Record<E>, Record<E>>>),
    FlatMap(Box<dyn FlatMapFunction<Record<E>, Record<E>, Target = DynIter<Record<E>>>>),
}

impl<E: Entry> FlatMapFunction<Record<E>, Record<E>> for FusedFunc<E> {
    type Target = DynIter<Record<E>>;

    fn exec(&self, input: Record<E>) -> FnResult<Self::Target> {
        let mut results = vec![];
        match self {
            FusedFunc::FilterMap(filter_map) => {
                if let Some(record) = filter_map.exec(input)? {
                    results.push(record);
                }
            }
            FusedFunc::FlatMap(flat_map) => {
                results.extend(flat_map.exec(input)?);
            }
        }

        Ok(Box::new(results.into_iter()))
    }
}

struct FusedOperator<E: Entry> {
    funcs: Vec<FusedFunc<E>>,
}

impl<E: Entry> FlatMapFunction<Record<E>, Record<E>> for FusedOperator<E> {
    type Target = DynIter<Record<E>>;

    fn exec(&self, input: Record<E>) -> FnResult<Self::Target> {
        let mut results: Vec<Record<E>> = vec![];
        let mut temp_container = vec![];
        if self.funcs.is_empty() {
            return Err(Box::new(FnExecError::unexpected_data_error("zero operator in `FusedOperator`")));
        }
        results.extend(self.funcs.first().unwrap().exec(input)?);
        for func in self.funcs.iter().skip(1) {
            for rec in results.drain(..) {
                temp_container.extend(func.exec(rec)?);
            }
            results.extend(temp_container.drain(..));
        }

        Ok(Box::new(results.into_iter()))
    }
}

impl<E: Entry> FlatMapFuncGen<E> for algebra_pb::FusedOperator {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record<E>, Record<E>, Target = DynIter<Record<E>>>>> {
        let mut funcs = vec![];
        for op in &self.oprs {
            let inner_op = op
                .opr
                .clone()
                .ok_or(ParsePbError::EmptyFieldError("Node::opr".to_string()))?;
            if let Ok(filter_map) = inner_op.clone().gen_filter_map() {
                funcs.push(FusedFunc::FilterMap(filter_map));
            } else if let Ok(flat_map) = inner_op.gen_flat_map() {
                funcs.push(FusedFunc::FlatMap(flat_map));
            } else {
                return Err(FnGenError::UnSupported(format!(
                    "the operator: {:?} cannot be fused as it is neither `FilterMap` or `FlatMap`",
                    op
                )));
            }
        }
        Ok(Box::new(FusedOperator { funcs }))
    }
}
