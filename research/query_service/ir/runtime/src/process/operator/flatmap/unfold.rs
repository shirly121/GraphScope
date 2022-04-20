//
//! Copyright 2022 Alibaba Group Holding Limited.
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

use std::convert::TryInto;

use ir_common::generated::algebra as algebra_pb;
use ir_common::KeyId;
use pegasus::api::function::{DynIter, FlatMapFunction, FnResult};

use crate::error::{FnExecError, FnGenResult};
use crate::process::operator::flatmap::FlatMapFuncGen;
use crate::process::record::{Record, RecordExpandIter};

#[derive(Debug)]
pub struct UnfoldOperator {
    tag: Option<KeyId>,
    alias: Option<KeyId>,
}

impl FlatMapFunction<Record, Record> for UnfoldOperator {
    type Target = DynIter<Record>;

    fn exec(&self, mut input: Record) -> FnResult<Self::Target> {
        let entry = input
            .take(self.tag.as_ref())
            .ok_or(FnExecError::get_tag_error("get start_v failed"))?;
        if let Some(collection) = entry.as_collection() {
            Ok(Box::new(RecordExpandIter::new(
                input,
                self.alias.as_ref(),
                Box::new(collection.clone().into_iter()),
            )))
        } else if let Some(graph_path) = entry.as_graph_path() {
            let path_end = graph_path
                .clone()
                .take_path()
                .ok_or(FnExecError::unexpected_data_error("Get path failed in UnfoldOperator"))?;
            Ok(Box::new(RecordExpandIter::new(input, self.alias.as_ref(), Box::new(path_end.into_iter()))))
        } else {
            Err(FnExecError::unexpected_data_error(&format!(
                "Cannot Expand from current entry {:?}",
                entry
            )))?
        }
    }
}

impl FlatMapFuncGen for algebra_pb::Unfold {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>> {
        let tag = self.tag.map(|tag| tag.try_into()).transpose()?;
        let alias = self
            .alias
            .map(|alias| alias.try_into())
            .transpose()?;
        let unfold_operator = UnfoldOperator { tag, alias };
        debug!("Runtime unfold operator {:?}", unfold_operator);
        Ok(Box::new(unfold_operator))
    }
}
