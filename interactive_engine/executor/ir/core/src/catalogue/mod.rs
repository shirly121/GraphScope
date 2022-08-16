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

use std::collections::HashMap;

use graph_store::graph_db::Direction;
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use serde::{Deserialize, Serialize};

pub type PatternId = usize;
pub type PatternLabelId = ir_common::KeyId;
pub type PatternGroupId = u32;
pub type PatternRankId = usize;
pub type DynIter<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PatternDirection {
    Out = 0,
    In,
}

impl Into<u8> for PatternDirection {
    fn into(self) -> u8 {
        match self {
            PatternDirection::Out => 0,
            PatternDirection::In => 1,
        }
    }
}

impl Into<Direction> for PatternDirection {
    fn into(self) -> Direction {
        match self {
            PatternDirection::Out => Direction::Outgoing,
            PatternDirection::In => Direction::Incoming,
        }
    }
}

impl PatternDirection {
    pub fn reverse(&self) -> PatternDirection {
        match self {
            PatternDirection::Out => PatternDirection::In,
            PatternDirection::In => PatternDirection::Out,
        }
    }
}

pub(crate) fn query_params(
    tables: Vec<common_pb::NameOrId>, columns: Vec<common_pb::NameOrId>,
    predicate: Option<common_pb::Expression>,
) -> pb::QueryParams {
    pb::QueryParams {
        tables,
        columns,
        is_all_columns: false,
        limit: None,
        predicate,
        extra: HashMap::new(),
    }
}

#[allow(dead_code)]
pub mod catalog;

pub mod canonical_label;

pub mod extend_step;

pub mod pattern;

pub mod pattern_meta;

pub mod sample;

pub mod sparsify;
