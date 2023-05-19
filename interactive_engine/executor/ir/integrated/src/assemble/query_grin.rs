//
//! Copyright 2023 Alibaba Group Holding Limited.
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
use std::sync::Arc;

use graph_proxy::{create_grin_store, GrinGraphProxy, GrinPartition};
use grin::grin_v6d::*;
use runtime::IRJobAssembly;

use crate::InitializeJobAssembly;

pub struct QueryGrin {
    partitioned_graph: Arc<GrinGraphProxy>,
    partition_server_index_mapping: HashMap<GrinPartitionId, u32>,
    computed_process_partition_list: Vec<GrinPartitionId>,
}

#[allow(dead_code)]
impl QueryGrin {
    pub fn new(
        partitioned_graph: Arc<GrinGraphProxy>,
        partition_server_index_mapping: HashMap<GrinPartitionId, u32>,
        computed_process_partition_list: Vec<GrinPartitionId>,
    ) -> Self {
        Self { partitioned_graph, partition_server_index_mapping, computed_process_partition_list }
    }
}

/// Initialize GremlinJobCompiler for Grin
impl InitializeJobAssembly for QueryGrin {
    fn initialize_job_assembly(&self) -> IRJobAssembly {
        let partitioner = GrinPartition::new(
            self.partitioned_graph.clone(),
            self.partition_server_index_mapping.clone(),
            self.computed_process_partition_list.clone(),
        );
        create_grin_store(self.partitioned_graph.clone());
        IRJobAssembly::new(partitioner)
    }
}
