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

// A partition utility that one server contains multiple graph partitions for GRIN store.
use std::collections::HashMap;
use std::sync::Arc;

/// Starting gaia with GRIN will pre-allocate partitions for each process,
/// thus we use graph_partitioner together with partition_worker_mapping for data routing.
use grin::grin_v6d::*;

use crate::adapters::GrinGraphProxy;
use crate::apis::partitioner::{PartitionId, PartitionInfo, PartitionedData, ServerId};
use crate::GraphProxyError;
use crate::GraphProxyResult;

/// GrinPartition is a partitioner for GRIN store.
pub struct GrinPartition {
    grin_graph_proxy: Arc<GrinGraphProxy>,
    // mapping of partition id -> server_index
    partition_server_index_mapping: HashMap<GrinPartitionId, ServerId>,
}

unsafe impl Send for GrinPartition {}
unsafe impl Sync for GrinPartition {}

impl GrinPartition {
    pub fn new(
        grin_graph_proxy: Arc<GrinGraphProxy>,
        partition_server_index_mapping: HashMap<GrinPartitionId, u32>,
    ) -> Self {
        GrinPartition { grin_graph_proxy, partition_server_index_mapping }
    }

    fn get_partition_id_by_vertex_id(&self, vertex_id: i64) -> GraphProxyResult<GrinPartitionId> {
        self.grin_graph_proxy
            .get_partition_id_by_vertex_id(vertex_id)
    }
}

impl PartitionInfo for GrinPartition {
    fn get_partition_id<D: PartitionedData>(&self, data: &D) -> GraphProxyResult<PartitionId> {
        let partition_key_id = data.get_partition_key_id();
        let partition_id = self.get_partition_id_by_vertex_id(partition_key_id as i64)?;
        Ok(partition_id as PartitionId)
    }

    fn get_server_id(&self, partition_id: PartitionId) -> GraphProxyResult<ServerId> {
        let server_index = *self
            .partition_server_index_mapping
            .get(&partition_id)
            .ok_or(GraphProxyError::query_store_error(&format!(
                "get server id failed on GRIN , partition_id of {:?}",
                partition_id
            )))?;
        Ok(server_index)
    }
}
