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

use crate::apis::{Partitioner, ID};
use crate::GraphProxyError;
use crate::GraphProxyResult;
use crate::GrinGraphProxy;

/// GrinPartition is a partitioner for GRIN store.
pub struct GrinPartition {
    grin_graph_proxy: Arc<GrinGraphProxy>,
    // mapping of partition id -> server_index
    partition_server_index_mapping: HashMap<GrinPartitionId, u32>,
    //`computed_process_partition_list` is the partition list that belongs to current GIE instance.
    // If one GIE instance corresponds to one vineyard instance, then computed_process_partition_list is the same as the local partition_list owned by PartitionGraph;
    // otherwise, we have pre-split the local partition_list owned by PartitionGraph into multiple partition_list, processed by multiple GIE instances.
    computed_process_partition_list: Vec<GrinPartitionId>,
}

unsafe impl Send for GrinPartition {}
unsafe impl Sync for GrinPartition {}

impl GrinPartition {
    pub fn new(
        grin_graph_proxy: Arc<GrinGraphProxy>,
        partition_server_index_mapping: HashMap<GrinPartitionId, u32>,
        computed_process_partition_list: Vec<GrinPartitionId>,
    ) -> Self {
        GrinPartition { grin_graph_proxy, partition_server_index_mapping, computed_process_partition_list }
    }

    fn get_partition_id_by_vertex_id(&self, vertex_id: i64) -> GraphProxyResult<GrinPartitionId> {
        self.grin_graph_proxy
            .get_partition_id_by_vertex_id(vertex_id)
    }
}

impl Partitioner for GrinPartition {
    fn get_partition(&self, id: &ID, worker_num_per_server: usize) -> GraphProxyResult<u64> {
        // The partitioning logics is as follows:
        // 1. `partition_id = self.graph_partition_manager.get_partition_id(*id as VertexId)` routes a given id
        // to the partition that holds its data.
        // 2. `server_index = partition_id % self.num_servers as u64` routes the partition id to the
        // server R that holds the partition
        // 3. `worker_index = partition_id % worker_num_per_server` picks up one worker to do the computation.
        // 4. `server_index * worker_num_per_server + worker_index` computes the worker index in server R
        // to do the computation.
        let vid = *id as i64;
        let worker_num_per_server = worker_num_per_server as u64;
        let partition_id = self.get_partition_id_by_vertex_id(vid)?;
        let server_index = *self
            .partition_server_index_mapping
            .get(&partition_id)
            .ok_or(GraphProxyError::query_store_error(&format!(
                "get server id failed on GRIN with vid of {:?}, partition_id of {:?}",
                vid, partition_id
            )))? as u64;
        let worker_index = partition_id as u64 % worker_num_per_server;
        Ok(server_index * worker_num_per_server + worker_index)
    }

    fn get_worker_partitions(
        &self, job_workers: usize, worker_id: u32,
    ) -> GraphProxyResult<Option<Vec<u64>>> {
        // Get worker partition list logic is as follows:
        // 1. `computed_process_partition_list` is the partition list that belongs to current process
        // 2. 'pid % job_workers' picks one worker to do the computation.
        // and 'pid % job_workers == worker_id % job_workers' checks if current worker is the picked worker
        let mut worker_partition_list = vec![];
        for pid in self.computed_process_partition_list.iter() {
            if pid % (job_workers as u32) == worker_id % (job_workers as u32) {
                worker_partition_list.push((*pid) as u64)
            }
        }
        info!(
            "job_workers {:?}, worker id: {:?},  worker_partition_list {:?}",
            job_workers, worker_id, worker_partition_list
        );
        Ok(Some(worker_partition_list))
    }
}
