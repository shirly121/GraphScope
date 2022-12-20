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
//!

use crate::FACTORY;
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use ir_common::generated::results as result_pb;
use ir_common::KeyId;
use pegasus::result::{ResultSink, ResultStream};
use pegasus::{run_opt, JobConf};
use pegasus_server::job::{JobAssembly, JobDesc};
use pegasus_server::JobRequest;
use prost::Message;
use runtime::process::record::{Entry, Record};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

pub fn submit_query(job_req: JobRequest, conf: JobConf) -> Result<(), Box<dyn Error>> {
    let (tx, rx) = crossbeam_channel::unbounded();
    let sink = ResultSink::new(tx);
    let cancel_hook = sink.get_cancel_hook().clone();
    let mut results = ResultStream::new(conf.job_id, cancel_hook, rx);
    let service = &FACTORY;
    let job = JobDesc { input: job_req.source, plan: job_req.plan, resource: job_req.resource };
    let query_execution_start_time = Instant::now();
    run_opt(conf, sink, move |worker| service.assemble(&job, worker))?;
    while let Some(result) = results.next() {
        if let Ok(record_code) = result {
            if let Some(record) = parse_result(record_code) {
                println!("{:?}", record);
            }
        }
    }
    println!("executing query time cost is {:?} ms", query_execution_start_time.elapsed().as_millis());
    Ok(())
}

pub fn parse_result(result: Vec<u8>) -> Option<Record> {
    let result: result_pb::Results = result_pb::Results::decode(result.as_slice()).unwrap();
    if let Some(result_pb::results::Inner::Record(record_pb)) = result.inner {
        let mut record = Record::default();
        for column in record_pb.columns {
            let tag: Option<KeyId> = if let Some(tag) = column.name_or_id {
                match tag.item.unwrap() {
                    common_pb::name_or_id::Item::Name(name) => Some(
                        name.parse::<KeyId>()
                            .unwrap_or(KeyId::max_value()),
                    ),
                    common_pb::name_or_id::Item::Id(id) => Some(id),
                }
            } else {
                None
            };
            let entry = column.entry.unwrap();
            // append entry without moving head
            if let Some(tag) = tag {
                let columns = record.get_columns_mut();
                columns.insert(tag as usize, Arc::new(Entry::try_from(entry).unwrap()));
            } else {
                record.append(Entry::try_from(entry).unwrap(), None);
            }
        }
        Some(record)
    } else {
        None
    }
}

pub fn match_pb_plan_add_source(pb_plan: &mut pb::LogicalPlan) -> Option<()> {
    if let pb::logical_plan::operator::Opr::Select(first_select) = pb_plan
        .nodes
        .get(0)
        .unwrap()
        .opr
        .as_ref()
        .unwrap()
        .opr
        .as_ref()
        .unwrap()
        .clone()
    {
        let label_id = first_select
            .predicate
            .as_ref()
            .unwrap()
            .operators
            .get(2)
            .and_then(|opr| opr.item.as_ref())
            .and_then(
                |item| if let common_pb::expr_opr::Item::Const(value) = item { Some(value) } else { None },
            )
            .and_then(|value| {
                if let Some(common_pb::value::Item::I64(label_id)) = value.item {
                    Some(label_id as i32)
                } else if let Some(common_pb::value::Item::I32(label_id)) = value.item {
                    Some(label_id)
                } else {
                    None
                }
            })
            .unwrap();
        let source = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(pb::QueryParams {
                tables: vec![label_id.into()],
                columns: vec![],
                is_all_columns: false,
                limit: None,
                predicate: None,
                sample_ratio: 1.0,
                extra: HashMap::new(),
            }),
            idx_predicate: None,
        };
        pb_plan.nodes.remove(0);
        pb_plan
            .nodes
            .insert(0, pb::logical_plan::Node { opr: Some(source.into()), children: vec![1] });
        Some(())
    } else {
        None
    }
}

pub fn pb_plan_add_count_sink_operator(pb_plan: &mut pb::LogicalPlan) {
    let pb_plan_len = pb_plan.nodes.len();
    let count = pb::GroupBy {
        mappings: vec![],
        functions: vec![pb::group_by::AggFunc {
            vars: vec![],
            aggregate: 3, // count
            alias: Some(0.into()),
        }],
    };
    pb_plan
        .nodes
        .push(pb::logical_plan::Node { opr: Some(count.into()), children: vec![(pb_plan_len + 1) as i32] });
    let sink = pb::Sink {
        tags: vec![common_pb::NameOrIdKey { key: Some(0.into()) }],
        sink_target: Some(pb::sink::SinkTarget {
            inner: Some(pb::sink::sink_target::Inner::SinkDefault(pb::SinkDefault {
                id_name_mappings: vec![],
            })),
        }),
    };
    pb_plan
        .nodes
        .push(pb::logical_plan::Node { opr: Some(sink.into()), children: vec![] });
}

pub fn print_pb_logical_plan(pb_plan: &pb::LogicalPlan) {
    println!("pb logical plan:");
    let mut id = 0;
    pb_plan.nodes.iter().for_each(|node| {
        println!("ID: {:?}, {:?}", id, node);
        id += 1;
    });
    println!("Roots: {:?}\n", pb_plan.roots);
}

/// Split the logical plan for each intermediate stage
pub fn split_intermediate_pb_logical_plan(pb_plan: &pb::LogicalPlan) -> Vec<pb::LogicalPlan> {
    use pb::logical_plan::operator::Opr;
    let mut intermediate_pb_plans: Vec<pb::LogicalPlan> = vec![];
    let mut intermediate_pb_plan: pb::LogicalPlan = pb::LogicalPlan::default();
    intermediate_pb_plan.roots = vec![];
    let pb_plan_len = pb_plan.nodes.len();
    for node_idx in 0..pb_plan_len {
        let pb_node = &pb_plan.nodes[node_idx];
        let opr = pb_node
            .opr
            .as_ref()
            .unwrap()
            .opr
            .as_ref()
            .unwrap();
        match opr {
            Opr::Edge(_opr) => {
                intermediate_pb_plans.push(intermediate_pb_plan.clone());
            }
            Opr::ExpandIntersect(_opr) => {
                intermediate_pb_plans.push(intermediate_pb_plan.clone());
            }
            _ => {}
        };
        intermediate_pb_plan.nodes.push(pb_node.clone());
    }

    return intermediate_pb_plans;
}
