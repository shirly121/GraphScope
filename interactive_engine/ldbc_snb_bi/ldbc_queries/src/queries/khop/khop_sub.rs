use graph_store::common::LabelId;
use mcsr::columns::{Column, Int32Column};
use pegasus::api::{Count, Map, Sink};
use pegasus::result::ResultStream;
use pegasus::JobConf;

use crate::queries::graph::*;

pub fn khop_sub(conf: JobConf) -> ResultStream<u64> {
    let workers = conf.workers;

    pegasus::run(conf, || {
        move |input, output| {
            let worker_id = input.get_worker_index();
            let servers = pegasus::get_servers_len();
            let post_id_col = CSR.vertex_prop_table[3_usize]
                // a random property
                .get_column_by_name("length")
                .as_any()
                .downcast_ref::<Int32Column>()
                .unwrap();

            let person_count = CSR.get_vertices_num(1 as LabelId) as u64;
            let partial_count = person_count / (workers as u64);
            let last_partial_count = person_count - partial_count * (workers as u64 - 1);

            let last_worker_of_current_server = workers * (pegasus::get_current_worker().server_id + 1) - 1;
            let person_begin = ((worker_id % workers) as u64) * partial_count;
            let person_end = if worker_id == last_worker_of_current_server {
                person_begin + last_partial_count as u64
            } else {
                person_begin + partial_count as u64
            };

            let stream = input.input_from(person_begin..person_end)?;
            stream
                .flat_map(move |person_internal_id| {
                    let mut post_list = vec![];
                    if let Some(edges) = POST_HASCREATOR_PERSON_IN.get_adj_list(person_internal_id as usize)
                    {
                        for e in edges {
                            post_list.push(CSR.get_global_id(e.neighbor, 3).unwrap() as u64)
                        }
                    }
                    Ok(post_list.into_iter())
                })?
                .repartition(move |id| Ok(get_partition(id, workers as usize, servers)))
                .flat_map(|post_global_id| {
                    let mut post_comment_list = vec![];
                    let post_id = CSR.get_internal_id(post_global_id as usize);
                    if let Some(edges) = COMMENT_REPLYOF_POST_IN.get_adj_list(post_id) {
                        for e in edges {
                            post_comment_list
                                .push((post_global_id, CSR.get_global_id(e.neighbor, 2).unwrap() as u64))
                        }
                    }
                    Ok(post_comment_list.into_iter())
                })?
                .repartition(move |(post_global_id, _comment_global_id)| {
                    Ok(get_partition(post_global_id, workers as usize, servers))
                })
                .map(move |(post_global_id, _comment_global_id)| {
                    let post_id = CSR.get_internal_id(post_global_id as usize);
                    let post_property = post_id_col.get(post_id).unwrap().to_string();
                    Ok(post_property)
                })?
                .count()?
                .sink_into(output)
        }
    })
    .expect("submit bi9_sub job failure")
}
