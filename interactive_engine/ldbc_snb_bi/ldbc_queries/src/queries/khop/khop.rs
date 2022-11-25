use graph_store::prelude::*;
use pegasus::api::{Count, Map, Sink};
use pegasus::result::ResultStream;
use pegasus::JobConf;

use crate::queries::graph::*;

/// g.V().hasLabel("PERSON").in("HASCREATOR").hasLabel("POST").as("a").in("REPLYOF").select("a").values("id").count()
/// the ID-Only Version.
pub fn khop(conf: JobConf) -> ResultStream<u64> {
    let workers = conf.workers;

    let schema = &GRAPH.graph_schema;
    let person_label = schema.get_vertex_label_id("PERSON").unwrap();
    let post_label = schema.get_vertex_label_id("POST").unwrap();
    let hascreator_label = schema.get_edge_label_id("HASCREATOR").unwrap();
    let replyof_label = schema.get_edge_label_id("REPLYOF").unwrap();

    pegasus::run(conf, || {
        move |input, output| {
            let worker_id = input.get_worker_index();
            let servers_len = pegasus::get_servers_len();
            let person_count = GRAPH
                .get_all_vertices(Some(&vec![person_label]))
                .count();
            let partial_count = person_count / workers as usize;
            let last_partial_count = person_count - partial_count * (workers as usize - 1);
            let all_persons = GRAPH
                .get_all_vertices(Some(&vec![person_label]))
                .map(|v| v.get_id() as u64);
            let last_worker_of_current_server = workers * (pegasus::get_current_worker().server_id + 1) - 1;
            let source = if worker_id == last_worker_of_current_server {
                all_persons
                    .skip((worker_id % workers) as usize * partial_count)
                    .take(last_partial_count)
            } else {
                all_persons
                    .skip((worker_id % workers) as usize * partial_count)
                    .take(partial_count)
            };

            // g.V().hasLabel("PERSON")
            let stream = input.input_from(source)?;
            stream
                // .in("HASCREATOR").hasLabel("POST")
                .flat_map(move |person_id: u64| {
                    let posts = GRAPH
                        .get_in_vertices(person_id as DefaultId, Some(&vec![hascreator_label]))
                        .filter(move |post_vertex| post_vertex.get_label()[0] == post_label)
                        .map(move |post_vertex| post_vertex.get_id() as u64);
                    Ok(posts)
                })?
                // .as("a").in("REPLYOF")
                .repartition(move |id| Ok(get_partition(id, workers as usize, servers_len)))
                .flat_map(move |post_id| {
                    let post_messages = GRAPH
                        .get_in_vertices(post_id as DefaultId, Some(&vec![replyof_label]))
                        .map(move |message_vertex| (post_id, message_vertex.get_id() as u64));
                    Ok(post_messages)
                })?
                //.select("a").values("id")
                .repartition(move |(post_id, _message_id)| {
                    Ok(get_partition(post_id, workers as usize, servers_len))
                })
                .map(|(post_id, _message_id)| {
                    let post_vertex = GRAPH.get_vertex(post_id as DefaultId).unwrap();
                    let post_property = post_vertex
                        .get_property("id")
                        .unwrap()
                        .as_i64()
                        .unwrap();
                    Ok(post_property)
                })?
                .count()?
                .sink_into(output)
        }
    })
    .expect("submit bi2 job failure")
}
