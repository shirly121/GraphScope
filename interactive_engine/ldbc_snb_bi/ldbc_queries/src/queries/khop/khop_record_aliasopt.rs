use graph_store::prelude::*;
use pegasus::api::{Count, Map, Sink};
use pegasus::result::ResultStream;
use pegasus::JobConf;

use crate::queries::graph::*;
use graph_proxy::apis::{Details, GraphElement};
use graph_proxy::{to_empty_vertex, to_empty_vertex_with_label0, to_runtime_vertex};
use itertools::__std_iter::Iterator;
use runtime::process::record::Record;

/// g.V().hasLabel("PERSON").in("HASCREATOR").hasLabel("POST").as("a").in("REPLYOF").select("a").values("id").count()
/// the Record Version. Alias when the new `HEAD` entry generated.
pub fn khop_record_aliasopt(conf: JobConf) -> ResultStream<u64> {
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
                .map(|v| to_runtime_vertex(v, None))
                .map(|v| Record::new(v, None));
            let source = if worker_id == (workers - 1) {
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
                // .in("HASCREATOR").hasLabel("POST").as("a")
                .flat_map(move |person_record: Record| {
                    let person_id = person_record
                        .get(None)
                        .unwrap()
                        .as_graph_vertex()
                        .unwrap()
                        .id();
                    let posts = GRAPH
                        .get_in_vertices(person_id as DefaultId, Some(&vec![hascreator_label]))
                        .map(|v| to_empty_vertex_with_label0(v))
                        .filter(move |post_vertex| post_vertex.label().unwrap() == (post_label as i32))
                        .map(move |post_vertex| {
                            let mut new_record = person_record.clone();
                            new_record.append(post_vertex, None);
                            new_record
                        });
                    Ok(posts)
                })?
                // .as("a").in("REPLYOF")
                .repartition(move |r: &Record| {
                    Ok(get_record_curr_partition(r, workers as usize, servers_len))
                })
                .flat_map(move |post_record: Record| {
                    let post_id = post_record
                        .get(None)
                        .unwrap()
                        .as_graph_vertex()
                        .unwrap()
                        .id();
                    let post_messages = GRAPH
                        .get_in_vertices(post_id as DefaultId, Some(&vec![replyof_label]))
                        .map(move |message_vertex| to_empty_vertex(message_vertex))
                        .map(move |message_vertex| {
                            let mut new_record = post_record.clone();
                            new_record.append_with_head_alias(message_vertex, Some(0));
                            new_record
                        });
                    Ok(post_messages)
                })?
                //.select("a").values("id")
                .repartition(move |message_record: &Record| {
                    let entry = message_record.get(Some(0)).unwrap();
                    Ok(get_entry_partition(entry, workers as usize, servers_len))
                })
                // late project: fused of auxilia + project
                .map(|mut message_record: Record| {
                    let post_id = message_record
                        .get(Some(0))
                        .unwrap()
                        .as_graph_vertex()
                        .unwrap()
                        .id();
                    let post_vertex = GRAPH.get_vertex(post_id as DefaultId).unwrap();
                    let runtime_post_vertex = to_runtime_vertex(post_vertex, None);
                    let post_property = runtime_post_vertex
                        .details()
                        .unwrap()
                        .get_property(&"id".into())
                        .unwrap()
                        .try_to_owned()
                        .unwrap();
                    message_record.append(post_property, None);
                    Ok(message_record)
                })?
                .count()?
                .sink_into(output)
        }
    })
    .expect("submit bi2 job failure")
}
