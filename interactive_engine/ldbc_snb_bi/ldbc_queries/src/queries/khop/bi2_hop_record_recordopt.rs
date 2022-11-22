use graph_store::prelude::*;
use itertools::__std_iter::Iterator;
use pegasus::api::{Count, Map, Sink};
use pegasus::result::ResultStream;
use pegasus::JobConf;
use runtime::process::record::Record;

use crate::queries::graph::*;

fn to_id_only_vertex(vertex: LocalVertex<DefaultId>) -> u64 {
    vertex.get_id() as u64
}

/// this is the handwritten + record + filter_push_down + recordopt version of partial bi2, with only two-hops expxansion by tag_class -- tag -- message;
pub fn bi2_hop_record_recordopt(conf: JobConf, date: String, tag_class: String) -> ResultStream<u64> {
    let workers = conf.workers;
    let start_date = parse_datetime(&date).unwrap();
    let duration = 100 * 24 * 3600 * 1000;
    let first_window = date_to_timestamp(start_date) + duration;
    let second_window = first_window + duration;
    let first_window = parse_datetime(&first_window.to_string()).unwrap() / 1000000000;
    let second_window = parse_datetime(&second_window.to_string()).unwrap() / 1000000000;
    println!("three data is {} {} {}", start_date, first_window, second_window);

    let schema = &GRAPH.graph_schema;
    let tagclass_label = schema.get_vertex_label_id("TAGCLASS").unwrap();
    let forum_label = schema.get_vertex_label_id("FORUM").unwrap();
    let hastype_label = schema.get_edge_label_id("HASTYPE").unwrap();
    let hastag_label = schema.get_edge_label_id("HASTAG").unwrap();

    pegasus::run(conf, || {
        let tag_class = tag_class.clone();
        move |input, output| {
            let worker_id = input.get_worker_index();
            let stream = input.input_from(vec![0])?;
            stream
                .flat_map(move |_source| {
                    let mut tag_record_list = vec![];
                    let tagclass_vertices = GRAPH.get_all_vertices(Some(&vec![tagclass_label]));
                    let tagclass_count = tagclass_vertices.count();
                    let partial_count = tagclass_count / workers as usize + 1;
                    let tag_records = GRAPH
                        .get_all_vertices(Some(&vec![tagclass_label]))
                        .skip((worker_id % workers) as usize * partial_count)
                        .take(partial_count)
                        .filter(|store_vertex| {
                            store_vertex
                                .get_property("name")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .into_owned()
                                == tag_class
                        })
                        .map(|v| to_id_only_vertex(v))
                        .map(|v| Record::new(v, None));
                    for tag_record in tag_records {
                        let tagclass_internal_id =
                            tag_record.get(None).unwrap().as_id().unwrap() as DefaultId;
                        for tag_vertex in
                            GRAPH.get_in_vertices(tagclass_internal_id, Some(&vec![hastype_label]))
                        {
                            let tag_runtime_vertex = to_id_only_vertex(tag_vertex);
                            let mut new_record = tag_record.clone();
                            // TODO: OPT: no need to alias once find tag; should alias for tag when further explore message
                            new_record.append(tag_runtime_vertex, Some(0));
                            tag_record_list.push(new_record);
                        }
                    }
                    Ok(tag_record_list.into_iter())
                })?
                .repartition(move |tag_record: &Record| {
                    let id = tag_record
                        .get(Some(0))
                        .unwrap()
                        .as_id()
                        .unwrap();
                    Ok(get_partition(&id, workers as usize, pegasus::get_servers_len()))
                })
                .flat_map(move |tag_record: Record| {
                    let tag_internal_id = tag_record
                        .get(Some(0))
                        .unwrap()
                        .as_id()
                        .unwrap();
                    let mut tag_message_record_list = vec![];
                    //       tag_message_record_list.push((0, tag_internal_id));

                    let vertices = GRAPH
                        .get_in_vertices(tag_internal_id as DefaultId, Some(&vec![hastag_label]))
                        .filter(|store_vertex| store_vertex.get_label()[0] != forum_label)
                        .map(|v| to_id_only_vertex(v));

                    for vertex in vertices {
                        let mut new_tag_message_record = tag_record.clone();
                        new_tag_message_record.append(vertex, None);
                        tag_message_record_list.push(new_tag_message_record);
                    }

                    Ok(tag_message_record_list.into_iter())
                })?
                .count()?
                .sink_into(output)
        }
    })
    .expect("submit bi2 job failure")
}
