use crate::queries::graph::*;
use dyn_type::Object;
use graph_proxy::apis::{Details, GraphElement};
use graph_proxy::{to_empty_vertex, to_runtime_vertex};
use graph_store::prelude::*;
use itertools::__std_iter::Iterator;
use pegasus::api::{Count, Map, Sink};
use pegasus::result::ResultStream;
use pegasus::JobConf;
use runtime::process::record::Record;

/// this is the handwritten + record version of partial bi2, with only two-hops expxansion by tag_class -- tag -- message;
pub fn bi2_hop_record(conf: JobConf, date: String, tag_class: String) -> ResultStream<u64> {
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
                        .map(|v| to_runtime_vertex(v, None))
                        .filter(|v| {
                            let property = v
                                .details()
                                .unwrap()
                                .get_property(&"name".into())
                                .unwrap()
                                .try_to_owned()
                                .unwrap();
                            property == Object::from(tag_class.clone())
                        })
                        .map(|v| Record::new(v, None));
                    for tag_record in tag_records {
                        let tag_record: Record = tag_record;
                        let vertex = tag_record
                            .get(None)
                            .unwrap()
                            .as_graph_vertex()
                            .unwrap();
                        let tagclass_internal_id = vertex.id() as DefaultId;
                        for tag_vertex in
                            GRAPH.get_in_vertices(tagclass_internal_id, Some(&vec![hastype_label]))
                        {
                            let tag_runtime_vertex = to_empty_vertex(tag_vertex);
                            let mut new_record = tag_record.clone();
                            // TODO: OPT: no need to alias once find tag; should alias for tag when further explore message
                            new_record.append(tag_runtime_vertex, Some(0));
                            tag_record_list.push(new_record);
                        }
                    }
                    Ok(tag_record_list.into_iter())
                })?
                .repartition(move |tag_record: &Record| {
                    let tag_entry = tag_record.get(Some(0)).unwrap();
                    Ok(get_entry_partition(tag_entry, workers as usize, pegasus::get_servers_len()))
                })
                .flat_map(move |tag_record: Record| {
                    let tag_vertex = tag_record
                        .get(Some(0))
                        .unwrap()
                        .as_graph_vertex()
                        .unwrap();
                    let tag_internal_id = tag_vertex.id();
                    let mut tag_message_record_list = vec![];
                    //       tag_message_record_list.push((0, tag_internal_id));

                    let vertices = GRAPH
                        .get_in_vertices(tag_internal_id as DefaultId, Some(&vec![hastag_label]))
                        .map(|v| to_empty_vertex(v))
                        .filter(|runtime_vertex| {
                            let label_obj: Object = runtime_vertex.label().unwrap().into();
                            let forum_label_obj: Object = (forum_label as i32).into();
                            label_obj == forum_label_obj
                        });

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
