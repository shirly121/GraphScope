use crate::queries::graph::*;
use graph_store::prelude::*;
use pegasus::api::{Count, Map, Sink};
use pegasus::result::ResultStream;
use pegasus::JobConf;

/// this is the handwritten version of partial bi2, with only two-hops expxansion by tag_class -- tag -- message
/// specifically, to avoid too many results, we add count() in the end
pub fn bi2_hop(conf: JobConf, date: String, tag_class: String) -> ResultStream<u64> {
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
                    let mut tag_id_list = vec![];
                    let tagclass_vertices = GRAPH.get_all_vertices(Some(&vec![tagclass_label]));
                    let tagclass_count = tagclass_vertices.count();
                    let partial_count = tagclass_count / workers as usize + 1;
                    for vertex in GRAPH
                        .get_all_vertices(Some(&vec![tagclass_label]))
                        .skip((worker_id % workers) as usize * partial_count)
                        .take(partial_count)
                    {
                        let tagclass_name = vertex
                            .get_property("name")
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .into_owned();
                        if tagclass_name == tag_class {
                            let tagclass_internal_id = vertex.get_id();
                            for tag_vertex in
                                GRAPH.get_in_vertices(tagclass_internal_id, Some(&vec![hastype_label]))
                            {
                                let tag_internal_id = tag_vertex.get_id() as u64;
                                tag_id_list.push(tag_internal_id);
                            }
                        }
                    }
                    Ok(tag_id_list.into_iter())
                })?
                .repartition(move |id| Ok(get_partition(id, workers as usize, pegasus::get_servers_len())))
                .flat_map(move |tag_internal_id| {
                    let mut message_id_list = vec![];
                    //  message_id_list.push((0, tag_internal_id));
                    for vertex in
                        GRAPH.get_in_vertices(tag_internal_id as DefaultId, Some(&vec![hastag_label]))
                    {
                        if vertex.get_label()[0] == forum_label {
                            continue;
                        }
                        message_id_list.push((vertex.get_id() as u64, tag_internal_id));
                    }
                    Ok(message_id_list.into_iter())
                })?
                .count()?
                .sink_into(output)
        }
    })
    .expect("submit bi2 job failure")
}
