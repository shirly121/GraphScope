use std::collections::HashMap;

use crate::queries::graph::*;
use graph_store::prelude::*;
use pegasus::api::{Fold, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

pub fn bi2(conf: JobConf, date: String, tag_class: String) -> ResultStream<(String, i32, i32, i32)> {
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
                    // TODO: is this for count when 0 message?
                    message_id_list.push((0, tag_internal_id));
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
                .repartition(move |(id, _)| {
                    Ok(get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .fold_partition(HashMap::<u64, (i32, i32)>::new(), move || {
                    move |mut collect, (message_internal_id, tag_internal_id)| {
                        if message_internal_id == 0 {
                            if let None = collect.get_mut(&tag_internal_id) {
                                collect.insert(tag_internal_id, (0, 0));
                            }
                        } else {
                            let message_vertex = GRAPH
                                .get_vertex(message_internal_id as DefaultId)
                                .unwrap();
                            let create_date = message_vertex
                                .get_property("creationDate")
                                .unwrap()
                                .as_u64()
                                .unwrap()
                                / 1000000000;
                            if create_date >= start_date && create_date < first_window {
                                if let Some(data) = collect.get_mut(&tag_internal_id) {
                                    data.0 += 1;
                                } else {
                                    collect.insert(tag_internal_id, (1, 0));
                                }
                            } else if create_date >= first_window && create_date < second_window {
                                if let Some(data) = collect.get_mut(&tag_internal_id) {
                                    data.1 += 1;
                                } else {
                                    collect.insert(tag_internal_id, (0, 1));
                                }
                            }
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| Ok(map.into_iter()))?
                .fold(HashMap::<u64, (i32, i32)>::new(), || {
                    |mut collect, (tag_internal_id, (count1, count2))| {
                        if let Some(data) = collect.get_mut(&tag_internal_id) {
                            data.0 += count1;
                            data.1 += count2;
                        } else {
                            collect.insert(tag_internal_id, (count1, count2));
                        }
                        Ok(collect)
                    }
                })?
                .unfold(|map| {
                    Ok(map
                        .into_iter()
                        .map(|(tag_internal_id, (count1, count2))| {
                            (tag_internal_id, count1, count2, (count1 - count2).abs())
                        }))
                })?
                .repartition(move |(id, _, _, _)| {
                    Ok(get_partition(id, workers as usize, pegasus::get_servers_len()))
                })
                .map(|(tag_internal_id, count1, count2, diff)| {
                    let tag_vertex = GRAPH
                        .get_vertex(tag_internal_id as DefaultId)
                        .unwrap();
                    let tag_name = tag_vertex
                        .get_property("name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .into_owned();
                    Ok((tag_name, count1, count2, diff))
                })?
                .sort_limit_by(100, |x, y| x.3.cmp(&y.3).reverse().then(x.0.cmp(&y.0)))?
                .sink_into(output)
        }
    })
    .expect("submit bi2 job failure")
}
