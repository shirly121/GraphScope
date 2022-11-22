use graph_store::common::LabelId;
use mcsr::columns::StringColumn;
use pegasus::api::{Count, Map, Sink};
use pegasus::result::ResultStream;
use pegasus::JobConf;

use crate::queries::graph::*;

fn get_tag_list(tagclass: String) -> (Vec<usize>, Vec<String>) {
    let tagclass_num = CSR.get_vertices_num(6 as LabelId);
    let tagclass_name_col = &CSR.vertex_prop_table[6_usize]
        .get_column_by_name("name")
        .as_any()
        .downcast_ref::<StringColumn>()
        .unwrap()
        .data;
    let mut tagclass_id = usize::MAX;
    let tag_name_col = &CSR.vertex_prop_table[7_usize]
        .get_column_by_name("name")
        .as_any()
        .downcast_ref::<StringColumn>()
        .unwrap()
        .data;
    for v in 0..tagclass_num {
        if tagclass_name_col[v] == tagclass {
            tagclass_id = v;
            break;
        }
    }
    if tagclass_id == usize::MAX {
        return (vec![], vec![]);
    }
    println!("target tagclass id is {}", tagclass_id);

    let mut tag_list = vec![];
    let mut tag_name_list = vec![];
    if let Some(edges) = TAG_HASTYPE_TAGCLASS_IN.get_adj_list(tagclass_id) {
        for e in edges {
            tag_list.push(e.neighbor);
            tag_name_list.push(tag_name_col[e.neighbor].clone());
        }
    }

    (tag_list, tag_name_list)
}

pub fn bi2_sub_hop(conf: JobConf, _date: String, tag_class: String) -> ResultStream<u64> {
    let workers = conf.workers;

    pegasus::run(conf, || {
        let tag_class = tag_class.clone();
        let (tag_list, _tag_name_list) = get_tag_list(tag_class);
        let tag_list_size = tag_list.len() as u32;

        move |input, output| {
            let worker_id = input.get_worker_index();
            let stream = input.input_from(vec![0_i32])?;
            stream
                .flat_map(move |_| {
                    let mut cur_tag_index = worker_id % workers;
                    let mut message_id_list = vec![];
                    loop {
                        if cur_tag_index >= tag_list_size {
                            break;
                        }
                        let tag_id = tag_list[cur_tag_index as usize];
                        if let Some(comment_hastag_tag_in) = COMMENT_HASTAG_TAG_IN.get_adj_list(tag_id) {
                            for comment in comment_hastag_tag_in {
                                message_id_list.push((comment.neighbor as u64, tag_id as u64));
                            }
                        }
                        if let Some(post_hastag_tag_in) = POST_HASTAG_TAG_IN.get_adj_list(tag_id) {
                            for post in post_hastag_tag_in {
                                message_id_list.push((post.neighbor as u64, tag_id as u64));
                            }
                        }
                        cur_tag_index += workers;
                    }
                    Ok(message_id_list.into_iter())
                })?
                .count()?
                .sink_into(output)
        }
    })
    .expect("submit bi2_sub job failed")
}
