use crate::queries::graph::*;
use graph_store::common::LabelId;
use mcsr::{
    columns::{DateTimeColumn, StringColumn},
    date,
};
use pegasus::api::{Fold, Map, Sink, SortLimitBy};
use pegasus::result::ResultStream;
use pegasus::JobConf;

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

pub fn bi2_sub(conf: JobConf, date: String, tag_class: String) -> ResultStream<(String, i32, i32, i32)> {
    let workers = conf.workers;
    let start_date = date::parse_date(&date).unwrap();
    let start_date_ts = start_date.to_u32();
    let first_window = start_date.add_days(100);
    let first_window_ts = first_window.to_u32();
    let second_window = first_window.add_days(100);
    let second_window_ts = second_window.to_u32();

    let comment_createdate_col = &CSR.vertex_prop_table[2_usize]
        .get_column_by_name("creationDate")
        .as_any()
        .downcast_ref::<DateTimeColumn>()
        .unwrap()
        .data;
    let post_createdate_col = &CSR.vertex_prop_table[3_usize]
        .get_column_by_name("creationDate")
        .as_any()
        .downcast_ref::<DateTimeColumn>()
        .unwrap()
        .data;

    pegasus::run(conf, || {
        let tag_class = tag_class.clone();
        let (tag_list, tag_name_list) = get_tag_list(tag_class);
        let tag_list_size = tag_list.len() as u32;

        move |input, output| {
            let worker_id = input.get_worker_index();
            let stream = input.input_from(vec![0_i32])?;
            stream
                .flat_map(move |_| {
                    let mut result = vec![];
                    let mut cur_tag_index = worker_id % workers;
                    println!("tag_list_size is {}", tag_list_size);
                    loop {
                        if cur_tag_index >= tag_list_size {
                            break;
                        }
                        let tag_id = tag_list[cur_tag_index as usize];
                        let mut count_window_1 = 0_i32;
                        let mut count_window_2 = 0_i32;
                        if let Some(edges) = COMMENT_HASTAG_TAG_IN.get_adj_list(tag_id) {
                            for e in edges {
                                let ts = comment_createdate_col[e.neighbor].date_to_u32();
                                if ts < first_window_ts && ts >= start_date_ts {
                                    count_window_1 += 1;
                                } else if ts < second_window_ts && ts >= first_window_ts {
                                    count_window_2 += 1;
                                }
                            }
                        }
                        if let Some(edges) = POST_HASTAG_TAG_IN.get_adj_list(tag_id) {
                            for e in edges {
                                let ts = post_createdate_col[e.neighbor].date_to_u32();
                                if ts < first_window_ts && ts >= start_date_ts {
                                    count_window_1 += 1;
                                } else if ts < second_window_ts && ts >= first_window_ts {
                                    count_window_2 += 1;
                                }
                            }
                        }
                        result.push((cur_tag_index, count_window_1, count_window_2));
                        cur_tag_index += workers;
                    }
                    println!("result size is {}", result.len());
                    Ok(result.into_iter())
                })?
                .fold(Vec::<(i32, i32)>::new(), move || {
                    let tag_list_size = tag_list_size;
                    move |mut collect, (index, count_window_1, count_window_2)| {
                        if collect.len() < tag_list_size as usize {
                            collect.resize(tag_list_size as usize, (0, 0));
                        }
                        collect[index as usize].0 += count_window_1;
                        collect[index as usize].1 += count_window_2;
                        Ok(collect)
                    }
                })?
                .unfold(move |list| {
                    let mut result = vec![];
                    for (index, (count1, count2)) in list.iter().enumerate() {
                        result.push((
                            tag_name_list[index].clone(),
                            *count1,
                            *count2,
                            i32::abs(*count1 - *count2),
                        ));
                        // result.push((tag_name_list[index], count1, count2, i32::abs(count1 - count2)));
                        // if *count1 > 0 || *count2 > 0 {
                        //     result.push((
                        //         tag_name_list[index].clone(),
                        //         *count1,
                        //         *count2,
                        //         i32::abs(*count1 - *count2),
                        //     ));
                        // }
                    }
                    Ok(result.into_iter())
                })?
                .sort_limit_by(100_u32, |x, y| y.3.cmp(&x.3).then(x.0.cmp(&y.0)))?
                .sink_into(output)
        }
    })
    .expect("submit bi2_sub job failed")
}
