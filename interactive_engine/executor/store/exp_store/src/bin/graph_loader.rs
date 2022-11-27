use std::borrow::BorrowMut;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use byteorder::{BigEndian, ReadBytesExt};
use clap::{App, Arg};
use graph_store::common::{LabelId, INVALID_LABEL_ID};
use graph_store::graph_db::{GlobalStoreUpdate, MutableGraphDB};
use graph_store::ldbc::get_graph_files;
use graph_store::parser::EdgeMeta;
use graph_store::prelude::{DefaultId, GraphDBConfig, InternalId, MutTopo, Row, NAME, VERSION};
use graph_store::table::{PropertyTable, SingleValueTable};
use pegasus_common::codec::Decode;
use std::fs::File;
use std::io::{BufReader, Read};

fn main() {
    env_logger::init();
    let matches = App::new(NAME)
        .version(VERSION)
        .about("Build graph storage on single machine.")
        .args(&[
            Arg::with_name("raw_data_dir")
                .short("r")
                .long_help("The directory to the raw data")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("graph_data_dir")
                .short("g")
                .long_help("The directory to graph store")
                .required(true)
                .takes_value(true)
                .index(2),
            Arg::with_name("partition")
                .short("p")
                .long_help("The number of partitions")
                .takes_value(true),
            Arg::with_name("partition_index")
                .short("i")
                .long_help("Current index number of partition")
                .takes_value(true),
            Arg::with_name("threads")
                .short("t")
                .long_help("Number of threads to execute graph loader parallelly")
                .takes_value(true),
        ])
        .get_matches();

    let raw_data_dir = matches
        .value_of("raw_data_dir")
        .unwrap()
        .to_string();

    let graph_data_dir = matches
        .value_of("graph_data_dir")
        .unwrap()
        .to_string();

    let partition_num = matches
        .value_of("partition")
        .unwrap_or("1")
        .parse::<i64>()
        .expect(&format!("Specify invalid partition number"));

    let partition_index = matches
        .value_of("partition_index")
        .unwrap_or("0")
        .parse::<usize>()
        .expect(&format!("Specify invalid partition index"));

    let mut thread_num = matches
        .value_of("threads")
        .unwrap_or("1")
        .parse::<usize>()
        .expect("Specify invalid thread_num");

    let config = GraphDBConfig::default()
        .root_dir(graph_data_dir.clone())
        .partition(partition_index);

    let graph_builder: Arc<
        Mutex<MutableGraphDB<DefaultId, InternalId, MutTopo<InternalId>, PropertyTable, SingleValueTable>>,
    > = Arc::new(Mutex::new(config.new()));

    let raw_data_dir = PathBuf::from(raw_data_dir);
    let files: Arc<Vec<(String, PathBuf)>> = Arc::new(
        get_graph_files(raw_data_dir.clone(), partition_index, partition_num as usize)
            .expect(&format!("get graph files failed, raw_data_dir is {:?}", raw_data_dir))
            .into_iter()
            .collect(),
    );

    if files.len() < thread_num {
        thread_num = files.len()
    }
    let mut thread_handles = vec![];
    for thread_id in 0..thread_num {
        let files = files.clone();
        let graph_builder = graph_builder.clone();
        let thread_handle = thread::spawn(move || {
            let split_files_with_path = get_split_files_with_path(files, thread_id, thread_num);
            let mut vertex_containers = vec![];
            let mut edge_containers = vec![];
            for (file_name, path) in split_files_with_path {
                let rdr = BufReader::new(File::open(path).unwrap());
                if file_name.contains("vertices") {
                    println!("process vertex file: {:?}", file_name);
                    let (vertex_container, vertices_num) = read_vertices(rdr);
                    println!("read vertex number: {:?}", vertices_num);
                    vertex_containers.push((vertex_container, vertices_num))
                } else if file_name.contains("edges") {
                    println!("process edge file: {:?}", file_name);
                    let (edge_container, edges_num) = read_edges(rdr);
                    println!("read edge number: {:?}", edges_num);
                    edge_containers.push((edge_container, edges_num))
                }
            }
            let mut graph = graph_builder.lock().unwrap();
            println!("thread{} start to write graphs", thread_id);
            for (vertex_container, vertices_num) in vertex_containers {
                write_vertices(&mut graph.borrow_mut(), vertex_container, vertices_num);
            }
            for (edge_container, edges_num) in edge_containers {
                write_edges(
                    &mut graph.borrow_mut(),
                    edge_container,
                    edges_num,
                    partition_index as i64,
                    partition_num,
                );
            }
            println!("thread{} finishes", thread_id);
        });
        thread_handles.push(thread_handle)
    }

    for thread_handle in thread_handles {
        thread_handle.join().unwrap();
    }

    graph_builder
        .lock()
        .unwrap()
        .export()
        .expect("Export error!");
}

fn read_vertices<R: Read>(mut rdr: BufReader<R>) -> (Vec<(usize, [u8; 2], Row)>, usize) {
    let mut vertex_count = 0;
    let mut container = vec![];
    loop {
        // id, primary_label, secondary_label, len, property_bytes
        let id = rdr.read_i64::<BigEndian>();
        if let Ok(id) = id {
            let primary_label = rdr.read_i32::<BigEndian>().unwrap();
            let secondary_label = rdr.read_i32::<BigEndian>().unwrap();
            let prop_len = rdr.read_i64::<BigEndian>().unwrap() as usize;
            let mut prop_buff: Vec<u8> = vec![0; prop_len];
            rdr.read_exact(prop_buff.as_mut()).unwrap();
            let prop = Row::read_from(&mut prop_buff.as_slice()).unwrap();
            container.push((id as DefaultId, [primary_label as LabelId, secondary_label as LabelId], prop));
            vertex_count += 1;
        } else {
            break;
        }
    }
    (container, vertex_count)
}

fn write_vertices(
    graph: &mut MutableGraphDB, vertex_container: Vec<(usize, [u8; 2], Row)>, vertex_count: usize,
) -> usize {
    let num_vertices =
        if let Ok(count) = graph.add_vertex_batches(vertex_container.into_iter()) { count } else { 0 };
    assert_eq!(vertex_count, num_vertices);
    vertex_count
}

fn read_edges<R: Read>(mut rdr: BufReader<R>) -> (Vec<(EdgeMeta<usize>, Row)>, usize) {
    let mut edge_count = 0;
    let mut container = vec![];
    loop {
        // edge_label_id, src_id, src_label_id, dst_id, dst_label_id, len, property_bytes
        let id = rdr.read_i32::<BigEndian>();
        if let Ok(label_id) = id {
            let src_vertex_id = rdr.read_i64::<BigEndian>().unwrap();
            let src_label_id = rdr.read_i32::<BigEndian>().unwrap();
            let dst_vertex_id = rdr.read_i64::<BigEndian>().unwrap();
            let dst_label_id = rdr.read_i32::<BigEndian>().unwrap();
            let prop_len = rdr.read_i64::<BigEndian>().unwrap() as usize;
            let mut prop_buff: Vec<u8> = vec![0; prop_len];
            rdr.read_exact(prop_buff.as_mut()).unwrap();
            let prop = Row::default();

            container.push((
                EdgeMeta {
                    src_global_id: src_vertex_id as DefaultId,
                    src_label_id: src_label_id as LabelId,
                    dst_global_id: dst_vertex_id as DefaultId,
                    dst_label_id: dst_label_id as LabelId,
                    label_id: label_id as LabelId,
                },
                prop,
            ));
            edge_count += 1;
        } else {
            break;
        }
    }
    (container, edge_count)
}

fn write_edges(
    graph: &mut MutableGraphDB, edge_container: Vec<(EdgeMeta<usize>, Row)>, edge_count: usize,
    curr_partition: i64, num_partitions: i64,
) -> usize {
    let mut count = 0;
    for (edge_meta, row) in edge_container.into_iter() {
        if edge_meta.src_global_id as i64 % num_partitions == curr_partition {
            graph.add_vertex(edge_meta.src_global_id, [edge_meta.src_label_id, INVALID_LABEL_ID]);
        } else {
            graph.add_corner_vertex(edge_meta.src_global_id, edge_meta.src_label_id);
        }
        if edge_meta.dst_global_id as i64 % num_partitions == curr_partition {
            graph.add_vertex(edge_meta.dst_global_id, [edge_meta.dst_label_id, INVALID_LABEL_ID]);
        } else {
            graph.add_corner_vertex(edge_meta.dst_global_id, edge_meta.dst_label_id);
        }
        let result = if row.is_empty() {
            graph.add_edge(edge_meta.src_global_id, edge_meta.dst_global_id, edge_meta.label_id)
        } else {
            graph
                .add_edge_with_properties(
                    edge_meta.src_global_id,
                    edge_meta.dst_global_id,
                    edge_meta.label_id,
                    row,
                )
                .is_ok()
        };
        if result {
            count += 1;
        }
    }

    assert_eq!(edge_count, count);
    edge_count
}

fn get_split_files_with_path(
    files_with_path: Arc<Vec<(String, PathBuf)>>, thread_id: usize, thread_num: usize,
) -> Vec<(String, PathBuf)> {
    let mut split_files_with_path = vec![];
    for (i, file_with_path) in files_with_path.iter().enumerate() {
        if thread_id == i % thread_num {
            split_files_with_path.push(file_with_path.clone())
        }
    }
    split_files_with_path
}
