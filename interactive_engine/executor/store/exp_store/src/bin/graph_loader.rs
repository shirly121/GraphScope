use std::path::PathBuf;

use byteorder::{BigEndian, ReadBytesExt};
use clap::{App, Arg};
use graph_store::common::{LabelId, INVALID_LABEL_ID};
use graph_store::graph_db::{GlobalStoreUpdate, MutableGraphDB};
use graph_store::ldbc::get_graph_files;
use graph_store::parser::EdgeMeta;
use graph_store::prelude::{DefaultId, GraphDBConfig, Row, NAME, VERSION};
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

    let config = GraphDBConfig::default()
        .root_dir(graph_data_dir.clone())
        .partition(partition_index);

    let mut graph_builder = config.new();

    let raw_data_dir = PathBuf::from(raw_data_dir);
    let files = get_graph_files(raw_data_dir.clone(), partition_index, partition_num as usize)
        .expect(&format!("get graph files failed, raw_data_dir is {:?}", raw_data_dir));

    for (file_name, path) in files {
        let rdr = BufReader::new(File::open(path).unwrap());
        if file_name.contains("vertices") {
            println!("process vertex file: {:?}", file_name);
            let vertices_num = write_vertices(&mut graph_builder, rdr);
            println!("write vertex number: {:?}", vertices_num);
        } else if file_name.contains("edges") {
            println!("process edge file: {:?}", file_name);
            let edges_num = write_edges(&mut graph_builder, rdr, partition_index as i64, partition_num);
            println!("write edge number: {:?}", edges_num);
        }
    }

    graph_builder.export().expect("Export error!");
}

fn write_vertices<R: Read>(graph: &mut MutableGraphDB, mut rdr: BufReader<R>) -> i64 {
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
    let num_vertices =
        if let Ok(count) = graph.add_vertex_batches(container.into_iter()) { count as i64 } else { 0 };
    assert_eq!(vertex_count, num_vertices);
    vertex_count
}

fn write_edges<R: Read>(
    graph: &mut MutableGraphDB, mut rdr: BufReader<R>, curr_partition: i64, num_partitions: i64,
) -> i64 {
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
            let prop = Row::read_from(&mut prop_buff.as_slice()).unwrap();

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

    let mut count = 0;
    for (edge_meta, row) in container.into_iter() {
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

    //  assert_eq!(edge_count, count);
    edge_count
}
