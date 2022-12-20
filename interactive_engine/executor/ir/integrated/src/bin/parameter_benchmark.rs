//
//! Copyright 2022 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.
//!

use graph_proxy::apis::{DynDetails, Vertex};
use pegasus::api::{Join, KeyBy, Map, Sink};
use pegasus::JobConf;
use runtime::process::record::{Entry, Record};
use std::collections::HashSet;
use std::error::Error;
use std::iter;
use std::time::Instant;
use structopt::StructOpt;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(StructOpt)]
pub struct Config {
    #[structopt(short = "n", long = "number", default_value = "1000000")]
    number: usize,
    #[structopt(short = "e", long = "expands", default_value = "10")]
    expands: usize,
    #[structopt(short = "l", long = "left_set_len", default_value = "100")]
    left_set_len: usize,
    #[structopt(short = "r", long = "right_set_len", default_value = "100")]
    right_set_len: usize,
    #[structopt(short = "i", long = "intersect_set_len", default_value = "10")]
    intersect_set_len: usize,
    #[structopt(short = "m", long = "mode", default_value = "flat_map")]
    mode: String,
    #[structopt(short = "p", long = "use_pegasus")]
    use_pegasus: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args();
    match config.mode.as_str() {
        "flat_map" => {
            let flat_map_time = if config.use_pegasus {
                test_flat_map_pegasus(config.number, config.expands)
            } else {
                test_flat_map(config.number, config.expands)
            };
            println!("flat map time is {} ms\n", flat_map_time);
        }
        "set" => {
            if config.use_pegasus {
                let generate_set_time = test_set_generate_pegasus(
                    config.number,
                    config.left_set_len,
                    config.right_set_len,
                    config.intersect_set_len,
                );
                println!("generate set time is {} ms\n", generate_set_time);
                let time_sum = test_set_generate_and_intersect_pegasus(
                    config.number,
                    config.left_set_len,
                    config.right_set_len,
                    config.intersect_set_len,
                );
                let intersect_set_time =
                    if time_sum < generate_set_time { 0 } else { time_sum - generate_set_time };
                println!("intersect set time is {} ms\n", intersect_set_time);
            } else {
                let generate_set_time = test_set_generate(
                    config.number,
                    config.left_set_len,
                    config.right_set_len,
                    config.intersect_set_len,
                );
                println!("generate set time is {} ms\n", generate_set_time);
                let time_sum = test_set_generate_and_intersect(
                    config.number,
                    config.left_set_len,
                    config.right_set_len,
                    config.intersect_set_len,
                );
                let intersect_set_time =
                    if time_sum < generate_set_time { 0 } else { time_sum - generate_set_time };
                println!("intersect set time is {} ms\n", intersect_set_time);
            }
        }
        "join" => {
            let join_time =
                test_join_pegasus(config.left_set_len, config.right_set_len, config.intersect_set_len);
            println!("join time is {} ms\n", join_time)
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn test_flat_map(number: usize, expands: usize) -> u128 {
    let source: Vec<Record> = iter::repeat(Record::default())
        .take(number)
        .collect();
    let start_time = Instant::now();
    let expand_iterator = source.into_iter().flat_map(|record| {
        (0..expands).map(move |i| {
            let mut new_record = record.clone();
            new_record.append(Entry::V(Vertex::new(i as u64, Some(0), DynDetails::default())), Some(0));
            new_record
        })
    });
    let mut count = 0;
    for _ in expand_iterator {
        count += 1;
    }
    println!("{}", count);
    start_time.elapsed().as_millis()
}

fn test_set_generate(
    number: usize, left_set_len: usize, right_set_len: usize, intersect_set_len: usize,
) -> u128 {
    let start_time = Instant::now();
    let set_iterator = (0..number).map(|_record| {
        let left_set: HashSet<Vertex> = (0..left_set_len)
            .map(|i| Vertex::new(i as u64, Some(0), DynDetails::default()))
            .collect();
        let right_set: HashSet<Vertex> = ((left_set_len - intersect_set_len)
            ..(left_set_len - intersect_set_len + right_set_len))
            .map(|i| Vertex::new(i as u64, Some(0), DynDetails::default()))
            .collect();
        (left_set, right_set)
    });
    let mut count = 0;
    for _ in set_iterator {
        count += 1;
    }
    println!("{}", count);
    start_time.elapsed().as_millis()
}

fn test_set_generate_and_intersect(
    number: usize, left_set_len: usize, right_set_len: usize, intersect_set_len: usize,
) -> u128 {
    let start_time = Instant::now();
    let intersect_iterator = (0..number).map(|_record| {
        let left_set: HashSet<Vertex> = (0..left_set_len)
            .map(|i| Vertex::new(i as u64, Some(0), DynDetails::default()))
            .collect();
        let right_set: HashSet<Vertex> = ((left_set_len - intersect_set_len)
            ..(left_set_len - intersect_set_len + right_set_len))
            .map(|i| Vertex::new(i as u64, Some(0), DynDetails::default()))
            .collect();
        let intersect_set: HashSet<Vertex> = left_set
            .intersection(&right_set)
            .cloned()
            .collect();
        intersect_set
    });
    let mut count = 0;
    for _set in intersect_iterator {
        count += 1;
    }
    println!("{}", count);
    start_time.elapsed().as_millis()
}

fn test_flat_map_pegasus(number: usize, expands: usize) -> u128 {
    let conf = JobConf::new("flat_map_test");
    let start_time = Instant::now();
    let expand_iterator = pegasus::run(conf, move || {
        move |input, output| {
            input
                .input_from(iter::repeat(Record::default()).take(number))?
                .flat_map(move |record| {
                    Ok((0..expands).map(move |i| {
                        let mut new_record = record.clone();
                        new_record.append(
                            Entry::V(Vertex::new(i as u64, Some(0), DynDetails::default())),
                            Some(0),
                        );
                        new_record
                    }))
                })?
                .sink_into(output)
        }
    })
    .unwrap();
    let mut count = 0;
    for _ in expand_iterator {
        count += 1;
    }
    println!("{}", count);
    start_time.elapsed().as_millis()
}

fn test_set_generate_pegasus(
    number: usize, left_set_len: usize, right_set_len: usize, intersect_set_len: usize,
) -> u128 {
    let conf = JobConf::new("set_generate_test");
    let start_time = Instant::now();
    let set_iterator = pegasus::run(conf, move || {
        move |input, output| {
            input
                .input_from(0..(number as u64))?
                .map(move |_record| {
                    let left_set: HashSet<Vertex> = (0..left_set_len)
                        .map(|i| Vertex::new(i as u64, Some(0), DynDetails::default()))
                        .collect();
                    let right_set: HashSet<Vertex> = ((left_set_len - intersect_set_len)
                        ..(left_set_len - intersect_set_len + right_set_len))
                        .map(|i| Vertex::new(i as u64, Some(0), DynDetails::default()))
                        .collect();
                    Ok((left_set.len() as u64, right_set.len() as u64))
                })?
                .sink_into(output)
        }
    })
    .unwrap();
    let mut count = 0;
    for _ in set_iterator {
        count += 1;
    }
    println!("{}", count);
    start_time.elapsed().as_millis()
}

fn test_set_generate_and_intersect_pegasus(
    number: usize, left_set_len: usize, right_set_len: usize, intersect_set_len: usize,
) -> u128 {
    let conf = JobConf::new("set_generate_test");
    let start_time = Instant::now();
    let set_iterator = pegasus::run(conf, move || {
        move |input, output| {
            input
                .input_from(0..(number as u64))?
                .map(move |_record| {
                    let left_set: HashSet<Vertex> = (0..left_set_len)
                        .map(|i| Vertex::new(i as u64, Some(0), DynDetails::default()))
                        .collect();
                    let right_set: HashSet<Vertex> = ((left_set_len - intersect_set_len)
                        ..(left_set_len - intersect_set_len + right_set_len))
                        .map(|i| Vertex::new(i as u64, Some(0), DynDetails::default()))
                        .collect();
                    let intersect_set: Vec<Vertex> = left_set
                        .intersection(&right_set)
                        .cloned()
                        .collect();
                    Ok(intersect_set)
                })?
                .sink_into(output)
        }
    })
    .unwrap();
    let mut count = 0;
    for _ in set_iterator {
        count += 1;
    }
    println!("{}", count);
    start_time.elapsed().as_millis()
}

fn test_join_pegasus(left_number: usize, right_number: usize, intersect_number: usize) -> u128 {
    let conf = JobConf::new("join test");
    let start_time = Instant::now();
    let join_iterator = pegasus::run(conf, move || {
        move |input, output| {
            let src1 = input.input_from([0])?;
            let (src1, src2) = src1.copied()?;
            let stream1 = src1.flat_map(move |_| {
                Ok((0..left_number).map(|i| {
                    Record::new(Entry::V(Vertex::new(i as u64, Some(0), DynDetails::default())), Some(0))
                }))
            })?;
            let stream2 = src2.flat_map(move |_| {
                Ok(((left_number - intersect_number)..(left_number - intersect_number + right_number)).map(
                    |i| {
                        Record::new(
                            Entry::V(Vertex::new(i as u64, Some(0), DynDetails::default())),
                            Some(0),
                        )
                    },
                ))
            })?;
            let stream3 = stream1
                .key_by(|record| {
                    Ok((
                        record
                            .get(Some(0))
                            .unwrap()
                            .as_graph_vertex()
                            .unwrap()
                            .clone(),
                        record,
                    ))
                })?
                .inner_join(stream2.key_by(|record| {
                    Ok((
                        record
                            .get(Some(0))
                            .unwrap()
                            .as_graph_vertex()
                            .unwrap()
                            .clone(),
                        record,
                    ))
                })?)?;
            stream3.sink_into(output)
        }
    })
    .unwrap();
    let mut count = 0;
    for _ in join_iterator {
        count += 1;
    }
    println!("{}", count);
    start_time.elapsed().as_millis()
}
