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
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args();
    match config.mode.as_str() {
        "flat_map" => {
            let flat_map_time = test_flat_map(config.number, config.expands);
            println!("flat map time is {} ms\n", flat_map_time);
        }
        "set" => {
            let generate_set_time = test_set_generate(
                config.number,
                config.left_set_len,
                config.right_set_len,
                config.intersect_set_len,
            );
            println!("generate set time is {} ms\n", generate_set_time);
            let intersect_set_time = test_set_generate_and_intersect(
                config.number,
                config.left_set_len,
                config.right_set_len,
                config.intersect_set_len,
            ) - generate_set_time;
            println!("intersect set time is {} ms\n", intersect_set_time);
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn test_flat_map(number: usize, expands: usize) -> u128 {
    let start_time = Instant::now();
    let repeat_iterator = iter::repeat(Record::default()).take(number);
    let expand_iterator = repeat_iterator.flat_map(|record| {
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
    let repeat_iterator = iter::repeat(Record::default()).take(number);
    let set_iterator = repeat_iterator.map(|_record| {
        let left_set: HashSet<usize> = (0..left_set_len).collect();
        let right_set: HashSet<usize> = ((left_set_len - intersect_set_len)
            ..(left_set_len - intersect_set_len + right_set_len))
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
    let repeat_iterator = iter::repeat(Record::default()).take(number);
    let intersect_iterator = repeat_iterator.map(|_record| {
        let left_set: HashSet<usize> = (0..left_set_len).collect();
        let right_set: HashSet<usize> = ((left_set_len - intersect_set_len)
            ..(left_set_len - intersect_set_len + right_set_len))
            .collect();
        let intersect_set: HashSet<usize> = left_set
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
