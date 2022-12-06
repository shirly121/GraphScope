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
use runtime_integration::{read_catalogue, read_pattern};
use std::error::Error;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

fn main() -> Result<(), Box<dyn Error>> {
    let pattern = read_pattern()?;
    let pattern_code = pattern.encode_to();
    let catalog = read_catalogue()?;
    if let Some(pattern_index) = catalog.get_pattern_index(&pattern_code) {
        let pattern_count = catalog
            .get_pattern_weight(pattern_index)
            .unwrap()
            .get_count();
        println!("{:?}", pattern);
        println!("Estimate pattern count is {}", pattern_count);
    } else {
        println!("Sorry, don't find pattern's info in the catalog");
    }
    Ok(())
}
