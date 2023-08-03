//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//!     http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    codegen_inplace()
}

// TODO: use cfg-if! instead
#[cfg(feature = "grin_features_enable_v6d")]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-link-search=/usr/local/lib");
    println!("cargo:rustc-link-lib=vineyard_grin");
    println!("cargo:rustc-link-lib=vineyard_graph");
    println!("cargo:rustc-link-lib=vineyard_io");
    println!("cargo:rustc-link-lib=vineyard_client");
    println!("cargo:rustc-link-lib=vineyard_basic");

    Ok(())
}

#[cfg(all(feature = "grin_features_enable_graphar", not(feature = "grin_features_enable_v6d")))]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-link-search=/usr/local/lib");
    println!("cargo:rustc-link-lib=gar-grin");

    Ok(())
}

#[cfg(not(any(feature = "grin_features_enable_v6d", feature = "grin_features_enable_graphar")))]
fn codegen_inplace() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}
