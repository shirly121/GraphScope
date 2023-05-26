//
//! Copyright 2020 Alibaba Group Holding Limited.
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
//!


use bloom::BloomFilter;
use crate::catalogue::pattern::{Pattern};
use std::{thread, collections::{HashMap, HashSet}, usize::MAX, time::Instant, iter::Iterator};

use graph_store::prelude::{GlobalStoreTrait, LargeGraphDB};

use super::gallop::intersect;

fn is_sorted<T>(data: &[T]) -> bool
where
    T: Ord,
{
    data.windows(2).all(|w| w[0] <= w[1])
}

pub fn gen_plan(pattern: &Pattern, edge_table: & mut HashMap<(u32, u32), i32>) ->Vec<(u32, Vec<usize>)>{
    let start_vtx = 0;
    let mut plan_stack = Vec::new();
    let mut plan = Vec::new();
    let pattern_size = pattern.get_vertices_num();
    let mut visited = vec![0;pattern_size];
    let mut neighbor_matrix = vec![0;pattern_size*pattern_size];
    for i in pattern.edges_iter() {
        let src_v = i.get_start_vertex().get_id();
        let dst_v = i.get_end_vertex().get_id();
        neighbor_matrix[src_v*pattern_size+dst_v]=1;
        neighbor_matrix[dst_v*pattern_size+src_v]=1;
    }
    plan_stack.push(start_vtx);
    while !plan_stack.is_empty() {
        let current_vtx = plan_stack.pop().unwrap();
        if visited[current_vtx]==1 {
            continue;
        }
        visited[current_vtx]=1;
        plan.push(current_vtx);
        for i in 0..pattern_size{
            if neighbor_matrix[i*pattern_size+current_vtx] ==1 && visited[i]==0 {
                plan_stack.push(i)
            }
        }

    }

    let pattern_size = pattern.get_vertices_num();
    let mut neighbor_matrix = vec![0;pattern_size*pattern_size];
    for i in pattern.edges_iter() {
        // print!("edge {:?}",i);
        let src_v = i.get_start_vertex().get_id();
        let dst_v = i.get_end_vertex().get_id();
        edge_table.insert((src_v as u32,dst_v as u32), i.get_label());
        edge_table.insert((dst_v as u32,src_v as u32), i.get_label());
        neighbor_matrix[src_v*pattern_size+dst_v]=1;
        neighbor_matrix[dst_v*pattern_size+src_v]=1;
    }
    let plan_len = plan.len();
    let mut plan_info = Vec::new();
    let mut parents = Vec::new();
    for i in 0..plan_len {
        let src_node_id = plan[i];
        let mut node_parent = Vec::new();
        for j in 0..pattern_size {
            if parents.contains(&j) && neighbor_matrix[src_node_id*pattern_size+j]==1 {
                node_parent.push(j);
            }
        }
        plan_info.push((src_node_id as u32,node_parent));
        parents.push(src_node_id);
        
    }
    plan_info
}

pub fn dfs_match_bf(pattern: &Pattern, src_graph: &LargeGraphDB, bf: &BloomFilter, v_size: usize, fp_rate: f64, print_time: bool) -> u64{
    let mut edge_table = HashMap::new();
    let plan_info = gen_plan(pattern, &mut edge_table);

    let query_execution_start_time = Instant::now();
    let count =recursive_dfs_bf(v_size, pattern, &edge_table, &plan_info, src_graph, bf, fp_rate);
    if print_time{
        print!(" {:?}", query_execution_start_time.elapsed().as_millis());
    }
    count
}

pub fn dfs_match_hs(pattern: &Pattern, src_graph: &LargeGraphDB, bf: &HashSet<usize>, v_size: usize, print_time: bool) -> u64{
    let mut edge_table = HashMap::new();
    let plan_info = gen_plan(pattern, &mut edge_table);

    let query_execution_start_time = Instant::now();
    let count =recursive_dfs_hs(v_size, pattern, &edge_table, &plan_info, src_graph, bf);
    if print_time{
        print!(" {:?}", query_execution_start_time.elapsed().as_millis());
    }
    count
}
pub fn dfs_match_ig(pattern: &Pattern, src_graph: &LargeGraphDB, print_time: bool) -> u64{
    let mut edge_table = HashMap::new();
    let plan_info = gen_plan(pattern, &mut edge_table);

    let query_execution_start_time = Instant::now();
    let count =recursive_dfs_ig(pattern, &edge_table, &plan_info, src_graph);
    if print_time{
        print!(" {:?}", query_execution_start_time.elapsed().as_millis());
    }
    count
}
pub fn dfs_match_ih(pattern: &Pattern, src_graph: &LargeGraphDB, print_time: bool) -> u64{
    let mut edge_table = HashMap::new();
    let plan_info = gen_plan(pattern, &mut edge_table);

    let query_execution_start_time = Instant::now();
    let count =recursive_dfs_ih(pattern, &edge_table, &plan_info, src_graph);
    if print_time{
        print!(" {:?}", query_execution_start_time.elapsed().as_millis());
    }
    count
}

pub fn recursive_dfs_ih(pattern: &Pattern, edge_table: &HashMap<(u32, u32), i32>, plan_info: &Vec<(u32, Vec<usize>)>, src_graph: &LargeGraphDB) -> u64{
    let mut path_stack = Vec::new();
    let mut result = 0;
    let vtx_num = pattern.get_vertices_num();
    let path= vec![0;vtx_num];
    // init
    let plan_index=0;
    let label = pattern.get_vertex(plan_info[plan_index].0 as usize).unwrap().get_label() as u8;
    let src_id = plan_info[plan_index].0;
    let init_vtx_set = src_graph.get_all_vertices(Some(&vec![label]));

    // let init_vtx_set = src_graph.get_all_vertices(None);
    for i in init_vtx_set{

        let mut new_path = path.clone();
        new_path[src_id as usize] = i.get_id();
        new_path.push(plan_index+1);
        path_stack.push(new_path);
    }
    // dfs
    while !path_stack.is_empty(){
        // print!("path len {:?}",path_stack.len());
        let mut path_item = path_stack.pop().unwrap();
        let plan_index = path_item.pop().unwrap();
        let path = path_item;
        let src_id = plan_info[plan_index].0;
        let parents = plan_info[plan_index].1.clone();

        // TO find min degree to start
        let mut intersect_list: Vec<HashSet<usize>> = Vec::new();
        let mut index_list = Vec::new();
        for i in parents.clone(){
            // print!("{:?} {:?}",src_id,i);
            let edge_label= edge_table[&(src_id,i as u32)] as u8;
            let cur_neg = src_graph.get_in_vertices(path[i as usize], Some(&vec![edge_label]));
            // let cur_neg = src_graph.get_in_vertices(path[i as usize], None);
            intersect_list.push(cur_neg.map(|f| f.get_id()).collect());
            index_list.push((index_list.len(),intersect_list[intersect_list.len()-1].len()));
        }
        index_list.sort_by(|a,b| a.1.cmp(&b.1));
        let mut match_result = intersect_list[index_list[0].0].clone();
        for (i,_j) in index_list.clone() {
            if i==index_list[0].0 {
                continue;
            }
            match_result = match_result.into_iter().filter(|f| {
                
                intersect_list[i].contains(f)
            }).collect();
        }
        for i in match_result {
            if plan_index< plan_info.len() -1{
                let mut new_path=path.clone();
                new_path[src_id as usize] = i;
                new_path.push(plan_index+1);
                path_stack.push(new_path);
            }
            else{
                let mut new_path=path.clone();
                new_path[src_id as usize] = i;
                result+=1;
            }
        }
        
    }
    result
}

pub fn recursive_dfs_ig(pattern: &Pattern, edge_table: &HashMap<(u32, u32), i32>, plan_info: &Vec<(u32, Vec<usize>)>, src_graph: &LargeGraphDB) -> u64{
    let mut path_stack = Vec::new();
    let mut result = 0;
    let vtx_num = pattern.get_vertices_num();
    let path= vec![0;vtx_num];
    // init
    let plan_index=0;
    let label = pattern.get_vertex(plan_info[plan_index].0 as usize).unwrap().get_label() as u8;
    let src_id = plan_info[plan_index].0;
    let init_vtx_set = src_graph.get_all_vertices(Some(&vec![label]));

    // let init_vtx_set = src_graph.get_all_vertices(None);
    for i in init_vtx_set{

        let mut new_path = path.clone();
        new_path[src_id as usize] = i.get_id();
        new_path.push(plan_index+1);
        path_stack.push(new_path);
    }
    // dfs
    while !path_stack.is_empty(){
        // print!("path len {:?}",path_stack.len());
        let mut path_item = path_stack.pop().unwrap();
        let plan_index = path_item.pop().unwrap();
        let path = path_item;
        let src_id = plan_info[plan_index].0;
        let parents = plan_info[plan_index].1.clone();

        // TO find min degree to start
        let mut intersect_list: Vec<Vec<usize>> = Vec::new();
        let mut index_list = Vec::new();
        for i in parents.clone(){
            // print!("{:?} {:?}",src_id,i);
            let edge_label= edge_table[&(src_id,i as u32)] as u8;
            let cur_neg = src_graph.get_in_vertices(path[i as usize], Some(&vec![edge_label]));
            // let cur_neg = src_graph.get_in_vertices(path[i as usize], None);
            intersect_list.push(cur_neg.map(|f| f.get_id()).collect());
            index_list.push((index_list.len(),intersect_list[intersect_list.len()-1].len()));
        }
        index_list.sort_by(|a,b| a.1.cmp(&b.1));
        let mut match_result = intersect_list[index_list[0].0].clone();
        for (i,_j) in index_list.clone() {
            if i==index_list[0].0 {
                continue;
            }
            let aaa = match_result.clone();
            match_result= Vec::new();
            // print!("{:?}",is_sorted(&intersect_list[i]));
            intersect(&aaa, &intersect_list[i], Some(& mut match_result));
        }
        for i in match_result {
            if plan_index< plan_info.len() -1{
                let mut new_path=path.clone();
                new_path[src_id as usize] = i;
                new_path.push(plan_index+1);
                path_stack.push(new_path);
            }
            else{
                let mut new_path=path.clone();
                new_path[src_id as usize] = i;
                result+=1;
            }
        }
        
    }
    result
}


pub fn recursive_dfs_bf(v_size: usize, pattern: &Pattern, edge_table: &HashMap<(u32, u32), i32>, plan_info: &Vec<(u32, Vec<usize>)>, src_graph: &LargeGraphDB,bf: &BloomFilter, fp_rate: f64) -> u64{
    let mut path_stack = Vec::new();
    let mut result = 0;
    let mut result2 =0;
    let vtx_num = pattern.get_vertices_num();
    let path= vec![0;vtx_num];
    // init
    let plan_index=0;
    let label = pattern.get_vertex(plan_info[plan_index].0 as usize).unwrap().get_label() as u8;
    let src_id = plan_info[plan_index].0;
    let init_vtx_set = src_graph.get_all_vertices(Some(&vec![label]));

    // let init_vtx_set = src_graph.get_all_vertices(None);
    for i in init_vtx_set{

        let mut new_path = path.clone();
        new_path[src_id as usize] = i.get_id();
        new_path.push(plan_index+1);
        path_stack.push(new_path);
    }
    // dfs
    while !path_stack.is_empty(){
        // print!("path len {:?}",path_stack.len());
        let mut path_item = path_stack.pop().unwrap();
        let plan_index = path_item.pop().unwrap();
        let path = path_item;
        let src_id = plan_info[plan_index].0;
        let parents = plan_info[plan_index].1.clone();

        // TO find min degree to start
        // let mut min_degree=MAX;
        // let mut min_deg_vtx =0;
        // let mut min_deg_edge = 0;
        // for i in parents.clone(){
        //     // print!("{:?} {:?}",src_id,i);
        //     let edge_label= edge_table[&(src_id,i as u32)] as u8;
        //     let cur_deg = src_graph.get_in_vertices(path[i as usize], Some(&vec![edge_label])).count();
        //     // let cur_deg = src_graph.get_in_vertices(path[i as usize], None).count();
        //     if cur_deg < min_degree {
        //         min_degree=cur_deg;
        //         min_deg_vtx = i;
        //         min_deg_edge = edge_label;
        //     }
        // }

        // let mut match_result  = src_graph.get_out_vertices(path[min_deg_vtx], Some(&vec![min_deg_edge])).map(|f| {
        // // let mut match_result  = src_graph.get_out_vertices(path[min_deg_vtx], None).map(|f| {
        //     f.get_id()
        // }).collect::<Vec<usize>>();
        let mut intersect_list: Vec<Vec<usize>> = Vec::new();
        let mut index_list = Vec::new();
        for i in parents.clone(){
            // print!("{:?} {:?}",src_id,i);
            let edge_label= edge_table[&(src_id,i as u32)] as u8;
            let cur_neg = src_graph.get_in_vertices(path[i as usize], Some(&vec![edge_label]));
            // let cur_neg = src_graph.get_in_vertices(path[i as usize], None);
            intersect_list.push(cur_neg.map(|f| f.get_id()).collect());
            index_list.push((index_list.len(),intersect_list[intersect_list.len()-1].len()));
        }
        index_list.sort_by(|a,b| a.1.cmp(&b.1));
        let mut match_result = intersect_list[index_list[0].0].clone();
        
        for i in 1..index_list.len() {
            let idx = parents[index_list[i].0];
            match_result = match_result.into_iter().filter(|f| {

                let edge_id = f*v_size+path[idx];
                let edge_id_rev = path[idx]*v_size + f;
                bf.contains(&edge_id) && bf.contains(&edge_id_rev)
            }).collect();
        }
        let mut local_result = 0;
        for i in match_result {
            if plan_index< plan_info.len() -1{
                let mut new_path=path.clone();
                new_path[src_id as usize] = i;
                new_path.push(plan_index+1);
                path_stack.push(new_path);
            }
            else{
                let mut new_path=path.clone();
                new_path[src_id as usize] = i;
                local_result+=1;
            }
        }
        if plan_index==plan_info.len()-1 {
            let divider = (1f64- fp_rate) as f64;
            let divided = local_result as f64 - index_list[0].1 as f64* fp_rate;
            result += (divided/ divider).ceil() as u64;
            result2 += local_result;
        }
    }
    // print!(" -{:?}-  -{:?}- ",result,result2);
    result
}


pub fn recursive_dfs_hs(v_size: usize, pattern: &Pattern, edge_table: &HashMap<(u32, u32), i32>, plan_info: &Vec<(u32, Vec<usize>)>, src_graph: &LargeGraphDB,bf: &HashSet<usize>) -> u64{
    let mut path_stack = Vec::new();
    let mut result = 0;
    let vtx_num = pattern.get_vertices_num();
    let path= vec![0;vtx_num];
    // init
    let plan_index=0;
    let label = pattern.get_vertex(plan_info[plan_index].0 as usize).unwrap().get_label() as u8;
    let src_id = plan_info[plan_index].0;
    let init_vtx_set = src_graph.get_all_vertices(Some(&vec![label]));

    // let init_vtx_set = src_graph.get_all_vertices(None);
    for i in init_vtx_set{

        let mut new_path = path.clone();
        new_path[src_id as usize] = i.get_id();
        new_path.push(plan_index+1);
        path_stack.push(new_path);
    }
    // dfs
    while !path_stack.is_empty(){
        // print!("path len {:?}",path_stack.len());
        let mut path_item = path_stack.pop().unwrap();
        let plan_index = path_item.pop().unwrap();
        let path = path_item;
        let src_id = plan_info[plan_index].0;
        let parents = plan_info[plan_index].1.clone();

        // TO find min degree to start
        // let mut min_degree=MAX;
        // let mut min_deg_vtx =0;
        // let mut min_deg_edge = 0;
        // for i in parents.clone(){
        //     // print!("{:?} {:?}",src_id,i);
        //     let edge_label= edge_table[&(src_id,i as u32)] as u8;
        //     let cur_deg = src_graph.get_in_vertices(path[i as usize], Some(&vec![edge_label])).count();
        //     // let cur_deg = src_graph.get_in_vertices(path[i as usize], None).count();
        //     if cur_deg < min_degree {
        //         min_degree=cur_deg;
        //         min_deg_vtx = i;
        //         min_deg_edge = edge_label;
        //     }
        // }
        // let mut match_result  = src_graph.get_out_vertices(path[min_deg_vtx], Some(&vec![min_deg_edge])).map(|f| {
        // // let mut match_result  = src_graph.get_out_vertices(path[min_deg_vtx], None).map(|f| {
        //     f.get_id()
        // }).collect::<Vec<usize>>();
        
        let mut intersect_list: Vec<Vec<usize>> = Vec::new();
        let mut index_list = Vec::new();
        for i in parents.clone(){
            // print!("{:?} {:?}",src_id,i);
            let edge_label= edge_table[&(src_id,i as u32)] as u8;
            let cur_neg = src_graph.get_in_vertices(path[i as usize], Some(&vec![edge_label]));
            // let cur_neg = src_graph.get_in_vertices(path[i as usize], None);
            intersect_list.push(cur_neg.map(|f| f.get_id()).collect());
            index_list.push((index_list.len(),intersect_list[intersect_list.len()-1].len()));
        }
        index_list.sort_by(|a,b| a.1.cmp(&b.1));
        let mut match_result = intersect_list[index_list[0].0].clone();
        
        for i in 1..index_list.len() {
            let idx = parents[index_list[i].0];
            match_result = match_result.into_iter().filter(|f| {

                let edge_id = f*v_size+path[idx];
                let edge_id_rev = path[idx]*v_size + f;
                bf.contains(&edge_id) && bf.contains(&edge_id_rev)
            }).collect();
        }
        for i in match_result {
            if plan_index< plan_info.len() -1{
                let mut new_path=path.clone();
                new_path[src_id as usize] = i;
                new_path.push(plan_index+1);
                path_stack.push(new_path);
            }
            else{
                let mut new_path=path.clone();
                new_path[src_id as usize] = i;
                result+=1;
            }
        }
        
    }
    result
}
