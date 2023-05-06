use grin::grin_graph_proxy::GrinVertexProxy;
use grin::grin_v6d::*;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use grin::native_utils::*;
use dyn_type::Object;


#[derive(Debug)]
pub struct Vertex {
  id: u64,
  label: Option<GrinVertexTypeId>,
  properties: HashMap<String, Object>
}

#[derive(Debug)]
pub struct Edge {
  id: u64,
  label: Option<GrinEdgeTypeId>,
  src_id: u64,
  dst_id: u64,
}

fn main() {
    let rust_str_1 = "/home/graphscope/gie-grin/temp/vineyard.sock";
    let rust_str_2 = "130878194502466848";

    unsafe {
        let mut arr: [*const c_char; 2] = [string_rust2c(rust_str_1), string_rust2c(rust_str_2)];
        // let mut arr: [*const c_char; 2] = [c_str_ptr_1, c_str_ptr_2];
        let arr_mut_ptr: *mut *mut c_char = arr.as_mut_ptr() as *mut *mut c_char;
        let pg = grin_get_partitioned_graph_from_storage(2, arr_mut_ptr);

        // traverse(pgraph)
        let vertices = scan_vertex(pg, vec![0], vec![0, 1]);
        println!("{:?}", vertices);

        grin_destroy_partitioned_graph(pg);
    }
}

unsafe fn scan_vertex(
  pg: GrinPartitionedGraph,
  partition_ids: Vec<GrinPartitionId>,
  labels: Vec<GrinVertexTypeId>,
) -> Vec<Vertex> {
  let mut vec = vec![];
  for partition_id in partition_ids {
    let partition = grin_get_partition_from_id(pg, partition_id);
    let local_graph = grin_get_local_graph_from_partition(pg, partition);
    let vlist = grin_get_vertex_list(local_graph);
    for label in &labels {
      let vtype = grin_get_vertex_type_from_id(local_graph, *label);
      if vtype.is_null() {
        continue;  // return some error
      }
      let vtypelist = grin_select_type_for_vertex_list(local_graph, vtype, vlist);
      if vtypelist.is_null() {
        continue;  // return some error
      }
      let vtypelist_size = grin_get_vertex_list_size(local_graph, vtypelist);
      for i in 0..vtypelist_size {
        let v = grin_get_vertex_from_list(local_graph, vtypelist, i);
        let vertex_proxy = GrinVertexProxy::new(local_graph, v);

        vec.push(Vertex {
          id: vertex_proxy.get_id() as u64,
          label: Some(vertex_proxy.get_label_id()),
          properties: vertex_proxy.get_properties().unwrap(),
        });
      }
      grin_destroy_vertex_list(local_graph, vtypelist);
      grin_destroy_vertex_type(local_graph, vtype);
    }
    grin_destroy_vertex_list(local_graph, vlist);
    grin_destroy_partition(pg, partition);
  }

  vec
}

/*
unsafe fn scan_edge(
  pg: GrinPartitionedGraph,
  partition_ids: Vec<GrinPartitionId>,
  labels: Vec<GrinEdgeTypeId>) -> Vec<Edge> {
    let mut vec = vec![];
    for partition_id in partition_ids {
      let partition = grin_get_partition_from_id(pg, partition_id);
      let local_graph = grin_get_local_graph_from_partition(pg, partition);
      for label in &labels {
        let etype = grin_get_edge_type_from_id(local_graph, *label);
        let elist = grin_select_type_for_vertex_list(local_graph, vtype, vlist);
        let vlist_size = grin_get_vertex_list_size(local_graph, vlist);
        for i in 0..vlist_size {
          let v = grin_get_vertex_from_list(local_graph, vlist, i);
          let v_id = grin_get_vertex_original_id(local_graph, v);

          vec.push(Vertex {
            id: grin_get_uint64(v_id),
            label: Some(*label),
          });

          grin_destroy_vertex(local_graph, v);
          grin_destroy_vertex_original_id(local_graph, v_id);
        }
        grin_destroy_vertex_list(local_graph, vlist);
        grin_destroy_vertex_type(local_graph, vtype);
      }
      grin_destroy_partition(pg, partition);
    }

    vec
  }

// TODO
// unsafe fn index_scan_vertex(property_values: Object) -> Vec<Vertex> { vec![] }

unsafe fn traverse(pg: GrinPartitionedGraph) {
    let local_partitions = grin_get_local_partition_list(pg);
    let pnum = grin_get_partition_list_size(pg, local_partitions);
    println!("partition num: {:?}", pnum);
    let vnum = grin_get_total_vertex_num(pg);
    println!("vertex num: {:?}", vnum);
    let enumber = grin_get_total_edge_num(pg);
    println!("edge num: {:?}", enumber);

    let partition = grin_get_partition_from_list(pg, local_partitions, 0);
    // LOG(INFO) << "Partition: " << *(static_cast<unsigned*>(partition));
    // sync_property(pg, partition, "knows", "age");
    get_vertex_property(pg, partition);
}




unsafe fn get_vertex_property(
  partitioned_graph: GrinPartitionedGraph,
  partition: GrinPartition
) {
  let g = grin_get_local_graph_from_partition(partitioned_graph, partition); // get local graph of partition
  let vtypes = grin_get_vertex_type_list(g);  // get vertex type list
  let vtypes_num = grin_get_vertex_type_list_size(g, vtypes);
  println!("vtypes_num: {:?}", vtypes_num);

  for i in 0 .. vtypes_num {
    let vtype = grin_get_vertex_type_from_list(g, vtypes, i);  // get vertex type
    let vpt = grin_get_vertex_property_table_by_type(g, vtype);  // prepare property table of vertex type for later use
    let vlist = grin_get_vertex_list(g);
    let type_vlist = grin_select_type_for_vertex_list(g, vtype, vlist);
    let type_vlist_num = grin_get_vertex_list_size(g, type_vlist);

    println!("type_vlist_num: {:?}", type_vlist_num);

    let v_props = grin_get_vertex_property_list_by_type(g, vtype);
    let v_props_num = grin_get_vertex_property_list_size(g, v_props);

    println!("v_props_num: {:?}", v_props_num);

    for j in 0 .. type_vlist_num {
      let v = grin_get_vertex_from_list(g, type_vlist, j);
      for k in 0 ..v_props_num {
        let v_prop = grin_get_vertex_property_from_list(g, v_props, k);
        let v_prop_name = grin_get_vertex_property_name(g, v_prop);
        println!("prop name: {:?}", string_c2rust(v_prop_name));
        let v_prop_dt = grin_get_vertex_property_data_type(g, v_prop);
        println!("prop dt: {:?}", v_prop_dt);
        if v_prop_dt == GRIN_DATATYPE_INT64 {
        let v_prop_value = grin_get_value_from_vertex_property_table(g, vpt, v, v_prop);
        if v_prop_dt == GRIN_DATATYPE_INT64 {
          println!("v_prop_name: {:?}, v_prop_dt: {:?}, v_prop_value: {:?}", string_c2rust(v_prop_name), v_prop_dt,
          grin_get_int64(v_prop_value as *mut c_void));
        } else if v_prop_dt == GRIN_DATATYPE_STRING {
          continue;
          println!("v_prop_name: {:?}, v_prop_dt: {:?}, v_prop_value: {:?}", string_c2rust(v_prop_name), v_prop_dt,
          string_c2rust(grin_get_string(v_prop_value as *mut c_void)));
        } else {
          println!("unsupported type: {:?}", v_prop_dt);
        }
      }
        // println!("v_prop_name: {:?}, v_prop_dt: {:?}, v_prop_value: {:?}", string_c2rust(v_prop_name), v_prop_dt, v_prop_value);
      }
    }


    /*


    let vpt_num = grin_get_vertex_property_table_size(g, vpt);
    println!("vpt_num: {:?}", vpt_num);

    for j in 0 .. vpt_num {
      let vp = grin_get_vertex_property_from_table(g, vpt, j);  // get property
      let vp_name = grin_get_vertex_property_name(g, vp);  // get property name
      let vp_dt = grin_get_vertex_property_data_type(g, vp);  // get property type
      println!("vp_name: {:?}, vp_dt: {:?}", string_c2rust(vp_name), vp_dt);
    }
    */
  }
}

unsafe fn sync_property(
    partitioned_graph: GrinPartitionedGraph,
    partition: GrinPartition,
    rust_edge_type_name: &str,
    rust_vertex_property_name: &str,
) {
    let g = grin_get_local_graph_from_partition(partitioned_graph, partition); // get local graph of partition
    let edge_type_name = string_rust2c(rust_edge_type_name);
    let vertex_property_name = string_rust2c(rust_vertex_property_name);

    let etype = grin_get_edge_type_by_name(g, edge_type_name); // get edge type from name
    println!("{:?}", grin_get_edge_type_id(g, etype));
    println!("{:?}", string_c2rust(grin_get_edge_type_name(g, etype)));

    let src_vtypes = grin_get_src_types_from_edge_type(g, etype);  // get related source vertex type list
    let dst_vtypes = grin_get_dst_types_from_edge_type(g, etype);  // get related destination vertex type list

    let src_vtypes_num = grin_get_vertex_type_list_size(g, src_vtypes);
    let dst_vtypes_num = grin_get_vertex_type_list_size(g, dst_vtypes);
    println!("src_vtypes_num: {:?}, dst_vtypes_num: {:?}", src_vtypes_num, dst_vtypes_num);

    for i in 0 .. src_vtypes_num {
      let src_vtype = grin_get_vertex_type_from_list(g, src_vtypes, i);  // get src type
      let dst_vtype = grin_get_vertex_type_from_list(g, dst_vtypes, i);  // get dst type
      let dst_vp = grin_get_vertex_property_by_name(g, dst_vtype, vertex_property_name);  // get the property called "features" under dst type
      if dst_vp.is_null() {
        println!("the property is null");
        continue;
      } else {
        println!("the property is not null");
      }

      let dst_vpt = grin_get_vertex_property_table_by_type(g, dst_vtype);  // prepare property table of dst vertex type for later use
      let dst_vp_dt = grin_get_vertex_property_data_type(g, dst_vp); // prepare property type for later use

      let __src_vl = grin_get_vertex_list(g);  // get the vertex list
      let _src_vl = grin_select_type_for_vertex_list(g, src_vtype, __src_vl);  // filter the vertex of source type
      let src_vl = grin_select_master_for_vertex_list(g, _src_vl);  // filter master vertices under source type

      let src_vl_num = grin_get_vertex_list_size(g, src_vl);

      for j in 0 .. src_vl_num { // iterate the src vertex
        let v = grin_get_vertex_from_list(g, src_vl, j);
        #[cfg(feature = "grin_trait_select_edge_type_for_adjacent_list")]
        let adj_list = {
          let  _adj_list = grin_get_adjacent_list(g, GRIN_DIRECTION_OUT, v);  // get the outgoing adjacent list of v
          grin_select_edge_type_for_adjacent_list(g, etype, _adj_list)  // filter edges under etype
        };

        #[cfg(not(feature = "grin_trait_select_edge_type_for_adjacent_list"))]
        let adj_lsit = grin_get_adjacent_list(g, GRIN_DIRECTION_OUT, v);  // get the outgoing adjacent list of v

        let al_sz = grin_get_adjacent_list_size(g, adj_list);

        for k in 0 .. al_sz {
          #[cfg(feature = "grin_trait_select_edge_type_for_adjacent_list")]
          {
            let edge = grin_get_edge_from_adjacent_list(g, adj_list, k);
            let edge_type = grin_get_edge_type(g, edge);
            if (!grin_equal_edge_type(g, edge_type, etype)) {
              continue;
            }
          }

          let u = grin_get_neighbor_from_adjacent_list(g, adj_list, k);  // get the dst vertex u
          let value = grin_get_value_from_vertex_property_table(g, dst_vpt, u, dst_vp);  // get the property value of "features" of u

          let uref = grin_get_vertex_ref_for_vertex(g, u);  // get the reference of u that can be recoginized by other partitions
          let u_master_partition = grin_get_master_partition_from_vertex_ref(g, uref);  // get the master partition for u
          let uref_ser = grin_serialize_vertex_ref(g, uref);

          if dst_vp_dt == GRIN_DATATYPE_INT32 {
            println!( "Message: {:?}, {:?}", uref_ser, value);
          }
      }
    }
  }
    // send_value(u_master_partition, uref, dst_vp_dt, value);  // the value must be casted to the correct type based on dst_vp_dt before sending
  let _ = string_c2rust(edge_type_name);
}

*/
