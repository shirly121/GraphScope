//
//! Copyright 2023 Alibaba Group Holding Limited.
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

mod common;

#[cfg(test)]
mod tests {
    use ir_core::FfiNameOrId;
    use ir_core::FfiVariable;
    use ir_common::generated::algebra as pb; 
    use ir_core::destroy_logical_plan;
    use ir_core::graph::*;
    use ir_core::init_logical_plan;
    use ir_core::plan::ffi::scan::*;
    use ir_core::plan::ffi::join::*;
    use ir_core::plan::ffi::sink::*;
    use ir_common::generated::common as common_pb;
    use ir_core::plan::logical::LogicalPlan;

  

    fn tag_gen(tag: &str) -> FfiNameOrId {
        let tag_pb = common_pb::NameOrId {
            item: Some(common_pb::name_or_id::Item::Name(tag.to_string())),
        }; 
        tag_pb.into()
    }

    fn variable_gen(tag: &str) -> FfiVariable {
        let mut variable = FfiVariable::default();
        let tag_pb = common_pb::NameOrId {
            item: Some(common_pb::name_or_id::Item::Name(tag.to_string())),
        }; 
        variable.tag = tag_pb.into();
        variable
    }

    #[test]
    fn test_match_ffi() {
        unsafe {
            let ptr_plan = init_logical_plan();

            // append 'V()' operator
            let ptr_scan = init_scan_operator(FfiScanOpt::Entity);
            let scan_id: *mut i32 = &mut 0;
            append_scan_operator(ptr_plan, ptr_scan, -1, scan_id);

            // build sentence: as('a').out().as('b')
            let ptr_expand = init_edgexpd_operator(FfiExpandOpt::Vertex, FfiDirection::Out);
            let ptr_sentence = init_pattern_sentence(FfiJoinKind::Inner);      
            set_sentence_start(ptr_sentence, tag_gen("a"));
            add_sentence_binder(ptr_sentence, ptr_expand, FfiBinderOpt::Edge);
            set_sentence_end(ptr_sentence, tag_gen("b"));

            // add sentence to left match
            let ptr_match = init_pattern_operator();
            add_pattern_sentence(ptr_match, ptr_sentence);
            let match_id: *mut i32 = &mut 0;
            append_pattern_operator(ptr_plan, ptr_match, *scan_id, match_id);

            // build sentence: as('c').out().as('b')
            let ptr_expand2 = init_edgexpd_operator(FfiExpandOpt::Vertex, FfiDirection::Out);
            let ptr_sentence2 = init_pattern_sentence(FfiJoinKind::Inner);
            set_sentence_start(ptr_sentence2, tag_gen("c"));
            add_sentence_binder(ptr_sentence2, ptr_expand2, FfiBinderOpt::Edge);
            set_sentence_end(ptr_sentence2, tag_gen("b"));

            // add sentence to right match
            let ptr_match2 = init_pattern_operator();
            add_pattern_sentence(ptr_match2, ptr_sentence2);
            let match_id2: *mut i32 = &mut 0;
            append_pattern_operator(ptr_plan, ptr_match2, *scan_id, match_id2);

            // append join operator which left is match1, right is match2
            let ptr_join = init_join_operator(FfiJoinKind::Inner);
            add_join_key_pair(ptr_join, variable_gen("b"), variable_gen("b"));
            let join_id: *mut i32 = &mut 0;
            append_join_operator(ptr_plan, ptr_join, *match_id, *match_id2, join_id);


            // append sink operator
            let ptr_sink = init_sink_operator();
            let sink_id: *mut i32 = &mut 0;
            append_sink_operator(ptr_plan, ptr_sink, *join_id, sink_id);

            // print plan
           
            let box_plan = Box::from_raw(ptr_plan as *mut LogicalPlan) ;
            let pb_plan: pb::LogicalPlan = box_plan.as_ref().clone().into();
            // let json_result = serde_json::to_string_pretty(&pb_plan);
            println!("{:#?}", pb_plan);

            destroy_logical_plan(ptr_plan);
        
    }

}

}
