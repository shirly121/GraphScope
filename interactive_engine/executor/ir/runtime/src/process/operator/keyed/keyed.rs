//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use std::convert::TryFrom;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::common as common_pb;
use ir_common::generated::physical as pb;
use pegasus::api::function::FnResult;

use crate::error::{FnGenError, FnGenResult};
use crate::process::functions::KeyFunction;
use crate::process::operator::keyed::{KeyFunctionGen, KeyValueFunctionGen};
use crate::process::operator::TagKey;
use crate::process::record::{Record, RecordKey};

#[derive(Debug)]
pub struct KeySelector {
    keys: Vec<TagKey>,
}

impl KeySelector {
    pub fn with(keys_pb: Vec<common_pb::Variable>) -> Result<Self, ParsePbError> {
        let keys = keys_pb
            .into_iter()
            .map(|tag_key_pb| TagKey::try_from(tag_key_pb))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(KeySelector { keys })
    }
}

impl KeyFunction<Record, RecordKey, Record> for KeySelector {
    fn get_kv(&self, mut input: Record) -> FnResult<(RecordKey, Record)> {
        let keys = self
            .keys
            .iter()
            .map(|key| key.get_arc_entry(&mut input))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((RecordKey::new(keys), input))
    }
}

#[derive(Debug, Default)]
pub struct GroupKeyValueSelector {
    keys: Vec<TagKey>,
    values: Vec<TagKey>,
}

impl GroupKeyValueSelector {
    pub fn with_keys(mut self, keys_pb: Vec<common_pb::Variable>) -> Result<Self, ParsePbError> {
        let keys = keys_pb
            .into_iter()
            .map(|tag_key_pb| TagKey::try_from(tag_key_pb))
            .collect::<Result<Vec<_>, _>>()?;
        self.keys = keys;
        Ok(self)
    }

    pub fn with_values(mut self, values_pb: Vec<common_pb::Variable>) -> Result<Self, ParsePbError> {
        let values = values_pb
            .into_iter()
            .map(|tag_key_pb| TagKey::try_from(tag_key_pb))
            .collect::<Result<Vec<_>, _>>()?;
        self.values = values;
        Ok(self)
    }
}

impl KeyFunction<Record, RecordKey, RecordKey> for GroupKeyValueSelector {
    fn get_kv(&self, mut input: Record) -> FnResult<(RecordKey, RecordKey)> {
        let keys = self
            .keys
            .iter()
            .map(|key| key.get_arc_entry(&mut input))
            .collect::<Result<Vec<_>, _>>()?;
        let values = self
            .values
            .iter()
            .map(|key| key.get_arc_entry(&mut input))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((RecordKey::new(keys), RecordKey::new(values)))
    }
}

impl KeyValueFunctionGen for pb::GroupBy {
    fn gen_key(self) -> FnGenResult<Box<dyn KeyFunction<Record, RecordKey, RecordKey>>> {
        let keys = self
            .mappings
            .iter()
            .map(|mapping| mapping.key.clone().unwrap())
            .collect::<Vec<_>>();
        let mut values = Vec::with_capacity(self.functions.len());
        for agg_func in self.functions {
            if agg_func.vars.len() != 1 {
                // e.g., count_distinct((a,b));
                // TODO: to support this, we may need to define MultiTagKey (could define TagKey Trait, and impl for SingleTagKey and MultiTagKey)
                Err(FnGenError::unsupported_error(&format!(
                    "aggregate multiple fields in `Accum`, fields are {:?}",
                    agg_func.vars
                )))?
            }
            values.push(agg_func.vars.first().unwrap().clone());
        }
        let key_value_selector = GroupKeyValueSelector::default()
            .with_keys(keys)?
            .with_values(values)?;
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime group operator key_value_selector: {:?}", key_value_selector);
        }
        Ok(Box::new(key_value_selector))
    }
}

impl KeyFunctionGen for algebra_pb::Dedup {
    fn gen_key(self) -> FnGenResult<Box<dyn KeyFunction<Record, RecordKey, Record>>> {
        let key_selector = KeySelector::with(self.keys)?;
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime dedup operator key_selector: {:?}", key_selector);
        }
        Ok(Box::new(key_selector))
    }
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use dyn_type::Object;
    use graph_proxy::apis::GraphElement;
    use graph_proxy::apis::{DynDetails, Vertex, ID};
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::NameOrId;
    use pegasus::api::{Dedup, KeyBy, Map, Sink};
    use pegasus::JobConf;

    use crate::process::entry::Entry;
    use crate::process::operator::keyed::KeyFunctionGen;
    use crate::process::operator::tests::PERSON_LABEL;
    use crate::process::record::Record;

    fn source_gen() -> Box<dyn Iterator<Item = Record> + Send> {
        let p1: HashMap<NameOrId, Object> = vec![("age".into(), 27.into())]
            .into_iter()
            .collect();
        let p2: HashMap<NameOrId, Object> = vec![("age".into(), 29.into())]
            .into_iter()
            .collect();

        let v1 = Vertex::new(1, Some(PERSON_LABEL), DynDetails::new(p1));
        let v2 = Vertex::new(1, Some(PERSON_LABEL), DynDetails::new(p2.clone()));
        let v3 = Vertex::new(3, Some(PERSON_LABEL), DynDetails::new(p2));
        let r1 = Record::new(v1, None);
        let r2 = Record::new(v2, None);
        let r3 = Record::new(v3, None);
        Box::new(vec![r1, r2, r3].into_iter())
    }

    fn dedup_test(key_str: String, expected_ids: Vec<ID>) {
        let conf = JobConf::new("dedup_test");
        let mut result = pegasus::run(conf, || {
            let key_str = key_str.clone();
            move |input, output| {
                let mut stream = input.input_from(source_gen())?;
                let dedup_opr_pb = pb::Dedup { keys: vec![common_pb::Variable::from(key_str.clone())] };
                let selector = dedup_opr_pb.clone().gen_key().unwrap();
                stream = stream
                    .key_by(move |record| selector.get_kv(record))?
                    .dedup()?
                    .map(|pair| Ok(pair.value))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_vertex() {
                result_ids.push(element.id());
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_ids);
    }

    // g.V().dedup()
    #[test]
    fn dedup_simple_test() {
        let key_str = "@".to_string();
        let expected_result = vec![1, 3];
        dedup_test(key_str, expected_result)
    }

    // g.V().dedup().by('age')
    #[test]
    fn dedup_by_property_test() {
        let key_str = "@.age".to_string();
        let expected_result = vec![1, 1];
        dedup_test(key_str, expected_result)
    }
}
