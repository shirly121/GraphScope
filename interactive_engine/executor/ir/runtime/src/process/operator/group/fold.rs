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
use ir_common::generated::common as common_pb;
use ir_common::generated::physical as pb;
use ir_common::KeyId;
use pegasus::api::function::{FnResult, MapFunction};
use pegasus_server::job_pb as server_pb;

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::process::functions::FoldGen;
use crate::process::operator::accum::{AccumFactoryGen, RecordAccumulator};
use crate::process::operator::TagKey;
use crate::process::record::{Record, RecordKey};

#[derive(Debug, Default)]
pub struct FoldValue {
    values: Vec<TagKey>,
}

impl FoldValue {
    pub fn with(values_pb: Vec<common_pb::Variable>) -> Result<Self, ParsePbError> {
        let values = values_pb
            .into_iter()
            .map(|tag_key_pb| TagKey::try_from(tag_key_pb))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FoldValue { values })
    }
}

impl MapFunction<Record, RecordKey> for FoldValue {
    fn exec(&self, mut input: Record) -> FnResult<RecordKey> {
        let values = self
            .values
            .iter()
            .map(|key| key.get_arc_entry(&mut input))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecordKey::new(values))
    }
}

impl FoldGen<Record, RecordKey> for pb::GroupBy {
    fn gen_fold_value(&self) -> FnGenResult<Box<dyn MapFunction<Record, RecordKey>>> {
        let mut values = Vec::with_capacity(self.functions.len());
        for agg_func in &self.functions {
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
        let fold_values = FoldValue::with(values)?;
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime fold operator fold_values: {:?}", fold_values);
        }
        Ok(Box::new(fold_values))
    }

    fn get_accum_kind(&self) -> server_pb::AccumKind {
        let accum_functions = &self.functions;
        if accum_functions.len() == 1 {
            let agg_kind: pb::group_by::agg_func::Aggregate =
                unsafe { std::mem::transmute(accum_functions[0].aggregate) };
            match agg_kind {
                pb::group_by::agg_func::Aggregate::Count => server_pb::AccumKind::Cnt,
                _ => server_pb::AccumKind::Custom,
            }
        } else {
            server_pb::AccumKind::Custom
        }
    }

    fn gen_fold_map(&self) -> FnGenResult<Box<dyn MapFunction<RecordKey, Record>>> {
        let value_aliases = self
            .functions
            .iter()
            .map(|func| func.alias)
            .collect();
        let fold_map = FoldMap { value_aliases };
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime fold operator fold_map: {:?}", fold_map);
        }
        Ok(Box::new(fold_map))
    }

    fn gen_fold_accum(&self) -> FnGenResult<RecordAccumulator> {
        self.clone().gen_accum()
    }
}

#[derive(Debug)]
struct FoldMap {
    /// aliases for fold values, if some value is not not required to be preserved, give None alias
    value_aliases: Vec<Option<KeyId>>,
}

impl MapFunction<RecordKey, Record> for FoldMap {
    fn exec(&self, values: RecordKey) -> FnResult<Record> {
        if values.len() != self.value_aliases.len() {
            Err(FnExecError::unexpected_data_error(&format!(
                "fold_value.len()!=fold_value_aliases.len() {:?}, {:?}",
                values, self.value_aliases
            )))?
        }
        let mut record = Record::default();
        for (entry, alias) in values
            .into_iter()
            .zip(self.value_aliases.iter())
        {
            record.append_arc_entry(entry, alias.clone());
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use ir_common::generated::common as common_pb;
    use ir_common::generated::physical as pb;
    use pegasus::api::{Count, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;
    use pegasus_server::job_pb as server_pb;

    use crate::process::entry::Entry;
    use crate::process::functions::FoldGen;
    use crate::process::operator::tests::{init_source, TAG_A};
    use crate::process::record::Record;

    fn count_test(source: Vec<Record>, fold_opr_pb: pb::GroupBy) -> ResultStream<Record> {
        let conf = JobConf::new("fold_test");
        let result = pegasus::run(conf, || {
            let fold_opr_pb = fold_opr_pb.clone();
            let source = source.clone();
            move |input, output| {
                let mut stream = input.input_from(source.into_iter())?;
                if let server_pb::AccumKind::Cnt = fold_opr_pb.get_accum_kind() {
                    let fold_map = fold_opr_pb.gen_fold_map()?;
                    stream = stream
                        .count()?
                        .map(move |cnt| fold_map.exec(cnt))?
                        .into_stream()?;
                }
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        result
    }

    // g.V().count()
    #[test]
    fn count_opt_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 3, // count
            alias: None,
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let mut result = count_test(init_source(), fold_opr_pb);
        let mut cnt = 0;
        if let Some(Ok(record)) = result.next() {
            if let Some(entry) = record.get(None) {
                cnt = entry.as_object().unwrap().as_u64().unwrap();
            }
        }
        assert_eq!(cnt, 2);
    }

    // g.V().count().as("a")
    #[test]
    fn count_opt_with_alias_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 3, // count
            alias: Some(TAG_A.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let mut result = count_test(init_source(), fold_opr_pb);
        let mut cnt = 0;
        if let Some(Ok(record)) = result.next() {
            if let Some(entry) = record.get(Some(TAG_A)) {
                cnt = entry.as_object().unwrap().as_u64().unwrap();
            }
        }
        assert_eq!(cnt, 2);
    }
}
