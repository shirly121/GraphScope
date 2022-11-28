use graph_store::prelude::*;
use pegasus::api::{Count, Map, Sink};
use pegasus::result::ResultStream;
use pegasus::JobConf;

use crate::queries::graph::*;
use graph_proxy::apis::{read_id, write_id, Details, GraphElement, ID};
use graph_proxy::to_runtime_vertex;
use ir_common::KeyId;
use itertools::__std_iter::Iterator;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use std::sync::Arc;
use vec_map::VecMap;

#[derive(Debug, Clone, Default)]
struct SimpleEntry {
    id: ID,
}

impl From<ID> for SimpleEntry {
    fn from(id: u64) -> Self {
        SimpleEntry { id }
    }
}

impl SimpleEntry {
    fn as_id(&self) -> ID {
        self.id
    }
}

#[derive(Debug, Clone, Default)]
struct SimpleRecord {
    curr: Arc<SimpleEntry>,
    columns: VecMap<Arc<SimpleEntry>>,
}

impl SimpleRecord {
    fn new<E: Into<SimpleEntry>>(entry: E, tag: Option<KeyId>) -> Self {
        let entry = Arc::new(entry.into());
        let mut columns = VecMap::new();
        if let Some(tag) = tag {
            columns.insert(tag as usize, entry.clone());
        }
        SimpleRecord { curr: entry, columns }
    }

    fn get(&self, tag: Option<KeyId>) -> Option<&Arc<SimpleEntry>> {
        if let Some(tag) = tag {
            self.columns.get(tag as usize)
        } else {
            Some(&self.curr)
        }
    }

    /// A handy api to append entry of different types that can be turned into `Entry`
    pub fn append<E: Into<SimpleEntry>>(&mut self, entry: E, alias: Option<KeyId>) {
        self.append_arc_entry(Arc::new(entry.into()), alias)
    }

    pub fn append_arc_entry(&mut self, entry: Arc<SimpleEntry>, alias: Option<KeyId>) {
        self.curr = entry.clone();
        if let Some(alias) = alias {
            self.columns.insert(alias as usize, entry);
        }
    }
}

impl Encode for SimpleEntry {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        write_id(writer, self.id)
    }
}

impl Decode for SimpleEntry {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let id = read_id(reader)?;
        Ok(SimpleEntry { id })
    }
}

impl Encode for SimpleRecord {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.curr.write_to(writer)?;
        writer.write_u32(self.columns.len() as u32)?;
        for (k, v) in self.columns.iter() {
            (k as KeyId).write_to(writer)?;
            v.write_to(writer)?;
        }
        Ok(())
    }
}

impl Decode for SimpleRecord {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let entry = <SimpleEntry>::read_from(reader)?;
        let size = <u32>::read_from(reader)? as usize;
        let mut columns = VecMap::with_capacity(size);
        for _i in 0..size {
            let k = <KeyId>::read_from(reader)? as usize;
            let v = <SimpleEntry>::read_from(reader)?;
            columns.insert(k, Arc::new(v));
        }
        Ok(SimpleRecord { curr: Arc::new(entry), columns })
    }
}

fn to_id_only_vertex(vertex: LocalVertex<DefaultId>) -> u64 {
    vertex.get_id() as u64
}

/// g.V().hasLabel("PERSON").in("HASCREATOR").hasLabel("POST").as("a").in("REPLYOF").select("a").values("id").count()
/// the Record Version. FilterPushDown + ID-only Record
pub fn khop_record_recordopt(conf: JobConf) -> ResultStream<u64> {
    let workers = conf.workers;

    let schema = &GRAPH.graph_schema;
    let person_label = schema.get_vertex_label_id("PERSON").unwrap();
    let post_label = schema.get_vertex_label_id("POST").unwrap();
    let hascreator_label = schema.get_edge_label_id("HASCREATOR").unwrap();
    let replyof_label = schema.get_edge_label_id("REPLYOF").unwrap();

    pegasus::run(conf, || {
        move |input, output| {
            let worker_id = input.get_worker_index();
            let servers_len = pegasus::get_servers_len();
            let person_count = GRAPH
                .get_all_vertices(Some(&vec![person_label]))
                .count();
            let partial_count = person_count / workers as usize;
            let last_partial_count = person_count - partial_count * (workers as usize - 1);
            let all_persons = GRAPH
                .get_all_vertices(Some(&vec![person_label]))
                .map(|v| to_id_only_vertex(v))
                .map(|v| SimpleRecord::new(v, None));
            let last_worker_of_current_server = workers * (pegasus::get_current_worker().server_id + 1) - 1;
            let source = if worker_id == last_worker_of_current_server {
                all_persons
                    .skip((worker_id % workers) as usize * partial_count)
                    .take(last_partial_count)
            } else {
                all_persons
                    .skip((worker_id % workers) as usize * partial_count)
                    .take(partial_count)
            };

            // g.V().hasLabel("PERSON")
            let stream = input.input_from(source)?;
            stream
                // .in("HASCREATOR").hasLabel("POST").as("a")
                .flat_map(move |person_record: SimpleRecord| {
                    let person_id = person_record.get(None).unwrap().as_id();
                    let posts = GRAPH
                        .get_in_vertices(person_id as DefaultId, Some(&vec![hascreator_label]))
                        .filter(move |post_vertex| post_vertex.get_label()[0] == post_label)
                        .map(|v| to_id_only_vertex(v))
                        .map(move |post_vertex| {
                            let mut new_record = person_record.clone();
                            new_record.append(post_vertex, Some(0));
                            new_record
                        });
                    Ok(posts)
                })?
                // .in("REPLYOF")
                .repartition(move |r: &SimpleRecord| {
                    let id = r.get(None).unwrap().as_id();
                    Ok(get_partition(&id, workers as usize, servers_len))
                })
                .flat_map(move |post_record: SimpleRecord| {
                    let post_id = post_record.get(None).unwrap().as_id();
                    let post_messages = GRAPH
                        .get_in_vertices(post_id as DefaultId, Some(&vec![replyof_label]))
                        .map(move |message_vertex| to_id_only_vertex(message_vertex))
                        .map(move |message_vertex| {
                            let mut new_record = post_record.clone();
                            new_record.append(message_vertex, None);
                            new_record
                        });
                    Ok(post_messages)
                })?
                //.select("a").values("id")
                .repartition(move |message_record: &SimpleRecord| {
                    let id = message_record.get(Some(0)).unwrap().as_id();
                    Ok(get_partition(&id, workers as usize, servers_len))
                })
                // late project: fused of auxilia + project
                .map(|mut message_record: SimpleRecord| {
                    let post_id = message_record.get(Some(0)).unwrap().as_id();
                    let post_vertex = GRAPH.get_vertex(post_id as DefaultId).unwrap();
                    let runtime_post_vertex = to_runtime_vertex(post_vertex, None);
                    let post_property = runtime_post_vertex
                        .details()
                        .unwrap()
                        .get_property(&"id".into())
                        .unwrap()
                        .try_to_owned()
                        .unwrap();
                    //     message_record.append(post_property, None);
                    //    Ok(message_record)
                    Ok(post_property)
                })?
                .count()?
                .sink_into(output)
        }
    })
    .expect("submit bi2 job failure")
}
