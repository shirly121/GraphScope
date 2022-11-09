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

use std::borrow::BorrowMut;
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Add;
use std::sync::Arc;

use dyn_type::{BorrowObject, Object};
use graph_proxy::apis::{Edge, Element, GraphElement, GraphPath, Vertex, VertexOrEdge};
use graph_proxy::utils::expr::eval::Context;
use ir_common::error::ParsePbError;
use ir_common::generated::results as result_pb;
use ir_common::{KeyId, NameOrId};
use pegasus::api::function::DynIter;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus::Data;
use vec_map::VecMap;

use crate::process::operator::map::Intersection;

pub trait Entry:
    Data
    + Element
    + PartialEq
    + PartialOrd
    + From<Vertex>
    + From<Edge>
    + From<Intersection>
    + From<GraphPath>
    + From<Object>
    + From<VertexOrEdge>
{
    fn as_graph_vertex(&self) -> Option<&Vertex>;

    fn as_graph_edge(&self) -> Option<&Edge>;

    fn as_graph_path(&self) -> Option<&GraphPath>;

    fn as_object(&self) -> Option<&Object>;

    fn as_graph_path_mut(&mut self) -> Option<&mut GraphPath>;

    fn as_intersection(&self) -> Option<&Intersection>;

    fn as_intersection_mut(&mut self) -> Option<&mut Intersection>;

    fn as_collection(&self) -> Option<&Vec<Self>>;

    fn as_collection_mut(&mut self) -> Option<&mut Vec<Self>>;

    fn is_none(&self) -> bool;
}

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd)]
pub enum CompleteEntry {
    V(Vertex),
    E(Edge),
    P(GraphPath),
    OffGraph(Object),
    Intersection(Intersection),
    Collection(Vec<CompleteEntry>),
}

impl Entry for CompleteEntry {
    fn as_graph_vertex(&self) -> Option<&Vertex> {
        match self {
            CompleteEntry::V(v) => Some(v),
            _ => None,
        }
    }

    fn as_graph_edge(&self) -> Option<&Edge> {
        match self {
            CompleteEntry::E(e) => Some(e),
            _ => None,
        }
    }

    fn as_graph_path(&self) -> Option<&GraphPath> {
        match self {
            CompleteEntry::P(p) => Some(p),
            _ => None,
        }
    }

    fn as_object(&self) -> Option<&Object> {
        match self {
            CompleteEntry::OffGraph(object) => Some(object),
            _ => None,
        }
    }

    fn as_graph_path_mut(&mut self) -> Option<&mut GraphPath> {
        match self {
            CompleteEntry::P(graph_path) => Some(graph_path),
            _ => None,
        }
    }

    fn as_intersection(&self) -> Option<&Intersection> {
        match self {
            CompleteEntry::Intersection(intersection) => Some(intersection),
            _ => None,
        }
    }

    fn as_intersection_mut(&mut self) -> Option<&mut Intersection> {
        match self {
            CompleteEntry::Intersection(intersection) => Some(intersection),
            _ => None,
        }
    }

    fn as_collection(&self) -> Option<&Vec<CompleteEntry>> {
        match self {
            CompleteEntry::Collection(collection) => Some(collection),
            _ => None,
        }
    }

    fn as_collection_mut(&mut self) -> Option<&mut Vec<Self>> {
        match self {
            CompleteEntry::Collection(collection) => Some(collection),
            _ => None,
        }
    }

    fn is_none(&self) -> bool {
        match self {
            CompleteEntry::OffGraph(Object::None) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd)]
pub enum SimpleEntry {
    V(Vertex),
    I(Intersection),
    Null,
}

impl Entry for SimpleEntry {
    fn as_graph_vertex(&self) -> Option<&Vertex> {
        match self {
            SimpleEntry::V(v) => Some(v),
            _ => None,
        }
    }

    fn as_graph_edge(&self) -> Option<&Edge> {
        None
    }

    fn as_graph_path(&self) -> Option<&GraphPath> {
        None
    }

    fn as_object(&self) -> Option<&Object> {
        None
    }

    fn as_graph_path_mut(&mut self) -> Option<&mut GraphPath> {
        None
    }

    fn as_intersection(&self) -> Option<&Intersection> {
        match self {
            SimpleEntry::I(i) => Some(i),
            _ => None,
        }
    }

    fn as_intersection_mut(&mut self) -> Option<&mut Intersection> {
        match self {
            SimpleEntry::I(i) => Some(i),
            _ => None,
        }
    }

    fn as_collection(&self) -> Option<&Vec<Self>> {
        None
    }

    fn as_collection_mut(&mut self) -> Option<&mut Vec<Self>> {
        None
    }

    fn is_none(&self) -> bool {
        match self {
            SimpleEntry::Null => true,
            _ => false,
        }
    }
}

impl Element for SimpleEntry {
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        match self {
            SimpleEntry::V(v) => v.as_graph_element(),
            _ => None,
        }
    }

    fn len(&self) -> usize {
        match self {
            SimpleEntry::V(v) => v.len(),
            SimpleEntry::I(i) => i.len(),
            SimpleEntry::Null => 0,
        }
    }

    fn as_borrow_object(&self) -> BorrowObject {
        match self {
            SimpleEntry::V(v) => v.as_borrow_object(),
            _ => unreachable!(),
        }
    }
}

impl Encode for SimpleEntry {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            SimpleEntry::V(v) => {
                writer.write_u8(0)?;
                v.write_to(writer)?;
            }
            SimpleEntry::I(i) => {
                writer.write_u8(1)?;
                i.write_to(writer)?;
            }
            SimpleEntry::Null => {
                writer.write_u8(2)?;
            }
        }
        Ok(())
    }
}

impl Decode for SimpleEntry {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        match opt {
            0 => {
                let v = <Vertex>::read_from(reader)?;
                Ok(SimpleEntry::V(v))
            }
            1 => {
                let i = <Intersection>::read_from(reader)?;
                Ok(SimpleEntry::I(i))
            }
            2 => Ok(SimpleEntry::Null),
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl From<Vertex> for SimpleEntry {
    fn from(v: Vertex) -> Self {
        SimpleEntry::V(v)
    }
}

impl From<Intersection> for SimpleEntry {
    fn from(i: Intersection) -> Self {
        SimpleEntry::I(i)
    }
}

impl From<GraphPath> for SimpleEntry {
    fn from(_: GraphPath) -> Self {
        SimpleEntry::Null
    }
}

impl From<Object> for SimpleEntry {
    fn from(_: Object) -> Self {
        SimpleEntry::Null
    }
}

impl From<VertexOrEdge> for SimpleEntry {
    fn from(_: VertexOrEdge) -> Self {
        SimpleEntry::Null
    }
}

impl From<Edge> for SimpleEntry {
    fn from(_: Edge) -> Self {
        SimpleEntry::Null
    }
}

#[derive(Debug, Clone)]
pub struct Record<E: Entry> {
    curr: Option<Arc<E>>,
    columns: VecMap<Arc<E>>,
}

impl<E: Entry> Default for Record<E> {
    fn default() -> Self {
        Record { curr: None, columns: VecMap::new() }
    }
}

impl<E: Entry> Record<E> {
    pub fn new<IE: Into<E>>(entry: IE, tag: Option<KeyId>) -> Self {
        let entry = Arc::new(entry.into());
        let mut columns = VecMap::new();
        if let Some(tag) = tag {
            columns.insert(tag as usize, entry.clone());
        }
        Record { curr: Some(entry), columns }
    }

    /// A handy api to append entry of different types that can be turned into `Entry`
    pub fn append<IE: Into<E>>(&mut self, entry: IE, alias: Option<KeyId>) {
        self.append_arc_entry(Arc::new(entry.into()), alias)
    }

    pub fn append_arc_entry(&mut self, entry: Arc<E>, alias: Option<KeyId>) {
        if let Some(alias) = alias {
            self.columns
                .insert(alias as usize, entry.clone());
        }
        self.curr = Some(entry);
    }

    /// Set new current entry for the record
    pub fn set_curr_entry(&mut self, entry: Option<Arc<E>>) {
        self.curr = entry;
    }

    pub fn get_column_mut(&mut self, tag: &KeyId) -> Option<&mut E> {
        self.columns
            .get_mut(*tag as usize)
            .map(|e| Arc::get_mut(e))
            .unwrap_or(None)
    }

    pub fn get_columns_mut(&mut self) -> &mut VecMap<Arc<E>> {
        self.columns.borrow_mut()
    }

    pub fn get(&self, tag: Option<KeyId>) -> Option<&Arc<E>> {
        if let Some(tag) = tag {
            self.columns.get(tag as usize)
        } else {
            self.curr.as_ref()
        }
    }

    pub fn take(&mut self, tag: Option<&KeyId>) -> Option<Arc<E>> {
        if let Some(tag) = tag {
            self.columns.remove(*tag as usize)
        } else {
            self.curr.take()
        }
    }

    /// To join this record with `other` record. After the join, the columns
    /// from both sides will be merged (and deduplicated). The `curr` entry of the joined
    /// record will be specified according to `is_left_opt`, namely, if
    /// * `is_left_opt = None` -> set as `None`,
    /// * `is_left_opt = Some(true)` -> set as left record,
    /// * `is_left_opt = Some(false)` -> set as right record.
    pub fn join(mut self, mut other: Record<E>, is_left_opt: Option<bool>) -> Record<E> {
        for column in other.columns.drain() {
            if !self.columns.contains_key(column.0) {
                self.columns.insert(column.0, column.1);
            }
        }

        if let Some(is_left) = is_left_opt {
            if !is_left {
                self.curr = other.curr;
            }
        } else {
            self.curr = None;
        }

        self
    }
}

impl From<Vertex> for CompleteEntry {
    fn from(v: Vertex) -> Self {
        CompleteEntry::V(v)
    }
}

impl From<Edge> for CompleteEntry {
    fn from(e: Edge) -> Self {
        CompleteEntry::E(e)
    }
}

impl From<GraphPath> for CompleteEntry {
    fn from(p: GraphPath) -> Self {
        CompleteEntry::P(p)
    }
}

impl From<VertexOrEdge> for CompleteEntry {
    fn from(v_or_e: VertexOrEdge) -> Self {
        match v_or_e {
            VertexOrEdge::V(v) => v.into(),
            VertexOrEdge::E(e) => e.into(),
        }
    }
}

impl From<Object> for CompleteEntry {
    fn from(o: Object) -> Self {
        CompleteEntry::OffGraph(o)
    }
}

impl From<Intersection> for CompleteEntry {
    fn from(i: Intersection) -> Self {
        CompleteEntry::Intersection(i)
    }
}

impl<E: Entry> Context<E> for Record<E> {
    fn get(&self, tag: Option<&NameOrId>) -> Option<&E> {
        let tag = if let Some(tag) = tag {
            match tag {
                // TODO: may better throw an unsupported error if tag is a string_tag
                NameOrId::Str(_) => None,
                NameOrId::Id(id) => Some(*id),
            }
        } else {
            None
        };
        self.get(tag)
            .map(|entry| {
                if let (None, None) = (entry.as_collection(), entry.as_intersection()) {
                    Some(entry.as_ref())
                } else {
                    None
                }
            })
            .unwrap_or(None)
    }
}

impl Element for CompleteEntry {
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        match self {
            CompleteEntry::V(v) => v.as_graph_element(),
            CompleteEntry::E(e) => e.as_graph_element(),
            CompleteEntry::P(p) => p.as_graph_element(),
            CompleteEntry::OffGraph(_) => None,
            CompleteEntry::Collection(_) => None,
            CompleteEntry::Intersection(_) => None,
        }
    }

    fn len(&self) -> usize {
        match self {
            CompleteEntry::V(v) => v.len(),
            CompleteEntry::E(e) => e.len(),
            CompleteEntry::P(p) => p.len(),
            CompleteEntry::OffGraph(obj) => obj.len(),
            CompleteEntry::Collection(c) => c.len(),
            CompleteEntry::Intersection(i) => i.len(),
        }
    }

    fn as_borrow_object(&self) -> BorrowObject {
        match self {
            CompleteEntry::V(v) => v.as_borrow_object(),
            CompleteEntry::E(e) => e.as_borrow_object(),
            CompleteEntry::P(p) => p.as_borrow_object(),
            CompleteEntry::OffGraph(obj) => obj.as_borrow(),
            CompleteEntry::Collection(_) => unreachable!(),
            CompleteEntry::Intersection(_) => unreachable!(),
        }
    }
}

/// RecordKey is the key fields of a Record, with each key corresponding to a request column_tag
#[derive(Clone, Debug, Hash, PartialEq)]
pub struct RecordKey<E: Entry> {
    key_fields: Vec<Arc<E>>,
}

impl<E: Entry> RecordKey<E> {
    pub fn new(key_fields: Vec<Arc<E>>) -> Self {
        RecordKey { key_fields }
    }
    pub fn take(self) -> Vec<Arc<E>> {
        self.key_fields
    }
}

impl<E: Entry> Eq for RecordKey<E> {}
impl Eq for CompleteEntry {}

pub struct RecordExpandIter<E: Entry, IE> {
    tag: Option<KeyId>,
    origin: Record<E>,
    children: DynIter<IE>,
}

impl<E: Entry, IE> RecordExpandIter<E, IE> {
    pub fn new(origin: Record<E>, tag: Option<&KeyId>, children: DynIter<IE>) -> Self {
        RecordExpandIter { tag: tag.map(|e| e.clone()), origin, children }
    }
}

impl<E: Entry, IE: Into<E>> Iterator for RecordExpandIter<E, IE> {
    type Item = Record<E>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = self.origin.clone();
        match self.children.next() {
            Some(elem) => {
                record.append(elem, self.tag.clone());
                Some(record)
            }
            None => None,
        }
    }
}

pub struct RecordPathExpandIter<E: Entry, IE> {
    origin: Record<E>,
    curr_path: GraphPath,
    children: DynIter<IE>,
}

impl<E: Entry, IE> RecordPathExpandIter<E, IE> {
    pub fn new(origin: Record<E>, curr_path: GraphPath, children: DynIter<IE>) -> Self {
        RecordPathExpandIter { origin, curr_path, children }
    }
}

impl<IE: Into<CompleteEntry>> Iterator for RecordPathExpandIter<CompleteEntry, IE> {
    type Item = Record<CompleteEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = self.origin.clone();
        let mut curr_path = self.curr_path.clone();
        loop {
            match self.children.next() {
                Some(elem) => {
                    let graph_obj = elem.into();
                    match graph_obj {
                        CompleteEntry::V(v) => {
                            if curr_path.append(v) {
                                record.append(curr_path, None);
                                return Some(record);
                            }
                        }
                        CompleteEntry::E(e) => {
                            if curr_path.append(e) {
                                record.append(curr_path, None);
                                return Some(record);
                            }
                        }
                        _ => {}
                    }
                }
                None => return None,
            }
        }
    }
}

impl Encode for CompleteEntry {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            CompleteEntry::V(v) => {
                writer.write_u8(0)?;
                v.write_to(writer)?;
            }
            CompleteEntry::E(e) => {
                writer.write_u8(1)?;
                e.write_to(writer)?;
            }
            CompleteEntry::P(p) => {
                writer.write_u8(2)?;
                p.write_to(writer)?;
            }
            CompleteEntry::OffGraph(obj) => {
                writer.write_u8(3)?;
                obj.write_to(writer)?;
            }
            CompleteEntry::Collection(collection) => {
                writer.write_u8(4)?;
                collection.write_to(writer)?
            }
            CompleteEntry::Intersection(intersection) => {
                writer.write_u8(5)?;
                intersection.write_to(writer)?
            }
        }
        Ok(())
    }
}

impl Decode for CompleteEntry {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        match opt {
            0 => {
                let v = <Vertex>::read_from(reader)?;
                Ok(CompleteEntry::V(v))
            }
            1 => {
                let e = <Edge>::read_from(reader)?;
                Ok(CompleteEntry::E(e))
            }
            2 => {
                let p = <GraphPath>::read_from(reader)?;
                Ok(CompleteEntry::P(p))
            }
            3 => {
                let obj = <Object>::read_from(reader)?;
                Ok(CompleteEntry::OffGraph(obj))
            }
            4 => {
                let collection = <Vec<CompleteEntry>>::read_from(reader)?;
                Ok(CompleteEntry::Collection(collection))
            }
            5 => {
                let intersection = <Intersection>::read_from(reader)?;
                Ok(CompleteEntry::Intersection(intersection))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl<E: Entry> Encode for Record<E> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match &self.curr {
            None => {
                writer.write_u8(0)?;
            }
            Some(entry) => {
                writer.write_u8(1)?;
                entry.write_to(writer)?;
            }
        }
        writer.write_u64(self.columns.len() as u64)?;
        for (k, v) in self.columns.iter() {
            (k as KeyId).write_to(writer)?;
            v.write_to(writer)?;
        }
        Ok(())
    }
}

impl<E: Entry> Decode for Record<E> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        let curr = if opt == 0 { None } else { Some(Arc::new(<E>::read_from(reader)?)) };
        let size = <u64>::read_from(reader)? as usize;
        let mut columns = VecMap::with_capacity(size);
        for _i in 0..size {
            let k = <KeyId>::read_from(reader)? as usize;
            let v = <E>::read_from(reader)?;
            columns.insert(k, Arc::new(v));
        }
        Ok(Record { curr, columns })
    }
}

impl<E: Entry> Encode for RecordKey<E> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32(self.key_fields.len() as u32)?;
        for key in self.key_fields.iter() {
            (&**key).write_to(writer)?
        }
        Ok(())
    }
}

impl<E: Entry> Decode for RecordKey<E> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let len = reader.read_u32()?;
        let mut key_fields = Vec::with_capacity(len as usize);
        for _i in 0..len {
            let entry = <E>::read_from(reader)?;
            key_fields.push(Arc::new(entry))
        }
        Ok(RecordKey { key_fields })
    }
}

impl TryFrom<result_pb::Entry> for CompleteEntry {
    type Error = ParsePbError;

    fn try_from(entry_pb: result_pb::Entry) -> Result<Self, Self::Error> {
        if let Some(inner) = entry_pb.inner {
            match inner {
                result_pb::entry::Inner::Element(e) => Ok(e.try_into()?),
                result_pb::entry::Inner::Collection(c) => Ok(CompleteEntry::Collection(
                    c.collection
                        .into_iter()
                        .map(|e| e.try_into())
                        .collect::<Result<Vec<_>, Self::Error>>()?,
                )),
            }
        } else {
            Err(ParsePbError::EmptyFieldError("entry inner is empty".to_string()))?
        }
    }
}

impl TryFrom<result_pb::Element> for CompleteEntry {
    type Error = ParsePbError;
    fn try_from(e: result_pb::Element) -> Result<Self, Self::Error> {
        if let Some(inner) = e.inner {
            match inner {
                result_pb::element::Inner::Vertex(v) => Ok(CompleteEntry::V(v.try_into()?)),
                result_pb::element::Inner::Edge(e) => Ok(CompleteEntry::E(e.try_into()?)),
                result_pb::element::Inner::GraphPath(p) => Ok(CompleteEntry::P(p.try_into()?)),
                result_pb::element::Inner::Object(o) => Ok(CompleteEntry::OffGraph(o.try_into()?)),
            }
        } else {
            Err(ParsePbError::EmptyFieldError("element inner is empty".to_string()))?
        }
    }
}

impl Add for CompleteEntry {
    type Output = CompleteEntry;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (CompleteEntry::OffGraph(o1), CompleteEntry::OffGraph(o2)) => match (o1, o2) {
                (o, Object::None) | (Object::None, o) => o.into(),
                (Object::Primitive(p1), Object::Primitive(p2)) => Object::Primitive(p1.add(p2)).into(),
                _ => Object::None.into(),
            },
            _ => Object::None.into(),
        }
    }
}
