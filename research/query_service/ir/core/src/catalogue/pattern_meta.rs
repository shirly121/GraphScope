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

use std::collections::{BTreeMap, BTreeSet};

use bimap::BiBTreeMap;

use crate::catalogue::{DynIter, PatternDirection, PatternLabelId};
use crate::plan::meta::KeyType;
use crate::plan::meta::Schema;

#[derive(Debug, Clone)]
pub struct PatternMeta {
    /// Key: vertex label name, Value: vertex labal id
    ///
    /// Usage: get a vertex label id from its label name
    vertex_label_map: BiBTreeMap<String, PatternLabelId>,
    /// Key: edge label name, Value: edge label id
    ///
    /// Usage: get an edge label id from its label name
    edge_label_map: BiBTreeMap<String, PatternLabelId>,
    /// Key: vertex label id, Value: BTreeSet<(edge label id, direction)>
    ///
    /// Usage: given a vertex(label id), find all its adjacent edges(label id) with directions
    v2edges_meta: BTreeMap<PatternLabelId, BTreeSet<(PatternLabelId, PatternDirection)>>,
    /// Key: edge label id, Value: Vec<(src vertex label id, dst vertex label id)>
    ///
    /// Usage: given an edge(label id):
    ///        find all its possible (src vertex label id, dst vertex label id) pairs
    e2vertices_meta: BTreeMap<PatternLabelId, Vec<(PatternLabelId, PatternLabelId)>>,
    /// Key: (src vertex label id, dst vertex label id), Value: Vec<(edge label id, direction)>
    ///
    /// Usage: given a (src vertex label id, dst vertex label id) pair:
    ///        find all possible edges(label id) between them
    vv2edges_meta: BTreeMap<(PatternLabelId, PatternLabelId), Vec<(PatternLabelId, PatternDirection)>>,
}

/// Initializer of PatternMeta
impl From<Schema> for PatternMeta {
    /// Pick necessary info from schema and reorganize them into the generated PatternMeta
    fn from(src_schema: Schema) -> PatternMeta {
        let (table_map, relation_labels) = src_schema.get_pattern_meta_info();
        let mut pattern_meta = PatternMeta {
            vertex_label_map: BiBTreeMap::new(),
            edge_label_map: BiBTreeMap::new(),
            v2edges_meta: BTreeMap::new(),
            e2vertices_meta: BTreeMap::new(),
            vv2edges_meta: BTreeMap::new(),
        };
        for (name, (key_type, id)) in &table_map {
            if let KeyType::Relation = key_type {
                // Case that this is an edge label
                let connections = relation_labels
                    .get(id)
                    .expect("Schema relation_bound_labels doesn't store edge info");
                pattern_meta
                    .edge_label_map
                    .insert(name.clone(), *id);
                for (start_v_meta, end_v_meta) in connections {
                    let start_v_name = start_v_meta.get_name();
                    let end_v_name = end_v_meta.get_name();
                    let start_v_id = src_schema.get_table_id(&start_v_name).unwrap();
                    let end_v_id = src_schema.get_table_id(&end_v_name).unwrap();
                    // Update connect information
                    pattern_meta
                        .v2edges_meta
                        .entry(start_v_id)
                        .or_insert(BTreeSet::new())
                        .insert((*id, PatternDirection::Out));
                    pattern_meta
                        .v2edges_meta
                        .entry(end_v_id)
                        .or_insert(BTreeSet::new())
                        .insert((*id, PatternDirection::In));
                    pattern_meta
                        .e2vertices_meta
                        .entry(*id)
                        .or_insert(vec![])
                        .push((start_v_id, end_v_id));
                    pattern_meta
                        .vv2edges_meta
                        .entry((start_v_id, end_v_id))
                        .or_insert(vec![])
                        .push((*id, PatternDirection::Out));
                    pattern_meta
                        .vv2edges_meta
                        .entry((end_v_id, start_v_id))
                        .or_insert(vec![])
                        .push((*id, PatternDirection::In));
                }
            } else if let KeyType::Entity = key_type {
                // Case that this is an vertex label
                pattern_meta
                    .vertex_label_map
                    .insert(name.clone(), *id);
            }
        }
        pattern_meta
    }
}

/// Iterators of fields in PatternMeta
impl PatternMeta {
    pub fn vertex_label_names_iter(&self) -> DynIter<&String> {
        Box::new(
            self.vertex_label_map
                .iter()
                .map(|(name, _)| name),
        )
    }

    pub fn edge_label_names_iter(&self) -> DynIter<&String> {
        Box::new(self.edge_label_map.iter().map(|(name, _)| name))
    }

    pub fn vertex_label_ids_iter(&self) -> DynIter<PatternLabelId> {
        Box::new(
            self.vertex_label_map
                .iter()
                .map(|(_, vertex_id)| *vertex_id),
        )
    }

    pub fn edge_label_ids_iter(&self) -> DynIter<PatternLabelId> {
        Box::new(
            self.edge_label_map
                .iter()
                .map(|(_, edge_id)| *edge_id),
        )
    }

    /// Given a soruce vertex label, iterate over all its neighboring adjacent vertices(label)
    pub fn adjacent_vlabels_iter(&self, src_v_label: PatternLabelId) -> DynIter<PatternLabelId> {
        match self.v2edges_meta.get(&src_v_label) {
            Some(adjacencies) => {
                let mut adjacent_vertices = BTreeSet::new();
                for (edge_id, dir) in adjacencies {
                    let possible_edges = self.e2vertices_meta.get(edge_id).unwrap();
                    for (start_v_id, end_v_id) in possible_edges {
                        if *start_v_id == src_v_label && *dir == PatternDirection::Out {
                            adjacent_vertices.insert(*end_v_id);
                        }
                        if *end_v_id == src_v_label && *dir == PatternDirection::In {
                            adjacent_vertices.insert(*start_v_id);
                        }
                    }
                }
                Box::new(adjacent_vertices.into_iter())
            }
            None => Box::new(std::iter::empty()),
        }
    }

    /// Given a source vertex label, iterate over all its neiboring adjacent edges(label)
    pub fn adjacent_elabels_iter(
        &self, src_v_label: PatternLabelId,
    ) -> DynIter<(PatternLabelId, PatternDirection)> {
        match self.v2edges_meta.get(&src_v_label) {
            Some(adjacencies) => Box::new(adjacencies.iter().cloned()),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Given a source edge label, iterate over all possible pairs of its (src vertex label, dst vertex label)
    pub fn associated_vlabels_iter_by_elabel(
        &self, src_e_label: PatternLabelId,
    ) -> DynIter<(PatternLabelId, PatternLabelId)> {
        match self.e2vertices_meta.get(&src_e_label) {
            Some(associations) => Box::new(associations.iter().cloned()),
            None => Box::new(std::iter::empty()),
        }
    }
    /// Given a src vertex label and a dst vertex label, iterate over all possible edges(label) between them with directions
    pub fn associated_elabels_iter_by_vlabel(
        &self, src_v_label: PatternLabelId, dst_v_label: PatternLabelId,
    ) -> DynIter<(PatternLabelId, PatternDirection)> {
        match self
            .vv2edges_meta
            .get(&(src_v_label, dst_v_label))
        {
            Some(edges) => Box::new(edges.iter().cloned()),
            None => Box::new(std::iter::empty()),
        }
    }
}

/// Methods for access some fields of PatternMeta or get some info from PatternMeta
impl PatternMeta {
    /// Get the number of vertex labels in the graph schema
    pub fn get_vertex_types_num(&self) -> usize {
        self.vertex_label_map.len()
    }

    /// Get the number of edge labels in the graph schema
    pub fn get_edge_types_num(&self) -> usize {
        self.edge_label_map.len()
    }

    pub fn get_vertex_label_id(&self, label_name: &str) -> Option<PatternLabelId> {
        self.vertex_label_map
            .get_by_left(label_name)
            .cloned()
    }

    pub fn get_edge_label_id(&self, label_name: &str) -> Option<PatternLabelId> {
        self.edge_label_map
            .get_by_left(label_name)
            .cloned()
    }

    pub fn get_vertex_label_name(&self, label_id: PatternLabelId) -> Option<String> {
        self.vertex_label_map
            .get_by_right(&label_id)
            .cloned()
    }

    pub fn get_edge_label_name(&self, label_id: PatternLabelId) -> Option<String> {
        self.edge_label_map
            .get_by_right(&label_id)
            .cloned()
    }

    /// Get the maximum vertex label id of the current pattern
    pub fn get_max_vertex_label(&self) -> Option<PatternLabelId> {
        match self.v2edges_meta.iter().last() {
            Some((max_label, _)) => Some(*max_label),
            None => None,
        }
    }

    /// Get the maximum edge label id of the current pattern
    pub fn get_max_edge_label(&self) -> Option<PatternLabelId> {
        match self.e2vertices_meta.iter().last() {
            Some((max_label, _)) => Some(*max_label),
            None => None,
        }
    }

    /// Compute at least how many bits are needed to represent vertex labels
    ///
    /// The result is at least 1 bit
    pub fn get_min_vertex_label_bit_num(&self) -> usize {
        if let Some(max_vertex_label) = self.get_max_vertex_label() {
            std::cmp::max((32 - max_vertex_label.leading_zeros()) as usize, 1)
        } else {
            1
        }
    }

    /// Compute at least how many bits are needed to represent edge labels
    ///
    /// The result is at least 1 bit
    pub fn get_min_edge_label_bit_num(&self) -> usize {
        if let Some(max_edge_label) = self.get_max_edge_label() {
            std::cmp::max((32 - max_edge_label.leading_zeros()) as usize, 1)
        } else {
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::PatternMeta;
    use crate::catalogue::test_cases::pattern_meta_cases::*;
    use crate::catalogue::{PatternDirection, PatternLabelId};

    /// Test whether the pattern meta from the modern graph obeys our expectation
    #[test]
    fn test_modern_graph_schema() {
        let modern_graph_schema = read_modern_graph_schema();
        let modern_pattern_meta = PatternMeta::from(modern_graph_schema);
        assert_eq!(modern_pattern_meta.get_edge_types_num(), 2);
        assert_eq!(modern_pattern_meta.get_vertex_types_num(), 2);
        assert_eq!(
            modern_pattern_meta
                .adjacent_elabels_iter(0)
                .collect::<Vec<(PatternLabelId, PatternDirection)>>()
                .len(),
            3
        );
        assert_eq!(
            modern_pattern_meta
                .adjacent_elabels_iter(1)
                .collect::<Vec<(PatternLabelId, PatternDirection)>>()
                .len(),
            1
        );
        assert_eq!(
            modern_pattern_meta
                .adjacent_vlabels_iter(0)
                .collect::<Vec<PatternLabelId>>()
                .len(),
            2
        );
        assert_eq!(
            modern_pattern_meta
                .adjacent_vlabels_iter(1)
                .collect::<Vec<PatternLabelId>>()
                .len(),
            1
        );
        assert_eq!(
            modern_pattern_meta
                .associated_elabels_iter_by_vlabel(0, 0)
                .collect::<Vec<(PatternLabelId, PatternDirection)>>()
                .len(),
            2
        );
        assert_eq!(
            modern_pattern_meta
                .associated_elabels_iter_by_vlabel(0, 1)
                .collect::<Vec<(PatternLabelId, PatternDirection)>>()
                .len(),
            1
        );
        assert_eq!(
            modern_pattern_meta
                .associated_elabels_iter_by_vlabel(1, 0)
                .collect::<Vec<(PatternLabelId, PatternDirection)>>()
                .len(),
            1
        );
    }

    /// Test whether the pattern meta from the ldbc graph obeys our expectation
    #[test]
    fn test_ldbc_graph_schema() {
        let ldbc_graph_schema = read_ldbc_graph_schema();
        let ldbc_pattern_meta = PatternMeta::from(ldbc_graph_schema.clone());
        assert_eq!(
            ldbc_pattern_meta
                .edge_label_ids_iter()
                .collect::<Vec<PatternLabelId>>()
                .len()
                + ldbc_pattern_meta
                    .vertex_label_ids_iter()
                    .collect::<Vec<PatternLabelId>>()
                    .len(),
            ldbc_graph_schema
                .get_pattern_meta_info()
                .0
                .len()
        );
        let all_vertex_names = ldbc_pattern_meta.vertex_label_names_iter();
        for vertex_name in all_vertex_names {
            let v_id_from_schema = ldbc_graph_schema
                .get_table_id(vertex_name)
                .unwrap();
            let v_id_from_pattern_meta = ldbc_pattern_meta
                .get_vertex_label_id(vertex_name)
                .unwrap();
            assert_eq!(v_id_from_schema, v_id_from_pattern_meta);
        }
        let all_edge_names = ldbc_pattern_meta.edge_label_names_iter();
        for edge_name in all_edge_names {
            let e_id_from_schema = ldbc_graph_schema
                .get_table_id(edge_name)
                .unwrap();
            let e_id_from_pattern_meta = ldbc_pattern_meta
                .get_edge_label_id(edge_name)
                .unwrap();
            assert_eq!(e_id_from_schema, e_id_from_pattern_meta);
        }
        let all_edge_ids = ldbc_pattern_meta.edge_label_ids_iter();
        let mut vertex_vertex_edges = BTreeMap::new();
        for edge_id in all_edge_ids {
            let edge_associate_vertices = ldbc_pattern_meta.associated_vlabels_iter_by_elabel(edge_id);
            for (start_v_id, end_v_id) in edge_associate_vertices {
                vertex_vertex_edges
                    .entry((start_v_id, end_v_id))
                    .or_insert(vec![])
                    .push((edge_id, PatternDirection::Out));
                vertex_vertex_edges
                    .entry((end_v_id, start_v_id))
                    .or_insert(vec![])
                    .push((edge_id, PatternDirection::In));
            }
        }
        for ((start_v_id, end_v_id), mut connections) in vertex_vertex_edges {
            let mut edges_between_vertices: Vec<(PatternLabelId, PatternDirection)> = ldbc_pattern_meta
                .associated_elabels_iter_by_vlabel(start_v_id, end_v_id)
                .collect();
            assert_eq!(connections.len(), edges_between_vertices.len());
            connections.sort();
            edges_between_vertices.sort();
            for i in 0..connections.len() {
                assert_eq!(connections[i], edges_between_vertices[i]);
            }
        }
        let all_vertex_ids = ldbc_pattern_meta.vertex_label_ids_iter();
        let mut vertex_vertex_edges = BTreeMap::new();
        for vertex_id in all_vertex_ids {
            let adjacent_edges = ldbc_pattern_meta.adjacent_elabels_iter(vertex_id);
            for (edge_id, dir) in adjacent_edges {
                let edges_with_dirs = ldbc_pattern_meta.associated_vlabels_iter_by_elabel(edge_id);
                for (start_v_id, end_v_id) in edges_with_dirs {
                    if start_v_id == vertex_id && dir == PatternDirection::Out {
                        vertex_vertex_edges
                            .entry((start_v_id, end_v_id))
                            .or_insert(vec![])
                            .push((edge_id, PatternDirection::Out));
                    }
                    if end_v_id == vertex_id && dir == PatternDirection::In {
                        vertex_vertex_edges
                            .entry((end_v_id, start_v_id))
                            .or_insert(vec![])
                            .push((edge_id, PatternDirection::In));
                    }
                }
            }
        }
        for ((start_v_id, end_v_id), mut connections) in vertex_vertex_edges {
            let mut edges_between_vertices: Vec<(PatternLabelId, PatternDirection)> = ldbc_pattern_meta
                .associated_elabels_iter_by_vlabel(start_v_id, end_v_id)
                .map(|(edge_label, dir)| (edge_label, dir))
                .collect();
            assert_eq!(connections.len(), edges_between_vertices.len());
            connections.sort();
            edges_between_vertices.sort();
            for i in 0..connections.len() {
                assert_eq!(connections[i], edges_between_vertices[i]);
            }
        }
    }
}
