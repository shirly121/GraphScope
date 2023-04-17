use crate::error::{IrError, IrResult};
use ir_common::LabelId;
use std::collections::HashMap;

/// Define the visibility of vertices.
/// AllVisible indicates that the vertex info can be visible on any partition,
/// OneVisible indicates that the vertex info can only be visible on one partition.
/// AdjVisible indicates that the vertex info can be visible along with edges (which is a common case in VCUT strategy).
#[derive(Clone)]
pub enum VertexVisibility {
    AllVisible,
    OneVisible,
    AdjVisible,
}

/// Define the visibility of edges.
/// AllVisible indicates that the edge info can be visible on any partition,
/// OneVisible indicates that the edge info can only be visible on one partition.
/// Specifically, when edge info are visible along with vertices (which is a common case in ECut strategy),
/// it can be further defined as SrcVisible, DstVisible and BothVisible,
/// which indicates that the edge info can be visible on the src/dst/both vertex respectively.
#[derive(Clone)]
pub enum EdgeVisibility {
    AllVisible,
    OneVisible,
    SrcVisible,
    DstVisible,
    BothVisible,
}

/// Define the visibility of properties.
/// MasterMirrorVisible indicates that the property info can be visible on any vertex/edge replica,
/// including master and mirror;
/// MasterVisible indicates that the property info can only be visible on the master vertex/edge.
#[derive(Clone)]
pub enum PropertyVisibility {
    MasterMirrorVisible,
    MasterVisible,
}

/// ETripLabel is uniquely identified for edge with (srcLabelId, edgeLabelId, dstLabelId)
/// NOTICE that direction is prserved in ETripLabel.
pub type ETripLabel = (LabelId, LabelId, LabelId);

/// Query the info Visibility including:
/// 1) Vertex Topology Visibility,
/// 2) Edge Topology Visibility, and
/// 3) Properties Visibility
pub trait QueryVisibility {
    fn get_vertex_visibility(&self, vlabel: &LabelId) -> IrResult<VertexVisibility>;
    fn get_vertex_property_visibility(&self, vlabel: &LabelId) -> IrResult<PropertyVisibility>;
    fn get_edge_visibility(&self, elabel: &ETripLabel) -> IrResult<EdgeVisibility>;
    fn get_edge_property_visibility(&self, elabel: &ETripLabel) -> IrResult<PropertyVisibility>;
}

/// the topology partition strategy over graph
pub enum GraphTopoPartitionStrategy {
    AllReplicaStrategy,
    // ECutStrategy, which is to partition vertices into partitions, and edges may follow src, dst, or both.
    ECutStrategy(EdgeVisibility),
    // VCutStrategy
    VCutStrategy,
    // HybridStrategy, i.e., the topology partition strategy are defined per label.
    HybridStrategy(HybridTopoVisibility),
}

/// the property partition strategy over graph
pub enum GraphPropPartitionStrategy {
    // EntityPropPartition, i.e., (VertexPropertyVisibility, EdgePropertyVisibility)
    EntityPropStrategy((PropertyVisibility, PropertyVisibility)),
    // HybridPropStrategy, i.e., the property partition strategy are defined per label.
    HybridPropStrategy(HybridPropVisibility),
}

/// Hybrid Topology Visibility, where the visibility for topology and defined per label.
pub struct HybridTopoVisibility {
    vertex_topology_visibility: HashMap<LabelId, VertexVisibility>,
    edge_topology_visibility: HashMap<ETripLabel, EdgeVisibility>,
}

/// Hybrid Property Visibility, where the visibility for property and defined per label.
pub struct HybridPropVisibility {
    vertex_property_visibility: HashMap<LabelId, PropertyVisibility>,
    edge_property_visibility: HashMap<ETripLabel, PropertyVisibility>,
}

pub struct GraphPartitionStrategy {
    topo_strategy: GraphTopoPartitionStrategy,
    prop_strategy: GraphPropPartitionStrategy,
}

impl QueryVisibility for GraphPartitionStrategy {
    fn get_vertex_visibility(&self, vlabel: &LabelId) -> IrResult<VertexVisibility> {
        match &self.topo_strategy {
            GraphTopoPartitionStrategy::AllReplicaStrategy => Ok(VertexVisibility::AllVisible),
            GraphTopoPartitionStrategy::ECutStrategy(_) => Ok(VertexVisibility::OneVisible),
            GraphTopoPartitionStrategy::VCutStrategy => Ok(VertexVisibility::AdjVisible),
            GraphTopoPartitionStrategy::HybridStrategy(hybrid) => hybrid
                .vertex_topology_visibility
                .get(vlabel)
                .cloned()
                .ok_or(IrError::TableNotExist(vlabel.clone().into())),
        }
    }

    fn get_vertex_property_visibility(&self, vlabel: &LabelId) -> IrResult<PropertyVisibility> {
        match &self.prop_strategy {
            GraphPropPartitionStrategy::EntityPropStrategy((vprop_visibility, _)) => {
                Ok(vprop_visibility.clone())
            }
            GraphPropPartitionStrategy::HybridPropStrategy(hybrid) => hybrid
                .vertex_property_visibility
                .get(vlabel)
                .cloned()
                .ok_or(IrError::TableNotExist(vlabel.clone().into())),
        }
    }

    fn get_edge_visibility(&self, elabel: &ETripLabel) -> IrResult<EdgeVisibility> {
        match &self.topo_strategy {
            GraphTopoPartitionStrategy::AllReplicaStrategy => Ok(EdgeVisibility::AllVisible),
            GraphTopoPartitionStrategy::ECutStrategy(e_topo_visibility) => Ok(e_topo_visibility.clone()),
            GraphTopoPartitionStrategy::VCutStrategy => Ok(EdgeVisibility::OneVisible),
            GraphTopoPartitionStrategy::HybridStrategy(hybrid) => hybrid
                .edge_topology_visibility
                .get(elabel)
                .cloned()
                .ok_or(IrError::TableNotExist(elabel.1.clone().into())),
        }
    }

    fn get_edge_property_visibility(&self, elabel: &ETripLabel) -> IrResult<PropertyVisibility> {
        match &self.prop_strategy {
            GraphPropPartitionStrategy::EntityPropStrategy((_, eprop_visibility)) => {
                Ok(eprop_visibility.clone())
            }
            GraphPropPartitionStrategy::HybridPropStrategy(hybrid) => hybrid
                .edge_property_visibility
                .get(elabel)
                .cloned()
                .ok_or(IrError::TableNotExist(elabel.1.clone().into())),
        }
    }
}
