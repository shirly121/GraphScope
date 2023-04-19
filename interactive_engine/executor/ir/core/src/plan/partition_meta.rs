use std::collections::HashMap;

use ir_common::LabelId;

use crate::error::{IrError, IrResult};

/// Define the distribution of vertices.
/// AllVisible indicates that the vertex info can be visible on any partition,
/// OneVisible indicates that the vertex info can only be visible on one partition.
/// AdjVisible indicates that the vertex info can be visible along with edges (which is a common case in VCUT strategy).
#[derive(Clone)]
pub enum VertexDistribution {
    AllVisible,
    OneVisible,
    AdjVisible,
}

/// Define the distribution of edges.
/// AllVisible indicates that the edge info can be visible on any partition,
/// OneVisible indicates that the edge info can only be visible on one partition.
/// Specifically, when edge info are visible along with vertices (which is a common case in ECut strategy),
/// it can be further defined as SrcVisible, DstVisible and BothVisible,
/// which indicates that the edge info can be visible on the src/dst/both vertex respectively.
#[derive(Clone)]
pub enum EdgeDistribution {
    AllVisible,
    OneVisible,
    SrcVisible,
    DstVisible,
    BothVisible,
}

impl EdgeDistribution {
    pub fn is_all_visible(&self) -> bool {
        match self {
            EdgeDistribution::AllVisible => true,
            _ => false,
        }
    }
}

/// Define the distribution of properties.
/// MasterMirrorVisible indicates that the property info can be visible on any vertex/edge replica,
/// including master and mirror;
/// MasterVisible indicates that the property info can only be visible on the master vertex/edge.
#[derive(Clone)]
pub enum PropertyDistribution {
    MasterMirrorVisible,
    MasterVisible,
}

/// ETripLabel is uniquely identified for edge with (srcLabelId, edgeLabelId, dstLabelId)
/// NOTICE that direction is prserved in ETripLabel.
pub type ETripLabel = (LabelId, LabelId, LabelId);

#[derive(Clone, Eq, PartialEq)]
pub enum EntityLabel {
    VLabel(LabelId),
    ELabel(ETripLabel),
}

impl From<LabelId> for EntityLabel {
    fn from(label: LabelId) -> Self {
        EntityLabel::VLabel(label)
    }
}

impl From<ETripLabel> for EntityLabel {
    fn from(label: ETripLabel) -> Self {
        EntityLabel::ELabel(label)
    }
}

/// Query the info Distribution including:
/// 1) Vertex Topology Distribution,
/// 2) Edge Topology Distribution, and
/// 3) Properties Distribution
pub trait QueryDistribution: Send + Sync {
    fn get_vertex_distribution(&self, vlabel: &LabelId) -> IrResult<VertexDistribution>;
    fn get_vertex_property_distribution(&self, vlabel: &LabelId) -> IrResult<PropertyDistribution>;
    fn get_edge_distribution(&self, elabel: &ETripLabel) -> IrResult<EdgeDistribution>;
    fn get_edge_property_distribution(&self, elabel: &ETripLabel) -> IrResult<PropertyDistribution>;
}

/// the topology partition strategy over graph
pub enum GraphTopoPartitionStrategy {
    AllReplicaStrategy,
    // ECutStrategy, which is to partition vertices into partitions, and edges may follow src, dst, or both.
    ECutStrategy(EdgeDistribution),
    // VCutStrategy
    VCutStrategy,
    // HybridStrategy, i.e., the topology partition strategy are defined per label.
    HybridStrategy(HybridTopoDistribution),
}

/// the property partition strategy over graph
pub enum GraphPropPartitionStrategy {
    // EntityPropPartition, i.e., (VertexPropertyDistribution, EdgePropertyDistribution)
    EntityPropStrategy((PropertyDistribution, PropertyDistribution)),
    // HybridPropStrategy, i.e., the property partition strategy are defined per label.
    HybridPropStrategy(HybridPropDistribution),
}

/// Hybrid Topology Distribution, where the distribution for topology and defined per label.
pub struct HybridTopoDistribution {
    vertex_topology_distribution: HashMap<LabelId, VertexDistribution>,
    edge_topology_distribution: HashMap<ETripLabel, EdgeDistribution>,
}

/// Hybrid Property Distribution, where the distribution for property and defined per label.
pub struct HybridPropDistribution {
    vertex_property_distribution: HashMap<LabelId, PropertyDistribution>,
    edge_property_distribution: HashMap<ETripLabel, PropertyDistribution>,
}

pub struct GraphPartitionStrategy {
    topo_strategy: GraphTopoPartitionStrategy,
    prop_strategy: GraphPropPartitionStrategy,
}

impl QueryDistribution for GraphPartitionStrategy {
    fn get_vertex_distribution(&self, vlabel: &LabelId) -> IrResult<VertexDistribution> {
        match &self.topo_strategy {
            GraphTopoPartitionStrategy::AllReplicaStrategy => Ok(VertexDistribution::AllVisible),
            GraphTopoPartitionStrategy::ECutStrategy(_) => Ok(VertexDistribution::OneVisible),
            GraphTopoPartitionStrategy::VCutStrategy => Ok(VertexDistribution::AdjVisible),
            GraphTopoPartitionStrategy::HybridStrategy(hybrid) => hybrid
                .vertex_topology_distribution
                .get(vlabel)
                .cloned()
                .ok_or(IrError::TableNotExist(vlabel.clone().into())),
        }
    }

    fn get_vertex_property_distribution(&self, vlabel: &LabelId) -> IrResult<PropertyDistribution> {
        match &self.prop_strategy {
            GraphPropPartitionStrategy::EntityPropStrategy((vprop_distribution, _)) => {
                Ok(vprop_distribution.clone())
            }
            GraphPropPartitionStrategy::HybridPropStrategy(hybrid) => hybrid
                .vertex_property_distribution
                .get(vlabel)
                .cloned()
                .ok_or(IrError::TableNotExist(vlabel.clone().into())),
        }
    }

    fn get_edge_distribution(&self, elabel: &ETripLabel) -> IrResult<EdgeDistribution> {
        match &self.topo_strategy {
            GraphTopoPartitionStrategy::AllReplicaStrategy => Ok(EdgeDistribution::AllVisible),
            GraphTopoPartitionStrategy::ECutStrategy(e_topo_distribution) => {
                Ok(e_topo_distribution.clone())
            }
            GraphTopoPartitionStrategy::VCutStrategy => Ok(EdgeDistribution::OneVisible),
            GraphTopoPartitionStrategy::HybridStrategy(hybrid) => hybrid
                .edge_topology_distribution
                .get(elabel)
                .cloned()
                .ok_or(IrError::TableNotExist(elabel.1.clone().into())),
        }
    }

    fn get_edge_property_distribution(&self, elabel: &ETripLabel) -> IrResult<PropertyDistribution> {
        match &self.prop_strategy {
            GraphPropPartitionStrategy::EntityPropStrategy((_, eprop_distribution)) => {
                Ok(eprop_distribution.clone())
            }
            GraphPropPartitionStrategy::HybridPropStrategy(hybrid) => hybrid
                .edge_property_distribution
                .get(elabel)
                .cloned()
                .ok_or(IrError::TableNotExist(elabel.1.clone().into())),
        }
    }
}
