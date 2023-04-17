use crate::error::IrResult;
use ir_common::LabelId;

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
    fn get_vertex_property_visibility(&self, label: &LabelId) -> IrResult<PropertyVisibility>;
    fn get_edge_visibility(&self, elabel: &ETripLabel) -> IrResult<EdgeVisibility>;
    fn get_edge_property_visibility(&self, label: &ETripLabel) -> IrResult<PropertyVisibility>;
}
