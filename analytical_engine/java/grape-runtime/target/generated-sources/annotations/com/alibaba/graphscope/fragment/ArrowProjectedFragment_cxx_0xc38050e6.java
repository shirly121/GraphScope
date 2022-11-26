package com.alibaba.graphscope.fragment;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.ds.BaseTypedArray;
import com.alibaba.graphscope.ds.DestList;
import com.alibaba.graphscope.ds.ProjectedAdjList;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.StringView;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::ArrowProjectedFragment<int64_t,uint64_t,std::string,std::string>")
@FFISynthetic("com.alibaba.graphscope.fragment.ArrowProjectedFragment")
public class ArrowProjectedFragment_cxx_0xc38050e6 extends FFIPointerImpl implements ArrowProjectedFragment<Long, Long, StringView, StringView> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(ArrowProjectedFragment_cxx_0xc38050e6.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public ArrowProjectedFragment_cxx_0xc38050e6(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArrowProjectedFragment_cxx_0xc38050e6 that = (ArrowProjectedFragment_cxx_0xc38050e6) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("edge_label")
  public int edgeLabel() {
    return nativeEdgeLabel(address);
  }

  @FFINameAlias("edge_label")
  public static native int nativeEdgeLabel(long ptr);

  @FFINameAlias("edge_prop_id")
  public int edgePropId() {
    return nativeEdgePropId(address);
  }

  @FFINameAlias("edge_prop_id")
  public static native int nativeEdgePropId(long ptr);

  public int fid() {
    return nativeFid(address);
  }

  public static native int nativeFid(long ptr);

  public int fnum() {
    return nativeFnum(address);
  }

  public static native int nativeFnum(long ptr);

  @FFINameAlias("get_arrow_fragment")
  @CXXValue
  public StdSharedPtr<ArrowFragment<Long>> getArrowFragment() {
    long ret$ = nativeGetArrowFragment(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.stdcxx.StdSharedPtr_cxx_0x6df10fdf.SIZE)); return (new com.alibaba.graphscope.stdcxx.StdSharedPtr_cxx_0x6df10fdf(ret$));
  }

  @FFINameAlias("get_arrow_fragment")
  @CXXValue
  public static native long nativeGetArrowFragment(long ptr, long rv_base);

  @FFINameAlias("get_edata_array_accessor")
  @CXXReference
  public BaseTypedArray<StringView> getEdataArrayAccessor() {
    long ret$ = nativeGetEdataArrayAccessor(address); return (new com.alibaba.graphscope.ds.BaseTypedArray_cxx_0x5d3f85f1(ret$));
  }

  @FFINameAlias("get_edata_array_accessor")
  @CXXReference
  public static native long nativeGetEdataArrayAccessor(long ptr);

  @FFINameAlias("GetEdgeNum")
  public long getEdgeNum() {
    return nativeGetEdgeNum(address);
  }

  @FFINameAlias("GetEdgeNum")
  public static native long nativeGetEdgeNum(long ptr);

  @FFINameAlias("GetFragId")
  public int getFragId(@CXXReference Vertex<Long> arg0) {
    return nativeGetFragId(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("GetFragId")
  public static native int nativeGetFragId(long ptr, long arg00);

  @FFINameAlias("get_ie_offsets_begin_ptr")
  public long getIEOffsetsBeginPtr() {
    return nativeGetIEOffsetsBeginPtr(address);
  }

  @FFINameAlias("get_ie_offsets_begin_ptr")
  public static native long nativeGetIEOffsetsBeginPtr(long ptr);

  @FFINameAlias("get_ie_offsets_end_ptr")
  public long getIEOffsetsEndPtr() {
    return nativeGetIEOffsetsEndPtr(address);
  }

  @FFINameAlias("get_ie_offsets_end_ptr")
  public static native long nativeGetIEOffsetsEndPtr(long ptr);

  @FFINameAlias("GetId")
  @CXXValue
  public Long getId(@CXXReference Vertex<Long> arg0) {
    return new java.lang.Long(nativeGetId(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("GetId")
  @CXXValue
  public static native long nativeGetId(long ptr, long arg00);

  @FFINameAlias("GetInEdgeNum")
  public long getInEdgeNum() {
    return nativeGetInEdgeNum(address);
  }

  @FFINameAlias("GetInEdgeNum")
  public static native long nativeGetInEdgeNum(long ptr);

  @FFINameAlias("get_in_edges_ptr")
  public PropertyNbrUnit<Long> getInEdgesPtr() {
    long ret$ = nativeGetInEdgesPtr(address); return (ret$ == 0L ? null : new com.alibaba.graphscope.ds.PropertyNbrUnit_cxx_0xe6668a20(ret$));
  }

  @FFINameAlias("get_in_edges_ptr")
  public static native long nativeGetInEdgesPtr(long ptr);

  @FFINameAlias("GetIncomingAdjList")
  @CXXValue
  public ProjectedAdjList<Long, StringView> getIncomingAdjList(@CXXReference Vertex<Long> arg0) {
    long ret$ = nativeGetIncomingAdjList(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.ProjectedAdjList_cxx_0xa9082e0f.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) arg0).address); return (new com.alibaba.graphscope.ds.ProjectedAdjList_cxx_0xa9082e0f(ret$));
  }

  @FFINameAlias("GetIncomingAdjList")
  @CXXValue
  public static native long nativeGetIncomingAdjList(long ptr, long rv_base, long arg00);

  @FFINameAlias("GetIncomingEdgeNum")
  public long getIncomingEdgeNum() {
    return nativeGetIncomingEdgeNum(address);
  }

  @FFINameAlias("GetIncomingEdgeNum")
  public static native long nativeGetIncomingEdgeNum(long ptr);

  @FFINameAlias("GetInnerVertex")
  public boolean getInnerVertex(@CXXReference Long arg0, @CXXReference Vertex<Long> arg1) {
    return nativeGetInnerVertex(address, arg0, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address);
  }

  @FFINameAlias("GetInnerVertex")
  public static native boolean nativeGetInnerVertex(long ptr, long arg00, long arg11);

  @FFINameAlias("GetInnerVertexGid")
  @CXXValue
  public Long getInnerVertexGid(@CXXReference Vertex<Long> arg0) {
    return new java.lang.Long(nativeGetInnerVertexGid(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("GetInnerVertexGid")
  @CXXValue
  public static native long nativeGetInnerVertexGid(long ptr, long arg00);

  @FFINameAlias("GetInnerVertexId")
  @CXXValue
  public Long getInnerVertexId(@CXXReference Vertex<Long> arg0) {
    return new java.lang.Long(nativeGetInnerVertexId(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("GetInnerVertexId")
  @CXXValue
  public static native long nativeGetInnerVertexId(long ptr, long arg00);

  @FFINameAlias("GetInnerVerticesNum")
  @CXXValue
  public long getInnerVerticesNum() {
    return nativeGetInnerVerticesNum(address);
  }

  @FFINameAlias("GetInnerVerticesNum")
  @CXXValue
  public static native long nativeGetInnerVerticesNum(long ptr);

  @FFINameAlias("GetLocalInDegree")
  public int getLocalInDegree(@CXXReference Vertex<Long> arg0) {
    return nativeGetLocalInDegree(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("GetLocalInDegree")
  public static native int nativeGetLocalInDegree(long ptr, long arg00);

  @FFINameAlias("GetLocalOutDegree")
  public int getLocalOutDegree(@CXXReference Vertex<Long> arg0) {
    return nativeGetLocalOutDegree(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("GetLocalOutDegree")
  public static native int nativeGetLocalOutDegree(long ptr, long arg00);

  @FFINameAlias("get_oe_offsets_begin_ptr")
  public long getOEOffsetsBeginPtr() {
    return nativeGetOEOffsetsBeginPtr(address);
  }

  @FFINameAlias("get_oe_offsets_begin_ptr")
  public static native long nativeGetOEOffsetsBeginPtr(long ptr);

  @FFINameAlias("get_oe_offsets_end_ptr")
  public long getOEOffsetsEndPtr() {
    return nativeGetOEOffsetsEndPtr(address);
  }

  @FFINameAlias("get_oe_offsets_end_ptr")
  public static native long nativeGetOEOffsetsEndPtr(long ptr);

  @FFINameAlias("GetOutEdgeNum")
  public long getOutEdgeNum() {
    return nativeGetOutEdgeNum(address);
  }

  @FFINameAlias("GetOutEdgeNum")
  public static native long nativeGetOutEdgeNum(long ptr);

  @FFINameAlias("get_out_edges_ptr")
  public PropertyNbrUnit<Long> getOutEdgesPtr() {
    long ret$ = nativeGetOutEdgesPtr(address); return (ret$ == 0L ? null : new com.alibaba.graphscope.ds.PropertyNbrUnit_cxx_0xe6668a20(ret$));
  }

  @FFINameAlias("get_out_edges_ptr")
  public static native long nativeGetOutEdgesPtr(long ptr);

  @FFINameAlias("GetOuterVertex")
  public boolean getOuterVertex(@CXXReference Long arg0, @CXXReference Vertex<Long> arg1) {
    return nativeGetOuterVertex(address, arg0, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address);
  }

  @FFINameAlias("GetOuterVertex")
  public static native boolean nativeGetOuterVertex(long ptr, long arg00, long arg11);

  @FFINameAlias("GetOuterVertexGid")
  @CXXValue
  public Long getOuterVertexGid(@CXXReference Vertex<Long> arg0) {
    return new java.lang.Long(nativeGetOuterVertexGid(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("GetOuterVertexGid")
  @CXXValue
  public static native long nativeGetOuterVertexGid(long ptr, long arg00);

  @FFINameAlias("GetOuterVertexId")
  @CXXValue
  public Long getOuterVertexId(@CXXReference Vertex<Long> arg0) {
    return new java.lang.Long(nativeGetOuterVertexId(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("GetOuterVertexId")
  @CXXValue
  public static native long nativeGetOuterVertexId(long ptr, long arg00);

  @FFINameAlias("GetOuterVerticesNum")
  @CXXValue
  public long getOuterVerticesNum() {
    return nativeGetOuterVerticesNum(address);
  }

  @FFINameAlias("GetOuterVerticesNum")
  @CXXValue
  public static native long nativeGetOuterVerticesNum(long ptr);

  @FFINameAlias("GetOutgoingAdjList")
  @CXXValue
  public ProjectedAdjList<Long, StringView> getOutgoingAdjList(@CXXReference Vertex<Long> arg0) {
    long ret$ = nativeGetOutgoingAdjList(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.ProjectedAdjList_cxx_0xa9082e0f.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) arg0).address); return (new com.alibaba.graphscope.ds.ProjectedAdjList_cxx_0xa9082e0f(ret$));
  }

  @FFINameAlias("GetOutgoingAdjList")
  @CXXValue
  public static native long nativeGetOutgoingAdjList(long ptr, long rv_base, long arg00);

  @FFINameAlias("GetOutgoingEdgeNum")
  public long getOutgoingEdgeNum() {
    return nativeGetOutgoingEdgeNum(address);
  }

  @FFINameAlias("GetOutgoingEdgeNum")
  public static native long nativeGetOutgoingEdgeNum(long ptr);

  @FFINameAlias("GetTotalVerticesNum")
  public long getTotalVerticesNum() {
    return nativeGetTotalVerticesNum(address);
  }

  @FFINameAlias("GetTotalVerticesNum")
  public static native long nativeGetTotalVerticesNum(long ptr);

  @FFINameAlias("get_vdata_array_accessor")
  @CXXReference
  public BaseTypedArray<StringView> getVdataArrayAccessor() {
    long ret$ = nativeGetVdataArrayAccessor(address); return (new com.alibaba.graphscope.ds.BaseTypedArray_cxx_0x5d3f85f1(ret$));
  }

  @FFINameAlias("get_vdata_array_accessor")
  @CXXReference
  public static native long nativeGetVdataArrayAccessor(long ptr);

  @FFINameAlias("GetVertex")
  public boolean getVertex(@CXXReference Long arg0, @CXXReference Vertex<Long> arg1) {
    return nativeGetVertex(address, arg0, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address);
  }

  @FFINameAlias("GetVertex")
  public static native boolean nativeGetVertex(long ptr, long arg00, long arg11);

  @FFINameAlias("GetVerticesNum")
  public Long getVerticesNum() {
    return new java.lang.Long(nativeGetVerticesNum(address));
  }

  @FFINameAlias("GetVerticesNum")
  public static native long nativeGetVerticesNum(long ptr);

  @FFINameAlias("Gid2Vertex")
  public boolean gid2Vertex(@CXXReference Long arg0, @CXXReference Vertex<Long> arg1) {
    return nativeGid2Vertex(address, arg0, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address);
  }

  @FFINameAlias("Gid2Vertex")
  public static native boolean nativeGid2Vertex(long ptr, long arg00, long arg11);

  public long id() {
    return nativeId(address);
  }

  public static native long nativeId(long ptr);

  @FFINameAlias("IEDests")
  @CXXValue
  public DestList ieDestList(@CXXReference Vertex<Long> arg0) {
    long ret$ = nativeIeDestList(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.DestList_cxx_0x69320d4b.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) arg0).address); return (new com.alibaba.graphscope.ds.DestList_cxx_0x69320d4b(ret$));
  }

  @FFINameAlias("IEDests")
  @CXXValue
  public static native long nativeIeDestList(long ptr, long rv_base, long arg00);

  @FFINameAlias("InnerVertexGid2Vertex")
  public boolean innerVertexGid2Vertex(@CXXReference Long arg0, @CXXReference Vertex<Long> arg1) {
    return nativeInnerVertexGid2Vertex(address, arg0, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address);
  }

  @FFINameAlias("InnerVertexGid2Vertex")
  public static native boolean nativeInnerVertexGid2Vertex(long ptr, long arg00, long arg11);

  @FFINameAlias("InnerVertices")
  @CXXValue
  public VertexRange<Long> innerVertices() {
    long ret$ = nativeInnerVertices(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543.SIZE)); return (new com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543(ret$));
  }

  @FFINameAlias("InnerVertices")
  @CXXValue
  public static native long nativeInnerVertices(long ptr, long rv_base);

  @FFINameAlias("IsInnerVertex")
  public boolean isInnerVertex(@CXXReference Vertex<Long> arg0) {
    return nativeIsInnerVertex(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("IsInnerVertex")
  public static native boolean nativeIsInnerVertex(long ptr, long arg00);

  @FFINameAlias("IsOuterVertex")
  public boolean isOuterVertex(@CXXReference Vertex<Long> arg0) {
    return nativeIsOuterVertex(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("IsOuterVertex")
  public static native boolean nativeIsOuterVertex(long ptr, long arg00);

  @FFINameAlias("OEDests")
  @CXXValue
  public DestList oeDestList(@CXXReference Vertex<Long> arg0) {
    long ret$ = nativeOeDestList(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.DestList_cxx_0x69320d4b.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) arg0).address); return (new com.alibaba.graphscope.ds.DestList_cxx_0x69320d4b(ret$));
  }

  @FFINameAlias("OEDests")
  @CXXValue
  public static native long nativeOeDestList(long ptr, long rv_base, long arg00);

  @FFINameAlias("OuterVertexGid2Vertex")
  public boolean outerVertexGid2Vertex(@CXXReference Long arg0, @CXXReference Vertex<Long> arg1) {
    return nativeOuterVertexGid2Vertex(address, arg0, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address);
  }

  @FFINameAlias("OuterVertexGid2Vertex")
  public static native boolean nativeOuterVertexGid2Vertex(long ptr, long arg00, long arg11);

  @FFINameAlias("OuterVertices")
  @CXXValue
  public VertexRange<Long> outerVertices() {
    long ret$ = nativeOuterVertices(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543.SIZE)); return (new com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543(ret$));
  }

  @FFINameAlias("OuterVertices")
  @CXXValue
  public static native long nativeOuterVertices(long ptr, long rv_base);

  @FFINameAlias("Vertex2Gid")
  @CXXValue
  public Long vertex2Gid(@CXXReference Vertex<Long> arg0) {
    return new java.lang.Long(nativeVertex2Gid(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("Vertex2Gid")
  @CXXValue
  public static native long nativeVertex2Gid(long ptr, long arg00);

  @FFINameAlias("vertex_label")
  public int vertexLabel() {
    return nativeVertexLabel(address);
  }

  @FFINameAlias("vertex_label")
  public static native int nativeVertexLabel(long ptr);

  @FFINameAlias("vertex_prop_id")
  public int vertexPropId() {
    return nativeVertexPropId(address);
  }

  @FFINameAlias("vertex_prop_id")
  public static native int nativeVertexPropId(long ptr);

  @FFINameAlias("Vertices")
  @CXXValue
  public VertexRange<Long> vertices() {
    long ret$ = nativeVertices(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543.SIZE)); return (new com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543(ret$));
  }

  @FFINameAlias("Vertices")
  @CXXValue
  public static native long nativeVertices(long ptr, long rv_base);
}
