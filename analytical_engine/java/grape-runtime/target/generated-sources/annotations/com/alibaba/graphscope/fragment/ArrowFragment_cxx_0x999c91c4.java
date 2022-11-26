package com.alibaba.graphscope.fragment;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.ds.PropertyAdjList;
import com.alibaba.graphscope.ds.PropertyRawAdjList;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::ArrowFragmentDefault<int64_t>")
@FFISynthetic("com.alibaba.graphscope.fragment.ArrowFragment")
public class ArrowFragment_cxx_0x999c91c4 extends FFIPointerImpl implements ArrowFragment<Long> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(ArrowFragment_cxx_0x999c91c4.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public ArrowFragment_cxx_0x999c91c4(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArrowFragment_cxx_0x999c91c4 that = (ArrowFragment_cxx_0x999c91c4) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("directed")
  public boolean directed() {
    return nativeDirected(address);
  }

  @FFINameAlias("directed")
  public static native boolean nativeDirected(long ptr);

  @FFINameAlias("edge_label_num")
  public int edgeLabelNum() {
    return nativeEdgeLabelNum(address);
  }

  @FFINameAlias("edge_label_num")
  public static native int nativeEdgeLabelNum(long ptr);

  @FFINameAlias("edge_property_num")
  public int edgePropertyNum(int arg0) {
    return nativeEdgePropertyNum(address, arg0);
  }

  @FFINameAlias("edge_property_num")
  public static native int nativeEdgePropertyNum(long ptr, int arg00);

  public int fid() {
    return nativeFid(address);
  }

  public static native int nativeFid(long ptr);

  public int fnum() {
    return nativeFnum(address);
  }

  public static native int nativeFnum(long ptr);

  @FFINameAlias("GetData<double>")
  public long getDoubleData(
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0, int arg1) {
    return nativeGetDoubleData(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("GetData<double>")
  public static native long nativeGetDoubleData(long ptr, long arg00, int arg11);

  @FFINameAlias("GetFragId")
  public int getFragId(
      @FFIConst @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0) {
    return nativeGetFragId(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("GetFragId")
  public static native int nativeGetFragId(long ptr, long arg00);

  @FFINameAlias("GetIncomingAdjList")
  @CXXValue
  @FFITypeAlias("gs::AdjListDefault<uint64_t>")
  public PropertyAdjList<Long> getIncomingAdjList(
      @FFIConst @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0,
      int arg1) {
    long ret$ = nativeGetIncomingAdjList(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.PropertyAdjList_cxx_0x8ba5a67d.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1); return (new com.alibaba.graphscope.ds.PropertyAdjList_cxx_0x8ba5a67d(ret$));
  }

  @FFINameAlias("GetIncomingAdjList")
  @CXXValue
  @FFITypeAlias("gs::AdjListDefault<uint64_t>")
  public static native long nativeGetIncomingAdjList(long ptr, long rv_base, long arg00, int arg11);

  @FFINameAlias("GetIncomingRawAdjList")
  @CXXValue
  @FFITypeAlias("gs::RawAdjListDefault<uint64_t>")
  public PropertyRawAdjList<Long> getIncomingRawAdjList(
      @FFIConst @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0,
      int arg1) {
    long ret$ = nativeGetIncomingRawAdjList(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.PropertyRawAdjList_cxx_0xae65a4ad.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1); return (new com.alibaba.graphscope.ds.PropertyRawAdjList_cxx_0xae65a4ad(ret$));
  }

  @FFINameAlias("GetIncomingRawAdjList")
  @CXXValue
  @FFITypeAlias("gs::RawAdjListDefault<uint64_t>")
  public static native long nativeGetIncomingRawAdjList(long ptr, long rv_base, long arg00,
      int arg11);

  @FFINameAlias("GetInnerVertex")
  public boolean getInnerVertex(int arg0, Long arg1,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg2) {
    return nativeGetInnerVertex(address, arg0, arg1, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address);
  }

  @FFINameAlias("GetInnerVertex")
  public static native boolean nativeGetInnerVertex(long ptr, int arg00, long arg11, long arg22);

  @FFINameAlias("GetInnerVertexGid")
  public Long getInnerVertexGid(
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0) {
    return new java.lang.Long(nativeGetInnerVertexGid(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("GetInnerVertexGid")
  public static native long nativeGetInnerVertexGid(long ptr, long arg00);

  @FFINameAlias("GetInnerVertexId")
  public Long getInnerVertexOid(
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg0) {
    return new java.lang.Long(nativeGetInnerVertexOid(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("GetInnerVertexId")
  public static native long nativeGetInnerVertexOid(long ptr, long arg00);

  @FFINameAlias("GetInnerVerticesNum")
  public long getInnerVerticesNum(int arg0) {
    return nativeGetInnerVerticesNum(address, arg0);
  }

  @FFINameAlias("GetInnerVerticesNum")
  public static native long nativeGetInnerVerticesNum(long ptr, int arg00);

  @FFINameAlias("GetData<uint32_t>")
  public long getIntData(@FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0,
      int arg1) {
    return nativeGetIntData(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("GetData<uint32_t>")
  public static native long nativeGetIntData(long ptr, long arg00, int arg11);

  @FFINameAlias("GetLocalInDegree")
  public int getLocalInDegree(
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0, int arg1) {
    return nativeGetLocalInDegree(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("GetLocalInDegree")
  public static native int nativeGetLocalInDegree(long ptr, long arg00, int arg11);

  @FFINameAlias("GetLocalOutDegree")
  public int getLocalOutDegree(
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0, int arg1) {
    return nativeGetLocalOutDegree(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("GetLocalOutDegree")
  public static native int nativeGetLocalOutDegree(long ptr, long arg00, int arg11);

  @FFINameAlias("GetData<uint64_t>")
  public long getLongData(@FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0,
      int arg1) {
    return nativeGetLongData(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("GetData<uint64_t>")
  public static native long nativeGetLongData(long ptr, long arg00, int arg11);

  @FFINameAlias("GetId")
  @CXXValue
  public Long getOid(@FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0) {
    return new java.lang.Long(nativeGetOid(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("GetId")
  @CXXValue
  public static native long nativeGetOid(long ptr, long arg00);

  @FFINameAlias("GetOuterVertex")
  public boolean getOuterVertex(int arg0, Long arg1,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg2) {
    return nativeGetOuterVertex(address, arg0, arg1, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address);
  }

  @FFINameAlias("GetOuterVertex")
  public static native boolean nativeGetOuterVertex(long ptr, int arg00, long arg11, long arg22);

  @FFINameAlias("GetOuterVertexGid")
  public Long getOuterVertexGid(
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0) {
    return new java.lang.Long(nativeGetOuterVertexGid(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("GetOuterVertexGid")
  public static native long nativeGetOuterVertexGid(long ptr, long arg00);

  @FFINameAlias("GetOuterVertexId")
  public Long getOuterVertexOid(
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg0) {
    return new java.lang.Long(nativeGetOuterVertexOid(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("GetOuterVertexId")
  public static native long nativeGetOuterVertexOid(long ptr, long arg00);

  @FFINameAlias("GetOuterVerticesNum")
  public long getOuterVerticesNum(int arg0) {
    return nativeGetOuterVerticesNum(address, arg0);
  }

  @FFINameAlias("GetOuterVerticesNum")
  public static native long nativeGetOuterVerticesNum(long ptr, int arg00);

  @FFINameAlias("GetOutgoingAdjList")
  @CXXValue
  @FFITypeAlias("gs::AdjListDefault<uint64_t>")
  public PropertyAdjList<Long> getOutgoingAdjList(
      @FFIConst @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0,
      int arg1) {
    long ret$ = nativeGetOutgoingAdjList(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.PropertyAdjList_cxx_0x8ba5a67d.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1); return (new com.alibaba.graphscope.ds.PropertyAdjList_cxx_0x8ba5a67d(ret$));
  }

  @FFINameAlias("GetOutgoingAdjList")
  @CXXValue
  @FFITypeAlias("gs::AdjListDefault<uint64_t>")
  public static native long nativeGetOutgoingAdjList(long ptr, long rv_base, long arg00, int arg11);

  @FFINameAlias("GetOutgoingRawAdjList")
  @CXXValue
  @FFITypeAlias("gs::RawAdjListDefault<uint64_t>")
  public PropertyRawAdjList<Long> getOutgoingRawAdjList(
      @FFIConst @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0,
      int arg1) {
    long ret$ = nativeGetOutgoingRawAdjList(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.PropertyRawAdjList_cxx_0xae65a4ad.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1); return (new com.alibaba.graphscope.ds.PropertyRawAdjList_cxx_0xae65a4ad(ret$));
  }

  @FFINameAlias("GetOutgoingRawAdjList")
  @CXXValue
  @FFITypeAlias("gs::RawAdjListDefault<uint64_t>")
  public static native long nativeGetOutgoingRawAdjList(long ptr, long rv_base, long arg00,
      int arg11);

  @FFINameAlias("GetTotalNodesNum")
  public int getTotalNodesNum() {
    return nativeGetTotalNodesNum(address);
  }

  @FFINameAlias("GetTotalNodesNum")
  public static native int nativeGetTotalNodesNum(long ptr);

  @FFINameAlias("GetTotalVerticesNum")
  public int getTotalVerticesNum() {
    return nativeGetTotalVerticesNum0(address);
  }

  @FFINameAlias("GetTotalVerticesNum")
  public static native int nativeGetTotalVerticesNum0(long ptr);

  @FFINameAlias("GetTotalVerticesNum")
  public int getTotalVerticesNum(int arg0) {
    return nativeGetTotalVerticesNum1(address, arg0);
  }

  @FFINameAlias("GetTotalVerticesNum")
  public static native int nativeGetTotalVerticesNum1(long ptr, int arg00);

  @FFINameAlias("GetVertex")
  public boolean getVertex(int arg0, Long arg1,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg2) {
    return nativeGetVertex(address, arg0, arg1, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address);
  }

  @FFINameAlias("GetVertex")
  public static native boolean nativeGetVertex(long ptr, int arg00, long arg11, long arg22);

  @FFINameAlias("GetVerticesNum")
  public long getVerticesNum(int arg0) {
    return nativeGetVerticesNum(address, arg0);
  }

  @FFINameAlias("GetVerticesNum")
  public static native long nativeGetVerticesNum(long ptr, int arg00);

  @FFINameAlias("Gid2Oid")
  public Long gid2Oid(Long arg0) {
    return new java.lang.Long(nativeGid2Oid(address, arg0));
  }

  @FFINameAlias("Gid2Oid")
  public static native long nativeGid2Oid(long ptr, long arg00);

  @FFINameAlias("Gid2Vertex")
  public boolean gid2Vertex(Long arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1) {
    return nativeGid2Vertex(address, arg0, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address);
  }

  @FFINameAlias("Gid2Vertex")
  public static native boolean nativeGid2Vertex(long ptr, long arg00, long arg11);

  @FFINameAlias("HasChild")
  public boolean hasChild(@FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0,
      int arg1) {
    return nativeHasChild(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("HasChild")
  public static native boolean nativeHasChild(long ptr, long arg00, int arg11);

  @FFINameAlias("HasParent")
  public boolean hasParent(@FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0,
      int arg1) {
    return nativeHasParent(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("HasParent")
  public static native boolean nativeHasParent(long ptr, long arg00, int arg11);

  @FFINameAlias("InnerVertexGid2Vertex")
  public boolean innerVertexGid2Vertex(Long arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1) {
    return nativeInnerVertexGid2Vertex(address, arg0, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address);
  }

  @FFINameAlias("InnerVertexGid2Vertex")
  public static native boolean nativeInnerVertexGid2Vertex(long ptr, long arg00, long arg11);

  @FFINameAlias("InnerVertices")
  @CXXValue
  @FFITypeAlias("grape::VertexRange<uint64_t>")
  public VertexRange<Long> innerVertices(int arg0) {
    long ret$ = nativeInnerVertices(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543.SIZE), arg0); return (new com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543(ret$));
  }

  @FFINameAlias("InnerVertices")
  @CXXValue
  @FFITypeAlias("grape::VertexRange<uint64_t>")
  public static native long nativeInnerVertices(long ptr, long rv_base, int arg00);

  @FFINameAlias("IsInnerVertex")
  public boolean isInnerVertex(
      @FFIConst @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0) {
    return nativeIsInnerVertex(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("IsInnerVertex")
  public static native boolean nativeIsInnerVertex(long ptr, long arg00);

  @FFINameAlias("IsOuterVertex")
  public boolean isOuterVertex(
      @FFIConst @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0) {
    return nativeIsOuterVertex(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("IsOuterVertex")
  public static native boolean nativeIsOuterVertex(long ptr, long arg00);

  @FFINameAlias("Oid2Gid")
  public boolean oid2Gid(int arg0, Long arg1,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg2) {
    return nativeOid2Gid(address, arg0, arg1, ((com.alibaba.fastffi.FFIPointerImpl) arg2).address);
  }

  @FFINameAlias("Oid2Gid")
  public static native boolean nativeOid2Gid(long ptr, int arg00, long arg11, long arg22);

  @FFINameAlias("OuterVertexGid2Vertex")
  public boolean outerVertexGid2Vertex(Long arg0,
      @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg1) {
    return nativeOuterVertexGid2Vertex(address, arg0, ((com.alibaba.fastffi.FFIPointerImpl) arg1).address);
  }

  @FFINameAlias("OuterVertexGid2Vertex")
  public static native boolean nativeOuterVertexGid2Vertex(long ptr, long arg00, long arg11);

  @FFINameAlias("OuterVertices")
  @CXXValue
  @FFITypeAlias("grape::VertexRange<uint64_t>")
  public VertexRange<Long> outerVertices(int arg0) {
    long ret$ = nativeOuterVertices(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543.SIZE), arg0); return (new com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543(ret$));
  }

  @FFINameAlias("OuterVertices")
  @CXXValue
  @FFITypeAlias("grape::VertexRange<uint64_t>")
  public static native long nativeOuterVertices(long ptr, long rv_base, int arg00);

  @FFINameAlias("Vertex2Gid")
  public Long vertex2Gid(@CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg0) {
    return new java.lang.Long(nativeVertex2Gid(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("Vertex2Gid")
  public static native long nativeVertex2Gid(long ptr, long arg00);

  @FFINameAlias("vertex_label")
  public int vertexLabel(
      @FFIConst @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0) {
    return nativeVertexLabel(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("vertex_label")
  public static native int nativeVertexLabel(long ptr, long arg00);

  @FFINameAlias("vertex_label_num")
  public int vertexLabelNum() {
    return nativeVertexLabelNum(address);
  }

  @FFINameAlias("vertex_label_num")
  public static native int nativeVertexLabelNum(long ptr);

  @FFINameAlias("vertex_offset")
  public long vertexOffset(
      @FFIConst @FFITypeAlias("grape::Vertex<uint64_t>") @CXXReference Vertex<Long> arg0) {
    return nativeVertexOffset(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("vertex_offset")
  public static native long nativeVertexOffset(long ptr, long arg00);

  @FFINameAlias("vertex_property_num")
  public int vertexPropertyNum(int arg0) {
    return nativeVertexPropertyNum(address, arg0);
  }

  @FFINameAlias("vertex_property_num")
  public static native int nativeVertexPropertyNum(long ptr, int arg00);

  @FFINameAlias("Vertices")
  @CXXValue
  @FFITypeAlias("grape::VertexRange<uint64_t>")
  public VertexRange<Long> vertices(int arg0) {
    long ret$ = nativeVertices(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543.SIZE), arg0); return (new com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543(ret$));
  }

  @FFINameAlias("Vertices")
  @CXXValue
  @FFITypeAlias("grape::VertexRange<uint64_t>")
  public static native long nativeVertices(long ptr, long rv_base, int arg00);
}
