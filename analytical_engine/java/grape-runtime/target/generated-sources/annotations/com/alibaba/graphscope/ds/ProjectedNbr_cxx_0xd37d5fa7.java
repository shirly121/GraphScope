package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Integer;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::arrow_projected_fragment_impl::NbrDefault<uint64_t,int32_t>")
@FFISynthetic("com.alibaba.graphscope.ds.ProjectedNbr")
public class ProjectedNbr_cxx_0xd37d5fa7 extends FFIPointerImpl implements ProjectedNbr<Long, Integer> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(ProjectedNbr_cxx_0xd37d5fa7.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public ProjectedNbr_cxx_0xd37d5fa7(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ProjectedNbr_cxx_0xd37d5fa7 that = (ProjectedNbr_cxx_0xd37d5fa7) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @CXXOperator("--")
  @CXXReference
  public ProjectedNbr<Long, Integer> dec() {
    long ret$ = nativeDec(address); return (new com.alibaba.graphscope.ds.ProjectedNbr_cxx_0xd37d5fa7(ret$));
  }

  @CXXOperator("--")
  @CXXReference
  public static native long nativeDec(long ptr);

  @FFINameAlias("edge_id")
  public long edgeId() {
    return nativeEdgeId(address);
  }

  @FFINameAlias("edge_id")
  public static native long nativeEdgeId(long ptr);

  @CXXOperator("==")
  public boolean eq(@CXXReference ProjectedNbr<Long, Integer> arg0) {
    return nativeEq(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @CXXOperator("==")
  public static native boolean nativeEq(long ptr, long arg00);

  @CXXOperator("++")
  @CXXReference
  public ProjectedNbr<Long, Integer> inc() {
    long ret$ = nativeInc(address); return (new com.alibaba.graphscope.ds.ProjectedNbr_cxx_0xd37d5fa7(ret$));
  }

  @CXXOperator("++")
  @CXXReference
  public static native long nativeInc(long ptr);

  @CXXValue
  public Vertex<Long> neighbor() {
    long ret$ = nativeNeighbor(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.Vertex_cxx_0xaccf3424.SIZE)); return (new com.alibaba.graphscope.ds.Vertex_cxx_0xaccf3424(ret$));
  }

  @CXXValue
  public static native long nativeNeighbor(long ptr, long rv_base);
}
