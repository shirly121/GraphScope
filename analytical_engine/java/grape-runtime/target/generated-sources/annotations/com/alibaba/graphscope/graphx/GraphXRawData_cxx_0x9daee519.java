package com.alibaba.graphscope.graphx;

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

@FFIForeignType("gs::GraphXRawData<int64_t,uint64_t,int32_t,int32_t>")
@FFISynthetic("com.alibaba.graphscope.graphx.GraphXRawData")
public class GraphXRawData_cxx_0x9daee519 extends FFIPointerImpl implements GraphXRawData<Long, Long, Integer, Integer> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(GraphXRawData_cxx_0x9daee519.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public GraphXRawData_cxx_0x9daee519(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GraphXRawData_cxx_0x9daee519 that = (GraphXRawData_cxx_0x9daee519) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("GetEdgeNum")
  public long getEdgeNum() {
    return nativeGetEdgeNum(address);
  }

  @FFINameAlias("GetEdgeNum")
  public static native long nativeGetEdgeNum(long ptr);

  @FFINameAlias("GetVertexNum")
  public long getVertexNum() {
    return nativeGetVertexNum(address);
  }

  @FFINameAlias("GetVertexNum")
  public static native long nativeGetVertexNum(long ptr);

  public long id() {
    return nativeId(address);
  }

  public static native long nativeId(long ptr);
}
