package com.alibaba.graphscope.column;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Double;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::DoubleColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>>")
@FFISynthetic("com.alibaba.graphscope.column.DoubleColumn")
public class DoubleColumn_cxx_0x3264d94b extends IColumn_cxx_0x53d1b253 implements DoubleColumn<ArrowProjectedFragment<Long, Long, Long, Long>> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(DoubleColumn_cxx_0x3264d94b.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public DoubleColumn_cxx_0x3264d94b(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DoubleColumn_cxx_0x3264d94b that = (DoubleColumn_cxx_0x3264d94b) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public double at(@CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg0) {
    return nativeAt(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  public static native double nativeAt(long ptr, long arg00);

  @CXXReference
  @FFITypeAlias("gs::VertexArrayDefault<double>")
  public GSVertexArray<Double> data() {
    long ret$ = nativeData(address); return (new com.alibaba.graphscope.ds.GSVertexArray_cxx_0x13932289(ret$));
  }

  @CXXReference
  @FFITypeAlias("gs::VertexArrayDefault<double>")
  public static native long nativeData(long ptr);

  @CXXValue
  public FFIByteString name() {
    long ret$ = nativeName(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.fastffi.impl.CXXStdString_cxx_0xcec1e274.SIZE)); return (new com.alibaba.fastffi.impl.CXXStdString_cxx_0xcec1e274(ret$));
  }

  @CXXValue
  public static native long nativeName(long ptr, long rv_base);

  public void set(@CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg0,
      double arg1) {
    nativeSet(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  public static native void nativeSet(long ptr, long arg00, double arg11);

  @FFINameAlias("set_name")
  public void setName(@CXXReference FFIByteString arg0) {
    nativeSetName(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("set_name")
  public static native void nativeSetName(long ptr, long arg00);
}
