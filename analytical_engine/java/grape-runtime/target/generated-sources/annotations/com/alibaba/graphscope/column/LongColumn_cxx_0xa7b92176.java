package com.alibaba.graphscope.column;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::LongColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>>")
@FFISynthetic("com.alibaba.graphscope.column.LongColumn")
public class LongColumn_cxx_0xa7b92176 extends FFIPointerImpl implements LongColumn<ArrowProjectedFragment<Long, Long, Long, Long>> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(LongColumn_cxx_0xa7b92176.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public LongColumn_cxx_0xa7b92176(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LongColumn_cxx_0xa7b92176 that = (LongColumn_cxx_0xa7b92176) o;
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
  @FFITypeAlias("gs::VertexArrayDefault<int64_t>")
  public GSVertexArray<Long> data() {
    long ret$ = nativeData(address); return (new com.alibaba.graphscope.ds.GSVertexArray_cxx_0x2d166c60(ret$));
  }

  @CXXReference
  @FFITypeAlias("gs::VertexArrayDefault<int64_t>")
  public static native long nativeData(long ptr);

  public void set(@CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg0,
      long arg1) {
    nativeSet(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  public static native void nativeSet(long ptr, long arg00, long arg11);
}
