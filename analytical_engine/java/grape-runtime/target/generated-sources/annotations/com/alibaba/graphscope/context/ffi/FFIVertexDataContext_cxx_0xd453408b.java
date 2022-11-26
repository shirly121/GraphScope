package com.alibaba.graphscope.context.ffi;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "grape::VertexDataContext<gs::ArrowProjectedFragment<int64_t,uint64_t,double,double>,int32_t>",
    factory = FFIVertexDataContext_cxx_0xd453408bFactory.class
)
@FFISynthetic("com.alibaba.graphscope.context.ffi.FFIVertexDataContext")
public class FFIVertexDataContext_cxx_0xd453408b extends FFIPointerImpl implements FFIVertexDataContext<ArrowProjectedFragment<Long, Long, Double, Double>, Integer> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(FFIVertexDataContext_cxx_0xd453408b.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public FFIVertexDataContext_cxx_0xd453408b(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FFIVertexDataContext_cxx_0xd453408b that = (FFIVertexDataContext_cxx_0xd453408b) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @CXXReference
  public GSVertexArray<Integer> data() {
    long ret$ = nativeData(address); return (new com.alibaba.graphscope.ds.GSVertexArray_cxx_0x2ceb3d1f(ret$));
  }

  @CXXReference
  public static native long nativeData(long ptr);

  public static native long nativeCreateFactory0(long arg00, boolean arg11);
}
