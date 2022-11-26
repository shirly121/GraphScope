package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Double;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::VertexArrayDefault<double>",
    factory = GSVertexArray_cxx_0x13932289Factory.class
)
@FFISynthetic("com.alibaba.graphscope.ds.GSVertexArray")
public class GSVertexArray_cxx_0x13932289 extends FFIPointerImpl implements GSVertexArray<Double> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(GSVertexArray_cxx_0x13932289.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public GSVertexArray_cxx_0x13932289(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GSVertexArray_cxx_0x13932289 that = (GSVertexArray_cxx_0x13932289) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @CXXReference
  @FFITypeAlias("grape::VertexRange<uint64_t>")
  public VertexRange<Long> GetVertexRange() {
    long ret$ = native_GetVertexRange(address); return (new com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543(ret$));
  }

  @CXXReference
  @FFITypeAlias("grape::VertexRange<uint64_t>")
  public static native long native_GetVertexRange(long ptr);

  @CXXOperator("delete")
  public void delete() {
    nativeDelete(address);
  }

  @CXXOperator("delete")
  public static native void nativeDelete(long ptr);

  @FFINameAlias("GetValue")
  @CXXOperator("[]")
  @CXXReference
  public Double get(@CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg0) {
    return nativeGet(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("GetValue")
  @CXXOperator("[]")
  @CXXReference
  public static native double nativeGet(long ptr, long arg00);

  @FFINameAlias("Init")
  public void init(
      @CXXReference @FFITypeAlias("grape::VertexRange<uint64_t>") VertexRange<Long> arg0) {
    nativeInit0(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("Init")
  public static native void nativeInit0(long ptr, long arg00);

  @FFINameAlias("Init")
  public void init(
      @CXXReference @FFITypeAlias("grape::VertexRange<uint64_t>") VertexRange<Long> arg0,
      @CXXReference Double arg1) {
    nativeInit1(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("Init")
  public static native void nativeInit1(long ptr, long arg00, double arg11);

  @FFINameAlias("SetValue")
  public void setValue(@CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> arg0,
      @CXXReference Double arg1) {
    nativeSetValue0(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("SetValue")
  public static native void nativeSetValue0(long ptr, long arg00, double arg11);

  @FFINameAlias("SetValue")
  public void setValue(
      @CXXReference @FFITypeAlias("grape::VertexRange<uint64_t>") VertexRange<Long> arg0,
      @CXXReference Double arg1) {
    nativeSetValue1(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("SetValue")
  public static native void nativeSetValue1(long ptr, long arg00, double arg11);

  @FFINameAlias("SetValue")
  public void setValue(@CXXReference Double arg0) {
    nativeSetValue2(address, arg0);
  }

  @FFINameAlias("SetValue")
  public static native void nativeSetValue2(long ptr, double arg00);

  @FFINameAlias("size")
  public long size() {
    return nativeSize(address);
  }

  @FFINameAlias("size")
  public static native long nativeSize(long ptr);

  @FFINameAlias("Swap")
  public void swap(@CXXReference GSVertexArray<Double> arg0) {
    nativeSwap(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("Swap")
  public static native void nativeSwap(long ptr, long arg00);

  public static native long nativeCreateFactory0();
}
