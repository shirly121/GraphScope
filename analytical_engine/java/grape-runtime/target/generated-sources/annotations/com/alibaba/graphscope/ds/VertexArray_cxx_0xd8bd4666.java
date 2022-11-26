package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::JavaVertexArray<uint64_t,int64_t>",
    factory = VertexArray_cxx_0xd8bd4666Factory.class
)
@FFISynthetic("com.alibaba.graphscope.ds.VertexArray")
public class VertexArray_cxx_0xd8bd4666 extends FFIPointerImpl implements VertexArray<Long, Long> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(VertexArray_cxx_0xd8bd4666.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public VertexArray_cxx_0xd8bd4666(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VertexArray_cxx_0xd8bd4666 that = (VertexArray_cxx_0xd8bd4666) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @CXXReference
  public VertexRange<Long> GetVertexRange() {
    long ret$ = native_GetVertexRange(address); return (new com.alibaba.graphscope.ds.VertexRange_cxx_0x528b3543(ret$));
  }

  @CXXReference
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
  public Long get(@CXXReference Vertex<Long> arg0) {
    return new java.lang.Long(nativeGet(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address));
  }

  @FFINameAlias("GetValue")
  @CXXOperator("[]")
  @CXXReference
  public static native long nativeGet(long ptr, long arg00);

  @FFINameAlias("Init")
  public void init(@CXXReference VertexRange<Long> arg0) {
    nativeInit0(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @FFINameAlias("Init")
  public static native void nativeInit0(long ptr, long arg00);

  @FFINameAlias("Init")
  public void init(@CXXReference VertexRange<Long> arg0, @CXXReference Long arg1) {
    nativeInit1(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("Init")
  public static native void nativeInit1(long ptr, long arg00, long arg11);

  @FFINameAlias("SetValue")
  public void setValue(@CXXReference Vertex<Long> arg0, @CXXReference Long arg1) {
    nativeSetValue0(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("SetValue")
  public static native void nativeSetValue0(long ptr, long arg00, long arg11);

  @FFINameAlias("SetValue")
  public void setValue(@CXXReference VertexRange<Long> arg0, @CXXReference Long arg1) {
    nativeSetValue1(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address, arg1);
  }

  @FFINameAlias("SetValue")
  public static native void nativeSetValue1(long ptr, long arg00, long arg11);

  @FFINameAlias("SetValue")
  public void setValue(@CXXReference Long arg0) {
    nativeSetValue2(address, arg0);
  }

  @FFINameAlias("SetValue")
  public static native void nativeSetValue2(long ptr, long arg00);

  public static native long nativeCreateFactory0();
}
