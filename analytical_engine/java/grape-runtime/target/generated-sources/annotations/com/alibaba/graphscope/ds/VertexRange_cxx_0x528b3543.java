package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXValue;
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
    value = "grape::VertexRange<uint64_t>",
    factory = VertexRange_cxx_0x528b3543Factory.class
)
@FFISynthetic("com.alibaba.graphscope.ds.VertexRange")
public class VertexRange_cxx_0x528b3543 extends FFIPointerImpl implements VertexRange<Long> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(VertexRange_cxx_0x528b3543.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public VertexRange_cxx_0x528b3543(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VertexRange_cxx_0x528b3543 that = (VertexRange_cxx_0x528b3543) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public void SetRange(@CXXValue Long arg0, @CXXValue Long arg1) {
    native_SetRange(address, arg0, arg1);
  }

  public static native void native_SetRange(long ptr, long arg00, long arg11);

  @FFINameAlias("begin_value")
  @CXXValue
  public Long beginValue() {
    return new java.lang.Long(nativeBeginValue(address));
  }

  @FFINameAlias("begin_value")
  @CXXValue
  public static native long nativeBeginValue(long ptr);

  @CXXOperator("delete")
  public void delete() {
    nativeDelete(address);
  }

  @CXXOperator("delete")
  public static native void nativeDelete(long ptr);

  @FFINameAlias("end_value")
  @CXXValue
  public Long endValue() {
    return new java.lang.Long(nativeEndValue(address));
  }

  @FFINameAlias("end_value")
  @CXXValue
  public static native long nativeEndValue(long ptr);

  public long size() {
    return nativeSize(address);
  }

  public static native long nativeSize(long ptr);

  public static native long nativeCreateFactory0();

  public static native long nativeCreateFactory1(long arg00, long arg11);
}
