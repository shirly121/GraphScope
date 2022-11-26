package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Integer;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::arrow_projected_fragment_impl::TypedArray<int32_t>",
    factory = PrimitiveTypedArray_cxx_0x4f4be6c2Factory.class
)
@FFISynthetic("com.alibaba.graphscope.ds.PrimitiveTypedArray")
public class PrimitiveTypedArray_cxx_0x4f4be6c2 extends BaseTypedArray_cxx_0x4f4be6c2 implements PrimitiveTypedArray<Integer> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(PrimitiveTypedArray_cxx_0x4f4be6c2.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public PrimitiveTypedArray_cxx_0x4f4be6c2(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PrimitiveTypedArray_cxx_0x4f4be6c2 that = (PrimitiveTypedArray_cxx_0x4f4be6c2) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @CXXOperator("[]")
  public Integer get(long arg0) {
    return new java.lang.Integer(nativeGet(address, arg0));
  }

  @CXXOperator("[]")
  public static native int nativeGet(long ptr, long arg00);

  @FFINameAlias("GetLength")
  public long getLength() {
    return nativeGetLength(address);
  }

  @FFINameAlias("GetLength")
  public static native long nativeGetLength(long ptr);

  public void setAddress(long arg0) {
    this.address = arg0;
  }

  public static native long nativeCreateFactory0();
}
