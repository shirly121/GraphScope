package com.alibaba.graphscope.arrow.array;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.arrow.Status;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::ArrowArrayBuilder<uint64_t>",
    factory = PrimitiveArrowArrayBuilder_cxx_0x10daeab4Factory.class
)
@FFISynthetic("com.alibaba.graphscope.arrow.array.PrimitiveArrowArrayBuilder")
public class PrimitiveArrowArrayBuilder_cxx_0x10daeab4 extends BaseArrowArrayBuilder_cxx_0x10daeab4 implements PrimitiveArrowArrayBuilder<Long> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(PrimitiveArrowArrayBuilder_cxx_0x10daeab4.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public PrimitiveArrowArrayBuilder_cxx_0x10daeab4(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PrimitiveArrowArrayBuilder_cxx_0x10daeab4 that = (PrimitiveArrowArrayBuilder_cxx_0x10daeab4) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("GetValue")
  @CXXReference
  public Long getValue(long arg0) {
    return new java.lang.Long(nativeGetValue(address, arg0));
  }

  @FFINameAlias("GetValue")
  @CXXReference
  public static native long nativeGetValue(long ptr, long arg00);

  @FFINameAlias("Reserve")
  @CXXValue
  public Status reserve(long arg0) {
    long ret$ = nativeReserve(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.arrow.Status_cxx_0xba0ae5b.SIZE), arg0); return (new com.alibaba.graphscope.arrow.Status_cxx_0xba0ae5b(ret$));
  }

  @FFINameAlias("Reserve")
  @CXXValue
  public static native long nativeReserve(long ptr, long rv_base, long arg00);

  @CXXOperator("[]")
  public void set(long arg0, @CXXReference Long arg1) {
    nativeSet(address, arg0, arg1);
  }

  @CXXOperator("[]")
  public static native void nativeSet(long ptr, long arg00, long arg11);

  public void setAddress(long arg0) {
    this.address = arg0;
  }

  @FFINameAlias("UnsafeAppend")
  public void unsafeAppend(@CXXReference Long arg0) {
    nativeUnsafeAppend(address, arg0);
  }

  @FFINameAlias("UnsafeAppend")
  public static native void nativeUnsafeAppend(long ptr, long arg00);

  public static native long nativeCreateFactory0();
}
