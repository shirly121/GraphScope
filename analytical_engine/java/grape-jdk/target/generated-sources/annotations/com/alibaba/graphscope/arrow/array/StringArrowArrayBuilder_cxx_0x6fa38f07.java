package com.alibaba.graphscope.arrow.array;

import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.arrow.Status;
import com.alibaba.graphscope.stdcxx.CCharPointer;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::ArrowArrayBuilder<std::string>",
    factory = StringArrowArrayBuilder_cxx_0x6fa38f07Factory.class
)
@FFISynthetic("com.alibaba.graphscope.arrow.array.StringArrowArrayBuilder")
public class StringArrowArrayBuilder_cxx_0x6fa38f07 extends BaseArrowArrayBuilder_cxx_0x95c87d79 implements StringArrowArrayBuilder {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(StringArrowArrayBuilder_cxx_0x6fa38f07.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public StringArrowArrayBuilder_cxx_0x6fa38f07(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StringArrowArrayBuilder_cxx_0x6fa38f07 that = (StringArrowArrayBuilder_cxx_0x6fa38f07) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("Reserve")
  @CXXValue
  public Status reserve(long additionalCapacity) {
    long ret$ = nativeReserve(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.arrow.Status_cxx_0xba0ae5b.SIZE), additionalCapacity); return (new com.alibaba.graphscope.arrow.Status_cxx_0xba0ae5b(ret$));
  }

  @FFINameAlias("Reserve")
  @CXXValue
  public static native long nativeReserve(long ptr, long rv_base, long additionalCapacity0);

  @FFINameAlias("ReserveData")
  @CXXValue
  public Status reserveData(long additionalBytes) {
    long ret$ = nativeReserveData(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.arrow.Status_cxx_0xba0ae5b.SIZE), additionalBytes); return (new com.alibaba.graphscope.arrow.Status_cxx_0xba0ae5b(ret$));
  }

  @FFINameAlias("ReserveData")
  @CXXValue
  public static native long nativeReserveData(long ptr, long rv_base, long additionalBytes0);

  public void setAddress(long arg0) {
    this.address = arg0;
  }

  @FFINameAlias("UnsafeAppend")
  public void unsafeAppend(@FFIConst CCharPointer ptr, int length) {
    nativeUnsafeAppend(address, ((com.alibaba.fastffi.FFIPointerImpl) ptr).address, length);
  }

  @FFINameAlias("UnsafeAppend")
  public static native void nativeUnsafeAppend(long ptr, long ptr0, int length1);

  public static native long nativeCreateFactory0();
}
