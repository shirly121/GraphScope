package com.alibaba.graphscope.parallel.message;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::PrimitiveMessage<int64_t>",
    factory = PrimitiveMessage_cxx_0x5581102cFactory.class
)
@FFISynthetic("com.alibaba.graphscope.parallel.message.PrimitiveMessage")
public class PrimitiveMessage_cxx_0x5581102c extends FFIPointerImpl implements PrimitiveMessage<Long> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(PrimitiveMessage_cxx_0x5581102c.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public PrimitiveMessage_cxx_0x5581102c(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PrimitiveMessage_cxx_0x5581102c that = (PrimitiveMessage_cxx_0x5581102c) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public Long getData() {
    return new java.lang.Long(nativeGetData(address));
  }

  public static native long nativeGetData(long ptr);

  public void setData(Long arg0) {
    nativeSetData(address, arg0);
  }

  public static native void nativeSetData(long ptr, long arg00);

  public static native long nativeCreateFactory0();

  public static native long nativeCreateFactory1(long arg00);
}
