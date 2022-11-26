package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "vineyard::ArrayBuilder<int64_t>",
    factory = VineyardArrayBuilder_cxx_0x7d5f0692Factory.class
)
@FFISynthetic("com.alibaba.graphscope.graphx.VineyardArrayBuilder")
public class VineyardArrayBuilder_cxx_0x7d5f0692 extends FFIPointerImpl implements VineyardArrayBuilder<Long> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(VineyardArrayBuilder_cxx_0x7d5f0692.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public VineyardArrayBuilder_cxx_0x7d5f0692(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VineyardArrayBuilder_cxx_0x7d5f0692 that = (VineyardArrayBuilder_cxx_0x7d5f0692) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @CXXOperator("[]")
  @CXXReference
  public Long get(long arg0) {
    return new java.lang.Long(nativeGet(address, arg0));
  }

  @CXXOperator("[]")
  @CXXReference
  public static native long nativeGet(long ptr, long arg00);

  @CXXOperator("[]")
  public void set(long arg0, Long arg1) {
    nativeSet(address, arg0, arg1);
  }

  @CXXOperator("[]")
  public static native void nativeSet(long ptr, long arg00, long arg11);

  public long size() {
    return nativeSize(address);
  }

  public static native long nativeSize(long ptr);

  public static native long nativeCreateFactory0(long arg00, long arg11);

  public static native long nativeCreateFactory1(long arg00, long arg11);
}
