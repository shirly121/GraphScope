package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Double;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::EdgeDataColumnDefault<double>")
@FFISynthetic("com.alibaba.graphscope.ds.EdgeDataColumn")
public class EdgeDataColumn_cxx_0x7bc33b49 extends FFIPointerImpl implements EdgeDataColumn<Double> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(EdgeDataColumn_cxx_0x7bc33b49.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public EdgeDataColumn_cxx_0x7bc33b49(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EdgeDataColumn_cxx_0x7bc33b49 that = (EdgeDataColumn_cxx_0x7bc33b49) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @CXXOperator("[]")
  @CXXValue
  public Double get(
      @FFIConst @CXXReference @FFITypeAlias("gs::NbrUnitDefault<uint64_t>") PropertyNbrUnit<Long> arg0) {
    return nativeGet(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @CXXOperator("[]")
  @CXXValue
  public static native double nativeGet(long ptr, long arg00);
}
