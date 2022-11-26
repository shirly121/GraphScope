package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "grape::Nbr<uint64_t,int64_t>",
    factory = GrapeNbr_cxx_0x979f8e12Factory.class
)
@FFISynthetic("com.alibaba.graphscope.ds.GrapeNbr")
public class GrapeNbr_cxx_0x979f8e12 extends FFIPointerImpl implements GrapeNbr<Long, Long> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(GrapeNbr_cxx_0x979f8e12.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public GrapeNbr_cxx_0x979f8e12(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GrapeNbr_cxx_0x979f8e12 that = (GrapeNbr_cxx_0x979f8e12) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public GrapeNbr<Long, Long> add(long arg0) {
    return new GrapeNbr_cxx_0x979f8e12(this.address + arg0);
  }

  public void addV(long arg0) {
    this.address += arg0;
  }

  @CXXOperator("*&")
  @CXXValue
  public GrapeNbr<Long, Long> copy() {
    long ret$ = nativeCopy(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.GrapeNbr_cxx_0x979f8e12.SIZE)); return (new com.alibaba.graphscope.ds.GrapeNbr_cxx_0x979f8e12(ret$));
  }

  @CXXOperator("*&")
  @CXXValue
  public static native long nativeCopy(long ptr, long rv_base);

  @FFIGetter
  @CXXReference
  public Long data() {
    return new java.lang.Long(nativeData(address));
  }

  @FFIGetter
  @CXXReference
  public static native long nativeData(long ptr);

  public long elementSize() {
    return SIZE;
  }

  public GrapeNbr<Long, Long> moveTo(long arg0) {
    return new GrapeNbr_cxx_0x979f8e12(arg0);
  }

  public void moveToV(long arg0) {
    this.address = arg0;
  }

  @FFIGetter
  @CXXReference
  public Vertex<Long> neighbor() {
    long ret$ = nativeNeighbor(address); return (new com.alibaba.graphscope.ds.Vertex_cxx_0xaccf3424(ret$));
  }

  @FFIGetter
  @CXXReference
  public static native long nativeNeighbor(long ptr);

  public void setAddress(long arg0) {
    this.address = arg0;
  }

  public static native long nativeCreateFactory0();

  public static native long nativeCreateFactory1(long arg00);

  public static native long nativeCreateFactory2(long arg00, long arg11);
}
