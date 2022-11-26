package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::NbrUnitDefault<uint64_t>")
@FFISynthetic("com.alibaba.graphscope.ds.PropertyNbrUnit")
public class PropertyNbrUnit_cxx_0xe6668a20 extends FFIPointerImpl implements PropertyNbrUnit<Long> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(PropertyNbrUnit_cxx_0xe6668a20.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public PropertyNbrUnit_cxx_0xe6668a20(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PropertyNbrUnit_cxx_0xe6668a20 that = (PropertyNbrUnit_cxx_0xe6668a20) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public PropertyNbrUnit<Long> add(long arg0) {
    return new PropertyNbrUnit_cxx_0xe6668a20(this.address + arg0);
  }

  public void addV(long arg0) {
    this.address += arg0;
  }

  @FFIGetter
  public long eid() {
    return nativeEid(address);
  }

  @FFIGetter
  public static native long nativeEid(long ptr);

  public long elementSize() {
    return SIZE;
  }

  @FFINameAlias("get_neighbor")
  @CXXValue
  public Vertex<Long> getNeighbor() {
    long ret$ = nativeGetNeighbor(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.Vertex_cxx_0xaccf3424.SIZE)); return (new com.alibaba.graphscope.ds.Vertex_cxx_0xaccf3424(ret$));
  }

  @FFINameAlias("get_neighbor")
  @CXXValue
  public static native long nativeGetNeighbor(long ptr, long rv_base);

  public PropertyNbrUnit<Long> moveTo(long arg0) {
    return new PropertyNbrUnit_cxx_0xe6668a20(arg0);
  }

  public void moveToV(long arg0) {
    this.address = arg0;
  }

  public void setAddress(long arg0) {
    this.address = arg0;
  }

  @FFIGetter
  public Long vid() {
    return new java.lang.Long(nativeVid(address));
  }

  @FFIGetter
  public static native long nativeVid(long ptr);
}
