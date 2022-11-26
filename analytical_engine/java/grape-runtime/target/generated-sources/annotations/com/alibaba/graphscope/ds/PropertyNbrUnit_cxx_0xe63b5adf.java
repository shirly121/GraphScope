package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Integer;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::NbrUnitDefault<uint32_t>")
@FFISynthetic("com.alibaba.graphscope.ds.PropertyNbrUnit")
public class PropertyNbrUnit_cxx_0xe63b5adf extends FFIPointerImpl implements PropertyNbrUnit<Integer> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(PropertyNbrUnit_cxx_0xe63b5adf.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public PropertyNbrUnit_cxx_0xe63b5adf(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PropertyNbrUnit_cxx_0xe63b5adf that = (PropertyNbrUnit_cxx_0xe63b5adf) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public PropertyNbrUnit<Integer> add(long arg0) {
    return new PropertyNbrUnit_cxx_0xe63b5adf(this.address + arg0);
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
  public Vertex<Integer> getNeighbor() {
    long ret$ = nativeGetNeighbor(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.Vertex_cxx_0xaca404e3.SIZE)); return (new com.alibaba.graphscope.ds.Vertex_cxx_0xaca404e3(ret$));
  }

  @FFINameAlias("get_neighbor")
  @CXXValue
  public static native long nativeGetNeighbor(long ptr, long rv_base);

  public PropertyNbrUnit<Integer> moveTo(long arg0) {
    return new PropertyNbrUnit_cxx_0xe63b5adf(arg0);
  }

  public void moveToV(long arg0) {
    this.address = arg0;
  }

  public void setAddress(long arg0) {
    this.address = arg0;
  }

  @FFIGetter
  public Integer vid() {
    return new java.lang.Integer(nativeVid(address));
  }

  @FFIGetter
  public static native int nativeVid(long ptr);
}
