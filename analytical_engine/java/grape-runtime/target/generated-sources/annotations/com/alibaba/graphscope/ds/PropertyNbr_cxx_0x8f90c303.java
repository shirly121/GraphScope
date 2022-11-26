package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Integer;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::NbrDefault<uint32_t>",
    factory = PropertyNbr_cxx_0x8f90c303Factory.class
)
@FFISynthetic("com.alibaba.graphscope.ds.PropertyNbr")
public class PropertyNbr_cxx_0x8f90c303 extends FFIPointerImpl implements PropertyNbr<Integer> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(PropertyNbr_cxx_0x8f90c303.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public PropertyNbr_cxx_0x8f90c303(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PropertyNbr_cxx_0x8f90c303 that = (PropertyNbr_cxx_0x8f90c303) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public PropertyNbr<Integer> add(long arg0) {
    return new PropertyNbr_cxx_0x8f90c303(this.address + arg0);
  }

  public void addV(long arg0) {
    this.address += arg0;
  }

  @CXXOperator("--")
  @CXXReference
  public PropertyNbr<Integer> dec() {
    long ret$ = nativeDec(address); return (new com.alibaba.graphscope.ds.PropertyNbr_cxx_0x8f90c303(ret$));
  }

  @CXXOperator("--")
  @CXXReference
  public static native long nativeDec(long ptr);

  public long elementSize() {
    return SIZE;
  }

  @CXXOperator("==")
  public boolean eq(@CXXReference PropertyNbr<Integer> arg0) {
    return nativeEq(address, ((com.alibaba.fastffi.FFIPointerImpl) arg0).address);
  }

  @CXXOperator("==")
  public static native boolean nativeEq(long ptr, long arg00);

  @FFINameAlias("get_double")
  public double getDouble(int arg0) {
    return nativeGetDouble(address, arg0);
  }

  @FFINameAlias("get_double")
  public static native double nativeGetDouble(long ptr, int arg00);

  @FFINameAlias("get_int")
  public int getInt(int arg0) {
    return nativeGetInt(address, arg0);
  }

  @FFINameAlias("get_int")
  public static native int nativeGetInt(long ptr, int arg00);

  @FFINameAlias("get_str")
  @CXXValue
  public FFIByteString getString(int arg0) {
    long ret$ = nativeGetString(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.fastffi.impl.CXXStdString_cxx_0xcec1e274.SIZE), arg0); return (new com.alibaba.fastffi.impl.CXXStdString_cxx_0xcec1e274(ret$));
  }

  @FFINameAlias("get_str")
  @CXXValue
  public static native long nativeGetString(long ptr, long rv_base, int arg00);

  @CXXOperator("++")
  @CXXReference
  public PropertyNbr<Integer> inc() {
    long ret$ = nativeInc(address); return (new com.alibaba.graphscope.ds.PropertyNbr_cxx_0x8f90c303(ret$));
  }

  @CXXOperator("++")
  @CXXReference
  public static native long nativeInc(long ptr);

  public PropertyNbr<Integer> moveTo(long arg0) {
    return new PropertyNbr_cxx_0x8f90c303(arg0);
  }

  public void moveToV(long arg0) {
    this.address = arg0;
  }

  @FFINameAlias("neighbor")
  @CXXValue
  public Vertex<Integer> neighbor() {
    long ret$ = nativeNeighbor(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.Vertex_cxx_0xaca404e3.SIZE)); return (new com.alibaba.graphscope.ds.Vertex_cxx_0xaca404e3(ret$));
  }

  @FFINameAlias("neighbor")
  @CXXValue
  public static native long nativeNeighbor(long ptr, long rv_base);

  public void setAddress(long arg0) {
    this.address = arg0;
  }

  public static native long nativeCreateFactory0();
}
