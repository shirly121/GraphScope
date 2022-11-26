package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::arrow_projected_fragment_impl::TypedArray<std::string>",
    factory = StringTypedArray_cxx_0x5d3f85f1Factory.class
)
@FFISynthetic("com.alibaba.graphscope.ds.StringTypedArray")
public class StringTypedArray_cxx_0x5d3f85f1 extends BaseTypedArray_cxx_0x708692cf implements StringTypedArray {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(StringTypedArray_cxx_0x5d3f85f1.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public StringTypedArray_cxx_0x5d3f85f1(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StringTypedArray_cxx_0x5d3f85f1 that = (StringTypedArray_cxx_0x5d3f85f1) o;
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
  public StringView get(long arg0) {
    long ret$ = nativeGet(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.StringView_cxx_0x58a38028.SIZE), arg0); return (new com.alibaba.graphscope.ds.StringView_cxx_0x58a38028(ret$));
  }

  @CXXOperator("[]")
  @CXXValue
  public static native long nativeGet(long ptr, long rv_base, long arg00);

  @FFINameAlias("GetLength")
  public long getLength() {
    return nativeGetLength(address);
  }

  @FFINameAlias("GetLength")
  public static native long nativeGetLength(long ptr);

  @FFINameAlias("GetRawData")
  public long getRawData() {
    return nativeGetRawData(address);
  }

  @FFINameAlias("GetRawData")
  public static native long nativeGetRawData(long ptr);

  @FFINameAlias("GetRawDataLength")
  public long getRawDataLength() {
    return nativeGetRawDataLength(address);
  }

  @FFINameAlias("GetRawDataLength")
  public static native long nativeGetRawDataLength(long ptr);

  public void setAddress(long arg0) {
    this.address = arg0;
  }

  public static native long nativeCreateFactory0();
}
