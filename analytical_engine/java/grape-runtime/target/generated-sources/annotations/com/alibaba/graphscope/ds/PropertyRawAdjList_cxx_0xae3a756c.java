package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Integer;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::RawAdjListDefault<uint32_t>")
@FFISynthetic("com.alibaba.graphscope.ds.PropertyRawAdjList")
public class PropertyRawAdjList_cxx_0xae3a756c extends FFIPointerImpl implements PropertyRawAdjList<Integer> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(PropertyRawAdjList_cxx_0xae3a756c.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public PropertyRawAdjList_cxx_0xae3a756c(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PropertyRawAdjList_cxx_0xae3a756c that = (PropertyRawAdjList_cxx_0xae3a756c) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("begin")
  public PropertyNbrUnit<Integer> begin() {
    long ret$ = nativeBegin(address); return (ret$ == 0L ? null : new com.alibaba.graphscope.ds.PropertyNbrUnit_cxx_0xe63b5adf(ret$));
  }

  @FFINameAlias("begin")
  public static native long nativeBegin(long ptr);

  @CXXOperator("delete")
  public void delete() {
    nativeDelete(address);
  }

  @CXXOperator("delete")
  public static native void nativeDelete(long ptr);

  @FFINameAlias("Empty")
  public boolean empty() {
    return nativeEmpty(address);
  }

  @FFINameAlias("Empty")
  public static native boolean nativeEmpty(long ptr);

  @FFINameAlias("end")
  public PropertyNbrUnit<Integer> end() {
    long ret$ = nativeEnd(address); return (ret$ == 0L ? null : new com.alibaba.graphscope.ds.PropertyNbrUnit_cxx_0xe63b5adf(ret$));
  }

  @FFINameAlias("end")
  public static native long nativeEnd(long ptr);

  @FFINameAlias("Size")
  public int size() {
    return nativeSize(address);
  }

  @FFINameAlias("Size")
  public static native int nativeSize(long ptr);
}
