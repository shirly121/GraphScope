package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Integer;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::AdjListDefault<uint32_t>")
@FFISynthetic("com.alibaba.graphscope.ds.PropertyAdjList")
public class PropertyAdjList_cxx_0x8b7a773c extends FFIPointerImpl implements PropertyAdjList<Integer> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(PropertyAdjList_cxx_0x8b7a773c.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public PropertyAdjList_cxx_0x8b7a773c(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PropertyAdjList_cxx_0x8b7a773c that = (PropertyAdjList_cxx_0x8b7a773c) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("begin")
  @CXXValue
  public PropertyNbr<Integer> begin() {
    long ret$ = nativeBegin(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.PropertyNbr_cxx_0x8f90c303.SIZE)); return (new com.alibaba.graphscope.ds.PropertyNbr_cxx_0x8f90c303(ret$));
  }

  @FFINameAlias("begin")
  @CXXValue
  public static native long nativeBegin(long ptr, long rv_base);

  @FFINameAlias("begin_unit")
  public PropertyNbrUnit<Integer> beginUnit() {
    long ret$ = nativeBeginUnit(address); return (ret$ == 0L ? null : new com.alibaba.graphscope.ds.PropertyNbrUnit_cxx_0xe63b5adf(ret$));
  }

  @FFINameAlias("begin_unit")
  public static native long nativeBeginUnit(long ptr);

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
  @CXXValue
  public PropertyNbr<Integer> end() {
    long ret$ = nativeEnd(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.PropertyNbr_cxx_0x8f90c303.SIZE)); return (new com.alibaba.graphscope.ds.PropertyNbr_cxx_0x8f90c303(ret$));
  }

  @FFINameAlias("end")
  @CXXValue
  public static native long nativeEnd(long ptr, long rv_base);

  @FFINameAlias("end_unit")
  public PropertyNbrUnit<Integer> endUnit() {
    long ret$ = nativeEndUnit(address); return (ret$ == 0L ? null : new com.alibaba.graphscope.ds.PropertyNbrUnit_cxx_0xe63b5adf(ret$));
  }

  @FFINameAlias("end_unit")
  public static native long nativeEndUnit(long ptr);

  @FFINameAlias("Size")
  public int size() {
    return nativeSize(address);
  }

  @FFINameAlias("Size")
  public static native int nativeSize(long ptr);
}
